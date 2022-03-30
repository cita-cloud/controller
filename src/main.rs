// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod auth;
mod chain;
mod config;
mod controller;
mod genesis;
mod node_manager;
mod panic_hook;
mod pool;
mod protocol;
#[macro_use]
mod util;
mod event;
mod utxo_set;

use crate::{
    chain::ChainStep,
    config::ControllerConfig,
    controller::Controller,
    event::EventTask,
    node_manager::ChainStatusInit,
    panic_hook::set_panic_handler,
    protocol::sync_manager::{SyncBlockRespond, SyncBlocks},
    util::{
        get_full_block, init_grpc_client, kms_client, load_data_maybe_empty, reconfigure,
        storage_client,
    },
    utxo_set::{
        LOCK_ID_ADMIN, LOCK_ID_BLOCK_INTERVAL, LOCK_ID_BLOCK_LIMIT, LOCK_ID_BUTTON,
        LOCK_ID_CHAIN_ID, LOCK_ID_EMERGENCY_BRAKE, LOCK_ID_VALIDATORS, LOCK_ID_VERSION, LOCK_ID_PACKAGE_LIMIT,
    },
};
use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx::UtxoTx, CompactBlock, RawTransaction, RawTransactions},
    common::{
        ConsensusConfiguration, ConsensusConfigurationResponse, Empty, Hash, Hashes, NodeNetInfo,
        Proposal, ProposalResponse, ProposalWithProof, TotalNodeInfo,
    },
    controller::SystemConfig,
    controller::{
        rpc_service_server::RpcService, rpc_service_server::RpcServiceServer, BlockNumber, Flag,
        PeerCount, SoftwareVersion, TransactionIndex,
    },
    network::RegisterInfo,
};
use clap::Parser;
use cloud_util::{
    crypto::{hash_data, sign_message},
    network::register_network_msg_handler,
    storage::{load_data, store_data},
};
use genesis::GenesisBlock;
use git_version::git_version;
use log::{debug, info, warn};
use prost::Message;
use status_code::StatusCode;
use std::net::AddrParseError;
use std::time::Duration;
use tokio::{sync::mpsc, time};
use tonic::{transport::Server, Request, Response, Status};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/controller";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Parser)]
#[clap(version, author)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Parser)]
struct RunOpts {
    /// Chain config path
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: String,
    /// log config path
    #[clap(short = 'l', long = "log", default_value = "controller-log4rs.yaml")]
    log_file: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");
    set_panic_handler();

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            let fin = run(opts);
            warn!("Should not reach here {:?}", fin);
        }
    }
}

// grpc server of RPC
pub struct RPCServer {
    controller: Controller,
}

impl RPCServer {
    fn new(controller: Controller) -> Self {
        RPCServer { controller }
    }
}

#[tonic::async_trait]
impl RpcService for RPCServer {
    async fn get_block_number(
        &self,
        request: Request<Flag>,
    ) -> Result<Response<BlockNumber>, Status> {
        debug!("get_block_number request: {:?}", request);

        let flag = request.into_inner();
        self.controller
            .rpc_get_block_number(flag.flag)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e)),
                |block_number| {
                    let reply = Response::new(BlockNumber { block_number });
                    Ok(reply)
                },
            )
    }

    async fn send_raw_transaction(
        &self,
        request: Request<RawTransaction>,
    ) -> Result<Response<Hash>, Status> {
        debug!("send_raw_transaction request: {:?}", request);

        let raw_tx = request.into_inner();

        self.controller
            .rpc_send_raw_transaction(raw_tx, self.controller.config.enable_forward)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |tx_hash| {
                    let reply = Response::new(Hash { hash: tx_hash });
                    Ok(reply)
                },
            )
    }

    async fn send_raw_transactions(
        &self,
        request: Request<RawTransactions>,
    ) -> Result<Response<Hashes>, Status> {
        debug!("send_raw_transactions request: {:?}", request);

        let raw_txs = request.into_inner();

        self.controller
            .batch_transactions(raw_txs, self.controller.config.enable_forward)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |hashes| {
                    let reply = Response::new(hashes);
                    Ok(reply)
                },
            )
    }

    async fn get_block_by_hash(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<CompactBlock>, Status> {
        debug!("get_block_by_hash request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_block_by_hash(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    async fn get_block_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<CompactBlock>, Status> {
        debug!("get_block_by_number request: {:?}", request);

        let block_number = request.into_inner();

        self.controller
            .rpc_get_block_by_number(block_number.block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    async fn get_transaction(
        &self,
        request: Request<Hash>,
    ) -> Result<tonic::Response<RawTransaction>, Status> {
        debug!("get_block_by_number request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_transaction(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |raw_tx| {
                    let reply = Response::new(raw_tx);
                    Ok(reply)
                },
            )
    }

    async fn get_system_config(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<SystemConfig>, Status> {
        debug!("get_system_config request: {:?}", request);

        self.controller.rpc_get_system_config().await.map_or_else(
            |e| Err(Status::invalid_argument(e.to_string())),
            |sys_config| {
                let reply = Response::new(SystemConfig {
                    version: sys_config.version,
                    chain_id: sys_config.chain_id,
                    admin: sys_config.admin,
                    block_interval: sys_config.block_interval,
                    validators: sys_config.validators,
                    emergency_brake: sys_config.emergency_brake,
                    package_limit: sys_config.package_limit as u32,
                    block_limit: sys_config.block_limit as u32,
                    version_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_VERSION)
                        .unwrap()
                        .to_owned(),
                    chain_id_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_CHAIN_ID)
                        .unwrap()
                        .to_owned(),
                    admin_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_ADMIN)
                        .unwrap()
                        .to_owned(),
                    block_interval_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_BLOCK_INTERVAL)
                        .unwrap()
                        .to_owned(),
                    validators_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_VALIDATORS)
                        .unwrap()
                        .to_owned(),
                    emergency_brake_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_EMERGENCY_BRAKE)
                        .unwrap()
                        .to_owned(),
                    package_limit_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_PACKAGE_LIMIT)
                        .unwrap()
                        .to_owned(),
                    block_limit_pre_hash: sys_config
                        .utxo_tx_hashes
                        .get(&LOCK_ID_BLOCK_LIMIT)
                        .unwrap()
                        .to_owned(),

                });
                Ok(reply)
            },
        )
    }

    async fn get_version(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<SoftwareVersion>, Status> {
        debug!("get_version request: {:?}", request);
        let reply = Response::new(SoftwareVersion {
            version: env!("CARGO_PKG_VERSION").to_string(),
        });
        Ok(reply)
    }

    async fn get_block_hash(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<Response<Hash>, Status> {
        debug!("get_block_hash request: {:?}", request);

        let block_number = request.into_inner();

        self.controller
            .rpc_get_block_hash(block_number.block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block_hash| {
                    let reply = Response::new(Hash { hash: block_hash });
                    Ok(reply)
                },
            )
    }

    async fn get_transaction_block_number(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<BlockNumber>, Status> {
        debug!("get_transaction_block_number request: {:?}", request);

        let tx_hash = request.into_inner();

        self.controller
            .rpc_get_tx_block_number(tx_hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block_number| {
                    let reply = Response::new(BlockNumber { block_number });
                    Ok(reply)
                },
            )
    }

    async fn get_transaction_index(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<TransactionIndex>, Status> {
        debug!("get_transaction_index request: {:?}", request);

        let tx_hash = request.into_inner();

        self.controller
            .rpc_get_tx_index(tx_hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |tx_index| {
                    let reply = Response::new(TransactionIndex { tx_index });
                    Ok(reply)
                },
            )
    }

    async fn get_peer_count(&self, request: Request<Empty>) -> Result<Response<PeerCount>, Status> {
        debug!("get_peer_count request: {:?}", request);

        self.controller.rpc_get_peer_count().await.map_or_else(
            |e| Err(Status::invalid_argument(e.to_string())),
            |peer_count| {
                let reply = Response::new(PeerCount { peer_count });
                Ok(reply)
            },
        )
    }

    async fn add_node(
        &self,
        request: Request<NodeNetInfo>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        debug!("add_node request: {:?}", request);

        Ok(self.controller.rpc_add_node(request).await)
    }

    async fn get_peers_info(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<TotalNodeInfo>, Status> {
        debug!("get_peers_info request: {:?}", request);

        Ok(Response::new(
            self.controller
                .rpc_get_peers_info(request)
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }
}

use cita_cloud_proto::controller::{
    consensus2_controller_service_server::Consensus2ControllerService,
    consensus2_controller_service_server::Consensus2ControllerServiceServer,
};

//grpc server for Consensus2ControllerService
pub struct Consensus2ControllerServer {
    controller: Controller,
}

impl Consensus2ControllerServer {
    fn new(controller: Controller) -> Self {
        Consensus2ControllerServer { controller }
    }
}

#[tonic::async_trait]
impl Consensus2ControllerService for Consensus2ControllerServer {
    async fn get_proposal(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ProposalResponse>, Status> {
        debug!("get_proposal request: {:?}", request);

        self.controller.chain_get_proposal().await.map_or_else(
            |status| {
                warn!("rpc: get_proposal failed: {:?}", status);
                Ok(Response::new(ProposalResponse {
                    status: Some(status.into()),
                    proposal: None,
                }))
            },
            |(height, data, status)| {
                let proposal = Proposal { height, data };
                Ok(Response::new(ProposalResponse {
                    status: Some(status.into()),
                    proposal: Some(proposal),
                }))
            },
        )
    }
    async fn check_proposal(
        &self,
        request: Request<Proposal>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        debug!("check_proposal request: {:?}", request);

        let proposal = request.into_inner();

        let height = proposal.height;
        let data = proposal.data;

        match self.controller.chain_check_proposal(height, &data).await {
            Err(e) => {
                warn!("rpc: check_proposal failed: {:?}", e.to_string());
                Ok(Response::new(e.into()))
            }
            Ok(_) => Ok(Response::new(StatusCode::Success.into())),
        }
    }
    async fn commit_block(
        &self,
        request: Request<ProposalWithProof>,
    ) -> Result<Response<ConsensusConfigurationResponse>, Status> {
        debug!("commit_block request: {:?}", request);

        let proposal_with_proof = request.into_inner();
        let proposal = proposal_with_proof.proposal.unwrap();
        let height = proposal.height;
        let data = proposal.data;
        let proof = proposal_with_proof.proof;

        let config = {
            let rd = self.controller.auth.read().await;
            rd.get_system_config()
        };

        self.controller
            .chain_commit_block(height, &data, &proof)
            .await
            .map_or_else(
                |e| {
                    warn!("rpc: commit_block failed: {:?}", e);

                    let con_cfg = ConsensusConfiguration {
                        height,
                        block_interval: config.block_interval,
                        validators: config.validators,
                    };
                    Ok(Response::new(ConsensusConfigurationResponse {
                        status: Some(e.into()),
                        config: Some(con_cfg),
                    }))
                },
                |r| {
                    Ok(Response::new(ConsensusConfigurationResponse {
                        status: Some(StatusCode::Success.into()),
                        config: Some(r),
                    }))
                },
            )
    }
}

use cita_cloud_proto::network::{
    network_msg_handler_service_server::NetworkMsgHandlerService,
    network_msg_handler_service_server::NetworkMsgHandlerServiceServer, NetworkMsg,
};
use util::network_client;

// grpc server of network msg handler
pub struct ControllerNetworkMsgHandlerServer {
    controller: Controller,
}

impl ControllerNetworkMsgHandlerServer {
    fn new(controller: Controller) -> Self {
        ControllerNetworkMsgHandlerServer { controller }
    }
}

#[tonic::async_trait]
impl NetworkMsgHandlerService for ControllerNetworkMsgHandlerServer {
    async fn process_network_msg(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        debug!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "controller" {
            Ok(Response::new(StatusCode::ModuleNotController.into()))
        } else {
            let msg_type = msg.r#type.clone();
            self.controller.process_network_msg(msg).await.map_or_else(
                |status| {
                    if status != StatusCode::HistoryDupTx || rand::random::<u16>() < 8 {
                        warn!("rpc: process_network_msg({}) failed: {}", msg_type, status);
                    }

                    Ok(Response::new(status.into()))
                },
                |_| Ok(Response::new(StatusCode::Success.into())),
            )
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCode> {
    // read consensus-config.toml
    let mut config = ControllerConfig::new(&opts.config_path);

    // init log4rs
    log4rs::init_file(&opts.log_file, Default::default())
        .map_err(|e| println!("log init err: {}", e))
        .unwrap();

    init_grpc_client(&config);

    let grpc_port = config.controller_port.to_string();

    info!("grpc port of this service: {}", grpc_port);

    let mut server_retry_interval =
        time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        server_retry_interval.tick().await;

        // register endpoint
        let request = RegisterInfo {
            module_name: "controller".to_owned(),
            hostname: "127.0.0.1".to_owned(),
            port: grpc_port.clone(),
        };

        match register_network_msg_handler(network_client(), request).await {
            StatusCode::Success => {
                info!("register network msg handler success!");
                break;
            }
            status => warn!(
                "register network msg handler failed({:?})! Retrying",
                status
            ),
        }
    }

    let mut server_retry_interval =
        time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        server_retry_interval.tick().await;
        // register endpoint
        {
            if let Ok(crypto_info) = kms_client().get_crypto_info(Request::new(Empty {})).await {
                let inner = crypto_info.into_inner();
                if inner.status.is_some() {
                    match StatusCode::from(inner.status.unwrap()) {
                        StatusCode::Success => {
                            config.hash_len = inner.hash_len;
                            config.signature_len = inner.signature_len;
                            config.address_len = inner.address_len;
                            info!("kms({}) is ready!", &inner.name);
                            break;
                        }
                        status => warn!("get get_crypto_info failed: {:?}", status),
                    }
                }
            }
        }
        warn!("kms not ready! Retrying");
    }

    // load sys_config
    info!("load sys_config");
    let genesis = GenesisBlock::new(&opts.config_path);
    let current_block_number;
    let current_block_hash;
    let mut server_retry_interval =
        time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        server_retry_interval.tick().await;
        {
            match load_data_maybe_empty(0, 0u64.to_be_bytes().to_vec()).await {
                Ok(current_block_number_bytes) => {
                    info!("get current block number success!");
                    if current_block_number_bytes.is_empty() {
                        info!("this is a new chain!");
                        current_block_number = 0u64;
                        current_block_hash = genesis.genesis_block_hash().await;
                    } else {
                        info!("this is an old chain!");
                        let mut bytes: [u8; 8] = [0; 8];
                        bytes[..8].clone_from_slice(&current_block_number_bytes[..8]);
                        current_block_number = u64::from_be_bytes(bytes);
                        current_block_hash =
                            load_data(storage_client(), 0, 1u64.to_be_bytes().to_vec())
                                .await
                                .unwrap();
                    }
                    break;
                }
                Err(e) => warn!("{}", e.to_string()),
            }
        }
        warn!("get current block number failed! Retrying");
    }
    info!(
        "current block number: {}, current block hash: 0x{}",
        current_block_number,
        hex::encode(&current_block_hash)
    );

    let mut sys_config = utxo_set::SystemConfig::new(&opts.config_path);
    for lock_id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
        // region 0 global
        match load_data(storage_client(), 0, lock_id.to_be_bytes().to_vec()).await {
            Ok(data_or_tx_hash) => {
                //data or tx_hash stored at this lock_id, read to update sys_config
                // region 1: tx_hash - tx
                if data_or_tx_hash.len() == config.hash_len as usize && lock_id != LOCK_ID_CHAIN_ID
                {
                    match load_data(storage_client(), 1, data_or_tx_hash.clone()).await {
                        Ok(raw_tx_bytes) => {
                            info!("lock_id: {} stored tx_hash", lock_id);
                            let raw_tx = RawTransaction::decode(raw_tx_bytes.as_slice()).unwrap();
                            let tx = raw_tx.tx.unwrap();
                            if let UtxoTx(utxo_tx) = tx {
                                sys_config.update(&utxo_tx, true);
                            } else {
                                panic!("tx_hash lock_id: {} stored is not utxo_tx", lock_id);
                            }
                        }
                        Err(e) => {
                            panic!("load tx stored at lock_id: {} failed: {}", lock_id, e)
                        }
                    }
                } else {
                    info!("lock_id: {} stored data", lock_id);
                    if !sys_config.match_data(lock_id, data_or_tx_hash) {
                        panic!("match data lock_id: {} stored failed", lock_id);
                    }
                }
            }
            Err(StatusCode::NotFound) => {
                //this lock_id is empty in local, store data from sys_config to local
                info!("lock_id: {} empty, store from config to local", lock_id);
                match lock_id {
                    LOCK_ID_VERSION => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.version.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_CHAIN_ID => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.chain_id.clone(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_ADMIN => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.admin.clone(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_BLOCK_INTERVAL => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.block_interval.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_VALIDATORS => {
                        let mut validators = Vec::new();
                        for validator in sys_config.validators.iter() {
                            validators.append(&mut validator.clone());
                        }
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            validators,
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_EMERGENCY_BRAKE => {
                        store_data(storage_client(), 0, lock_id.to_be_bytes().to_vec(), vec![])
                            .await
                            .is_success()?;
                    }
                    LOCK_ID_BLOCK_LIMIT => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.block_limit.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_PACKAGE_LIMIT => {
                        store_data(
                            storage_client(),
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.package_limit.to_be_bytes().to_vec(),
                        )
                            .await
                            .is_success()?;
                    }
                    _ => {
                        warn!("Invalid lock_id: {}", lock_id);
                    }
                };
            }
            Err(e) => panic!("load_data lock_id: {} failed: {}", lock_id, e),
        }
    }
    info!("load sys_config complete");

    // todo config
    let (task_sender, mut task_receiver) = mpsc::channel(64);

    let controller = Controller::new(
        config.clone(),
        current_block_number,
        current_block_hash,
        sys_config.clone(),
        genesis,
        task_sender,
    );

    config.set_global();
    controller.init(current_block_number, sys_config).await;

    let controller_for_reconnect = controller.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            controller_for_reconnect
                .config
                .origin_node_reconnect_interval,
        ));
        loop {
            interval.tick().await;
            {
                controller_for_reconnect
                    .task_sender
                    .send(EventTask::BroadCastCSI)
                    .await
                    .unwrap();
            }
        }
    });

    let controller_for_task = controller.clone();
    tokio::spawn(async move {
        while let Some(event_task) = task_receiver.recv().await {
            match event_task {
                EventTask::SyncBlockReq(req, origin) => {
                    use crate::protocol::sync_manager::sync_block_respond::Respond;
                    let mut block_vec = Vec::new();

                    for h in req.start_height..=req.end_height {
                        if let Ok(block) = get_full_block(h).await {
                            block_vec.push(block);
                        } else {
                            log::warn!("handle sync_block error: not get block(h: {})", h);
                            break;
                        }
                    }

                    if block_vec.len() as u64 != req.end_height - req.start_height + 1 {
                        let sync_block_respond = SyncBlockRespond {
                            respond: Some(Respond::MissBlock(
                                controller_for_task.local_address.clone(),
                            )),
                        };
                        controller_for_task
                            .unicast_sync_block_respond(origin, sync_block_respond)
                            .await;
                    } else {
                        log::info!(
                            "send sync_block_res: {}-{}",
                            req.start_height,
                            req.end_height
                        );
                        let sync_block = SyncBlocks {
                            address: Some(controller_for_task.local_address.clone()),
                            sync_blocks: block_vec,
                        };
                        let sync_block_respond = SyncBlockRespond {
                            respond: Some(Respond::Ok(sync_block)),
                        };
                        controller_for_task
                            .unicast_sync_block_respond(origin, sync_block_respond)
                            .await;
                    }
                }
                EventTask::SyncBlock => {
                    log::debug!("receive sync block event");
                    let (global_address, global_status) =
                        controller_for_task.get_global_status().await;
                    let mut own_status = controller_for_task.get_status().await;
                    // get chain lock means syncing
                    controller_for_task.set_sync_state(true).await;
                    {
                        let mut chain = controller_for_task.chain.write().await;
                        match chain.next_step(&global_status).await {
                            ChainStep::SyncStep => {
                                while let Some((addr, block)) = controller_for_task
                                    .sync_manager
                                    .pop_block(own_status.height + 1)
                                    .await
                                {
                                    chain.clear_candidate();
                                    match chain.process_block(block, false).await {
                                        Ok((consensus_config, mut status)) => {
                                            // todo reconfigure failed
                                            reconfigure(consensus_config)
                                                .await
                                                .is_success()
                                                .unwrap();
                                            status.address =
                                                Some(controller_for_task.local_address.clone());
                                            controller_for_task.set_status(status.clone()).await;
                                            own_status = status;
                                        }
                                        Err(e) => {
                                            warn!(
                                                "sync block error: {}, node: 0x{} been misbehavior_node",
                                                e.to_string(),
                                                hex::encode(&addr.address)
                                            );
                                            let _ = controller_for_task
                                                .node_manager
                                                .set_misbehavior_node(&addr)
                                                .await;
                                            if global_address == addr {
                                                let (ex_addr, ex_status) = controller_for_task
                                                    .node_manager
                                                    .pick_node()
                                                    .await;
                                                controller_for_task
                                                    .update_global_status(ex_addr, ex_status)
                                                    .await;
                                            }
                                            if let Some(range_heights) = controller_for_task
                                                .sync_manager
                                                .clear_node_block(&addr, &own_status)
                                                .await
                                            {
                                                let (global_address, global_status) =
                                                    controller_for_task.get_global_status().await;
                                                if !global_address.address.is_empty() {
                                                    let global_origin = controller_for_task
                                                        .node_manager
                                                        .get_origin(&global_address)
                                                        .await
                                                        .unwrap();
                                                    for range_height in range_heights {
                                                        if let Some(reqs) = controller_for_task
                                                            .sync_manager
                                                            .re_sync_block_req(
                                                                range_height,
                                                                &global_status,
                                                            )
                                                        {
                                                            for req in reqs {
                                                                controller_for_task
                                                                    .unicast_sync_block(
                                                                        global_origin,
                                                                        req,
                                                                    )
                                                                    .await;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            ChainStep::OnlineStep => {
                                controller_for_task.set_sync_state(false).await;
                                controller_for_task.sync_manager.clear().await;
                            }
                            ChainStep::BusyState => unreachable!(),
                        }
                    }
                    controller_for_task.sync_block().await.unwrap();
                }
                EventTask::BroadCastCSI => {
                    log::info!("receive BroadCastCSI event task");
                    let status = controller_for_task.get_status().await;

                    let mut chain_status_bytes = Vec::new();
                    status
                        .encode(&mut chain_status_bytes)
                        .map_err(|_| {
                            log::warn!("process_network_msg: encode ChainStatus failed");
                            StatusCode::EncodeError
                        })
                        .unwrap();
                    let msg_hash = hash_data(kms_client(), &chain_status_bytes).await.unwrap();
                    let signature =
                        sign_message(kms_client(), controller_for_task.config.key_id, &msg_hash)
                            .await
                            .unwrap();

                    controller_for_task
                        .broadcast_chain_status_init(ChainStatusInit {
                            chain_status: Some(status),
                            signature,
                        })
                        .await
                        .await
                        .unwrap();
                }
            }
        }
    });

    let addr_str = format!("0.0.0.0:{}", grpc_port);
    let addr = addr_str.parse().map_err(|e: AddrParseError| {
        warn!("grpc listen addr parse failed: {} ", e.to_string());
        StatusCode::FatalError
    })?;

    info!("start grpc server!");
    Server::builder()
        .add_service(RpcServiceServer::new(RPCServer::new(controller.clone())))
        .add_service(Consensus2ControllerServiceServer::new(
            Consensus2ControllerServer::new(controller.clone()),
        ))
        .add_service(NetworkMsgHandlerServiceServer::new(
            ControllerNetworkMsgHandlerServer::new(controller),
        ))
        .serve(addr)
        .await
        .map_err(|e| {
            warn!("start controller grpc server failed: {} ", e.to_string());
            StatusCode::FatalError
        })?;

    Ok(())
}
