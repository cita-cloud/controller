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
mod health_check;
mod system_config;

use crate::{
    chain::ChainStep,
    config::ControllerConfig,
    controller::Controller,
    event::EventTask,
    node_manager::{ChainStatusInit, NodeAddress},
    panic_hook::set_panic_handler,
    protocol::sync_manager::{SyncBlockRespond, SyncBlocks},
    system_config::{
        LOCK_ID_ADMIN, LOCK_ID_BLOCK_INTERVAL, LOCK_ID_BLOCK_LIMIT, LOCK_ID_BUTTON,
        LOCK_ID_CHAIN_ID, LOCK_ID_EMERGENCY_BRAKE, LOCK_ID_QUOTA_LIMIT, LOCK_ID_VALIDATORS,
        LOCK_ID_VERSION,
    },
    util::{
        crypto_client, get_full_block, get_hash_in_range, init_grpc_client, load_data_maybe_empty,
        reconfigure, storage_client,
    },
};
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    client::CryptoClientTrait,
    common::{
        ConsensusConfiguration, ConsensusConfigurationResponse, Empty, Hash, Hashes, NodeNetInfo,
        NodeStatus, Proof, Proposal, ProposalResponse, ProposalWithProof, StateRoot,
    },
    controller::SystemConfig,
    controller::{
        rpc_service_server::RpcService, rpc_service_server::RpcServiceServer, BlockNumber, Flag,
        TransactionIndex,
    },
    health_check::health_server::HealthServer,
    network::RegisterInfo,
    storage::Regions,
};
use clap::Parser;
use cloud_util::{
    crypto::{hash_data, sign_message},
    metrics::{run_metrics_exporter, MiddlewareLayer},
    network::register_network_msg_handler,
    storage::{load_data, store_data},
};
use genesis::GenesisBlock;
use health_check::HealthCheckServer;
use prost::Message;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::AddrParseError;
use std::time::Duration;
use tokio::{sync::mpsc, time};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

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

        let flag = request.into_inner().flag;
        self.controller
            .rpc_get_block_number(flag)
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

    async fn get_height_by_hash(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<BlockNumber>, Status> {
        debug!("get_height_by_hash request: {:?}", request);

        let hash = request.into_inner().hash;

        self.controller
            .rpc_get_height_by_hash(hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |height| {
                    let reply = Response::new(height);
                    Ok(reply)
                },
            )
    }

    async fn get_block_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<CompactBlock>, Status> {
        debug!("get_block_by_number request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_by_number(block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    async fn get_state_root_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<StateRoot>, Status> {
        debug!("get_state_root_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        self.controller
            .rpc_get_state_root_by_number(height)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |state_root| {
                    let reply = Response::new(state_root);
                    Ok(reply)
                },
            )
    }

    async fn get_proof_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<Proof>, Status> {
        debug!("get_proof_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        self.controller
            .rpc_get_proof_by_number(height)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |proof| {
                    let reply = Response::new(proof);
                    Ok(reply)
                },
            )
    }

    async fn get_block_detail_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<Block>, Status> {
        debug!("get_block_detail_by_number request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_detail_by_number(block_number)
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
                let reply = Response::new(sys_config.generate_proto_sys_config());
                Ok(reply)
            },
        )
    }

    async fn get_system_config_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<Response<SystemConfig>, Status> {
        debug!("get_system_config_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        let mut initial_sys_config = self.controller.initial_sys_config.clone();
        let utxo_tx_hashes = self
            .controller
            .rpc_get_system_config()
            .await
            .unwrap()
            .utxo_tx_hashes;

        for lock_id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
            let hash = utxo_tx_hashes.get(&lock_id).unwrap().to_owned();
            if hash != vec![0u8; 33] {
                let hash_in_range = get_hash_in_range(hash, height)
                    .await
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                if hash_in_range != vec![0u8; 33] {
                    //modify sys_config by utxo_tx
                    let res = initial_sys_config
                        .modify_sys_config_by_utxotx_hash(hash_in_range)
                        .await;
                    if res != StatusCodeEnum::Success {
                        return Err(Status::invalid_argument(res.to_string()));
                    }
                }
            }
        }

        let reply = Response::new(initial_sys_config.generate_proto_sys_config());

        Ok(reply)
    }

    async fn get_block_hash(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<Response<Hash>, Status> {
        debug!("get_block_hash request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_hash(block_number)
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

        let tx_hash = request.into_inner().hash;

        self.controller
            .rpc_get_tx_block_number(tx_hash)
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

    async fn add_node(
        &self,
        request: Request<NodeNetInfo>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        debug!("add_node request: {:?}", request);

        let info = request.into_inner();

        let reply = Response::new(self.controller.rpc_add_node(info).await);

        Ok(reply)
    }

    async fn get_node_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<NodeStatus>, Status> {
        debug!("get_node_status request: {:?}", request);

        Ok(Response::new(
            self.controller
                .rpc_get_node_status(request.into_inner())
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
                warn!(
                    "rpc: check_proposal({}) failed: {:?}",
                    height,
                    e.to_string()
                );
                Ok(Response::new(e.into()))
            }
            Ok(_) => Ok(Response::new(StatusCodeEnum::Success.into())),
        }
    }
    #[instrument(skip(self))]
    async fn commit_block(
        &self,
        request: Request<ProposalWithProof>,
    ) -> Result<Response<ConsensusConfigurationResponse>, Status> {
        cloud_util::tracer::set_parent(&request);
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

        if height != u64::MAX {
            self.controller
                .chain_commit_block(height, &data, &proof)
                .await
                .map_or_else(
                    |e| {
                        warn!("rpc: commit_block({}) failed: {:?}", height, e);

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
                            status: Some(StatusCodeEnum::Success.into()),
                            config: Some(r),
                        }))
                    },
                )
        } else {
            let con_cfg = ConsensusConfiguration {
                height: self.controller.get_status().await.height,
                block_interval: config.block_interval,
                validators: config.validators,
            };
            Ok(Response::new(ConsensusConfigurationResponse {
                status: Some(StatusCodeEnum::Success.into()),
                config: Some(con_cfg),
            }))
        }
    }
}

use crate::node_manager::ChainStatus;
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
            Ok(Response::new(StatusCodeEnum::ModuleNotController.into()))
        } else {
            let msg_type = msg.r#type.clone();
            let msg_origin = msg.origin;
            self.controller.process_network_msg(msg).await.map_or_else(
                |status| {
                    if status != StatusCodeEnum::HistoryDupTx || rand::random::<u16>() < 8 {
                        warn!(
                            "rpc: process_network_msg({} from {:x}) failed: {}",
                            msg_type, msg_origin, status
                        );
                    }
                    Ok(Response::new(status.into()))
                },
                |_| Ok(Response::new(StatusCodeEnum::Success.into())),
            )
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCodeEnum> {
    #[cfg(not(windows))]
    tokio::spawn(cloud_util::signal::handle_signals());

    // read consensus-config.toml
    let mut config = ControllerConfig::new(&opts.config_path);
    let enable_metrics = config.enable_metrics;
    let metrics_port = config.metrics_port;
    let metrics_buckets = config.metrics_buckets.clone();

    // init tracer
    cloud_util::tracer::init_tracer(&config.node_address, &config.log_config)
        .map_err(|e| println!("tracer init err: {e}"))
        .unwrap();

    init_grpc_client(&config);

    let grpc_port = config.controller_port.to_string();

    info!("grpc port of controller: {}", grpc_port);

    let health_check_timeout = config.health_check_timeout;

    info!("health check timeout: {}", health_check_timeout);

    let http2_keepalive_interval = config.http2_keepalive_interval;
    let http2_keepalive_timeout = config.http2_keepalive_timeout;
    let tcp_keepalive = config.tcp_keepalive;

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
            StatusCodeEnum::Success => {
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
            if let Ok(crypto_info) = crypto_client().get_crypto_info(Empty {}).await {
                if crypto_info.status.is_some() {
                    match StatusCodeEnum::from(crypto_info.status.unwrap()) {
                        StatusCodeEnum::Success => {
                            config.hash_len = crypto_info.hash_len;
                            config.signature_len = crypto_info.signature_len;
                            config.address_len = crypto_info.address_len;
                            info!("crypto({}) is ready!", &crypto_info.name);
                            break;
                        }
                        status => warn!("get get_crypto_info failed: {:?}", status),
                    }
                }
            }
        }
        warn!("crypto not ready! Retrying");
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
            match load_data_maybe_empty(
                i32::from(Regions::Global) as u32,
                0u64.to_be_bytes().to_vec(),
            )
            .await
            {
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
                        current_block_hash = load_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            1u64.to_be_bytes().to_vec(),
                        )
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

    let mut sys_config = system_config::SystemConfig::new(&opts.config_path);
    let initial_sys_config = sys_config.clone();
    for lock_id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
        // region 0 global
        match load_data(
            storage_client(),
            i32::from(Regions::Global) as u32,
            lock_id.to_be_bytes().to_vec(),
        )
        .await
        {
            Ok(data_or_tx_hash) => {
                //data or tx_hash stored at this lock_id, read to update sys_config
                // region 1: tx_hash - tx
                if data_or_tx_hash.len() == config.hash_len as usize && lock_id != LOCK_ID_CHAIN_ID
                {
                    if sys_config
                        .modify_sys_config_by_utxotx_hash(data_or_tx_hash.clone())
                        .await
                        != StatusCodeEnum::Success
                    {
                        panic!("modify_sys_config_by_utxotx_hash failed in lockid: {}, with utxo hash: {:?}", lock_id, hex::encode(data_or_tx_hash))
                    }
                } else {
                    info!("lock_id: {} stored data", lock_id);
                    if !sys_config.match_data(lock_id, data_or_tx_hash, true) {
                        panic!("match data lock_id: {lock_id} stored failed");
                    }
                }
            }
            Err(StatusCodeEnum::NotFound) => {
                //this lock_id is empty in local, store data from sys_config to local
                info!("lock_id: {} empty, store from config to local", lock_id);
                match lock_id {
                    LOCK_ID_VERSION => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.version.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_CHAIN_ID => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.chain_id.clone(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_ADMIN => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.admin.clone(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_BLOCK_INTERVAL => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
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
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            validators,
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_EMERGENCY_BRAKE => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            vec![],
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_BLOCK_LIMIT => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.block_limit.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    LOCK_ID_QUOTA_LIMIT => {
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            sys_config.quota_limit.to_be_bytes().to_vec(),
                        )
                        .await
                        .is_success()?;
                    }
                    _ => {
                        warn!("Invalid lock_id: {}", lock_id);
                    }
                };
            }
            Err(e) => panic!("load_data lock_id: {lock_id} failed: {e}"),
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
        initial_sys_config,
    )
    .await;

    config.set_global();
    controller.init(current_block_number, sys_config).await;

    let controller_for_reconnect = controller.clone();
    tokio::spawn(async move {
        let mut long_interval = time::interval(Duration::from_secs(
            controller_for_reconnect
                .config
                .origin_node_reconnect_interval,
        ));
        loop {
            long_interval.tick().await;
            {
                controller_for_reconnect
                    .task_sender
                    .send(EventTask::BroadCastCSI)
                    .await
                    .unwrap();
                controller_for_reconnect
                    .task_sender
                    .send(EventTask::RecordAllNode)
                    .await
                    .unwrap();
            }
        }
    });

    let controller_for_healthy = controller.clone();
    tokio::spawn(async move {
        let mut current_height = u64::MAX;
        // only above retry_limit allow broadcast retry, retry timing is 1, 1, 2, 4, 8...2^n
        let mut retry_limit: u64 = 0;
        // tick count interval times
        let mut tick: u64 = 0;
        let mut short_interval = time::interval(Duration::from_secs(
            controller_for_healthy
                .config
                .inner_block_growth_check_interval,
        ));
        loop {
            short_interval.tick().await;
            {
                tick += 1;
                if current_height == u64::MAX {
                    current_height = controller_for_healthy.get_status().await.height;
                    tick = 0;
                } else if controller_for_healthy.get_status().await.height == current_height
                    && tick >= retry_limit
                {
                    info!(
                        "inner healthy check: broadcast csi h: {} the {}th time",
                        current_height, tick
                    );
                    if controller_for_healthy.get_global_status().await.1.height > current_height {
                        let mut chain = controller_for_healthy.chain.write().await;
                        chain.clear_candidate();
                    }
                    controller_for_healthy
                        .task_sender
                        .send(EventTask::BroadCastCSI)
                        .await
                        .unwrap();
                    if controller_for_healthy.get_sync_state().await {
                        controller_for_healthy.set_sync_state(false).await;
                        controller_for_healthy.sync_manager.clear().await;
                    }
                    retry_limit += tick;
                    tick = 0;
                } else if controller_for_healthy.get_status().await.height < current_height {
                    unreachable!()
                } else if controller_for_healthy.get_status().await.height > current_height {
                    // update current height
                    current_height = controller_for_healthy.get_status().await.height;
                    tick = 0;
                    retry_limit = 0;
                }
            }
        }
    });

    let controller_for_retransmission = controller.clone();
    tokio::spawn(async move {
        let mut forward_interval = time::interval(Duration::from_micros(
            controller_for_retransmission.config.buffer_duration,
        ));
        loop {
            forward_interval.tick().await;
            {
                let mut f_pool = controller_for_retransmission.forward_pool.write().await;
                if !f_pool.body.is_empty() {
                    controller_for_retransmission
                        .broadcast_send_txs(f_pool.clone())
                        .await;
                    f_pool.body.clear();
                }
            }
        }
    });

    let controller_for_task = controller.clone();
    tokio::spawn(async move {
        let mut old_status: HashMap<NodeAddress, ChainStatus> = HashMap::new();
        while let Some(event_task) = task_receiver.recv().await {
            match event_task {
                EventTask::SyncBlockReq(req, origin) => {
                    use crate::protocol::sync_manager::sync_block_respond::Respond;
                    let mut block_vec = Vec::new();

                    for h in req.start_height..=req.end_height {
                        if let Ok(block) = get_full_block(h).await {
                            block_vec.push(block);
                        } else {
                            warn!("handle sync_block error: not get block(h: {})", h);
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
                        info!(
                            "send sync_block_res: {}-{}",
                            req.start_height, req.end_height
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
                    debug!("receive sync block event");
                    let (global_address, global_status) =
                        controller_for_task.get_global_status().await;
                    let mut own_status = controller_for_task.get_status().await;
                    // get chain lock means syncing
                    controller_for_task.set_sync_state(true).await;
                    {
                        match {
                            let chain = controller_for_task.chain.read().await;
                            chain.next_step(&global_status).await
                        } {
                            ChainStep::SyncStep => {
                                let mut syncing = false;
                                {
                                    let mut chain = controller_for_task.chain.write().await;
                                    while let Some((addr, block)) = controller_for_task
                                        .sync_manager
                                        .pop_block(own_status.height + 1)
                                        .await
                                    {
                                        chain.clear_candidate();
                                        match chain.process_block(block, false).await {
                                            Ok((consensus_config, mut status)) => {
                                                reconfigure(consensus_config)
                                                    .await
                                                    .is_success()
                                                    .unwrap();
                                                status.address =
                                                    Some(controller_for_task.local_address.clone());
                                                controller_for_task
                                                    .set_status(status.clone())
                                                    .await;
                                                own_status = status.clone();
                                                if status.height
                                                    % controller_for_task
                                                        .config
                                                        .send_chain_status_interval_sync
                                                    == 0
                                                {
                                                    controller_for_task
                                                        .broadcast_chain_status(status)
                                                        .await;
                                                }
                                                syncing = true;
                                            }
                                            Err(e) => {
                                                if (e as u64) % 100 == 0 {
                                                    warn!("sync block error: {}", e);
                                                    continue;
                                                }
                                                warn!("sync block error: {}, node: 0x{} been misbehavior_node", e.to_string(), hex::encode(&addr.address));
                                                let del_node_addr = NodeAddress::from(&addr);
                                                let _ = controller_for_task
                                                    .node_manager
                                                    .set_misbehavior_node(&del_node_addr)
                                                    .await;
                                                if global_address == del_node_addr {
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
                                                        controller_for_task
                                                            .get_global_status()
                                                            .await;
                                                    if global_address.0 != 0 {
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
                                                                            global_address.0,
                                                                            req,
                                                                        )
                                                                        .await;
                                                                }
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    syncing = true;
                                                }
                                            }
                                        }
                                    }
                                }
                                if syncing {
                                    controller_for_task.sync_block().await.unwrap();
                                }
                            }
                            ChainStep::OnlineStep => {
                                controller_for_task.set_sync_state(false).await;
                                controller_for_task.sync_manager.clear().await;
                            }
                            ChainStep::BusyState => unreachable!(),
                        }
                    }
                }
                EventTask::BroadCastCSI => {
                    info!("receive BroadCastCSI event task");
                    let status = controller_for_task.get_status().await;

                    let mut chain_status_bytes = Vec::new();
                    status
                        .encode(&mut chain_status_bytes)
                        .map_err(|_| {
                            warn!("process_network_msg: encode ChainStatus failed");
                            StatusCodeEnum::EncodeError
                        })
                        .unwrap();
                    let msg_hash = hash_data(crypto_client(), &chain_status_bytes)
                        .await
                        .unwrap();
                    let signature = sign_message(crypto_client(), &msg_hash).await.unwrap();

                    controller_for_task
                        .broadcast_chain_status_init(ChainStatusInit {
                            chain_status: Some(status),
                            signature,
                        })
                        .await
                        .await
                        .unwrap();
                }
                EventTask::RecordAllNode => {
                    let nodes = controller_for_task.node_manager.nodes.read().await.clone();
                    for (na, current_cs) in nodes.iter() {
                        if let Some(old_cs) = old_status.get(na) {
                            match old_cs.height.cmp(&current_cs.height) {
                                Ordering::Greater => {
                                    error!(
                                        "node({}) is a misbehave node, old height: {}, current height: {}",
                                        &na,
                                        old_cs.height,
                                        current_cs.height
                                    );
                                    let _ = controller_for_task
                                        .node_manager
                                        .set_misbehavior_node(na)
                                        .await;
                                }
                                Ordering::Equal => {
                                    warn!("node({}) is stale node, height: {}", &na, old_cs.height);
                                    if controller_for_task.node_manager.in_node(na).await {
                                        controller_for_task.node_manager.delete_node(na).await;
                                    }
                                }
                                Ordering::Less => {
                                    // update node in old status
                                    old_status.insert(*na, current_cs.clone());
                                }
                            }
                        } else {
                            old_status.insert(*na, current_cs.clone());
                        }
                    }
                }
            }
        }
    });

    let addr_str = format!("0.0.0.0:{grpc_port}");
    let addr = addr_str.parse().map_err(|e: AddrParseError| {
        warn!("grpc listen addr parse failed: {} ", e.to_string());
        StatusCodeEnum::FatalError
    })?;

    let layer = if enable_metrics {
        tokio::spawn(async move {
            run_metrics_exporter(metrics_port).await.unwrap();
        });

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    info!("start controller grpc server!");
    if layer.is_some() {
        info!("metrics on");
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(layer.unwrap())
            .add_service(RpcServiceServer::new(RPCServer::new(controller.clone())))
            .add_service(Consensus2ControllerServiceServer::new(
                Consensus2ControllerServer::new(controller.clone()),
            ))
            .add_service(NetworkMsgHandlerServiceServer::new(
                ControllerNetworkMsgHandlerServer::new(controller.clone()),
            ))
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                health_check_timeout,
            )))
            .serve(addr)
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {} ", e.to_string());
                StatusCodeEnum::FatalError
            })?;
    } else {
        info!("metrics off");
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .add_service(RpcServiceServer::new(RPCServer::new(controller.clone())))
            .add_service(Consensus2ControllerServiceServer::new(
                Consensus2ControllerServer::new(controller.clone()),
            ))
            .add_service(NetworkMsgHandlerServiceServer::new(
                ControllerNetworkMsgHandlerServer::new(controller.clone()),
            ))
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                health_check_timeout,
            )))
            .serve(addr)
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {} ", e.to_string());
                StatusCodeEnum::FatalError
            })?;
    }

    Ok(())
}
