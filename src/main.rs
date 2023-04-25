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
mod pool;
mod protocol;
#[macro_use]
mod util;
mod event;
mod grpc_client;
mod grpc_server;
mod health_check;
mod system_config;

#[macro_use]
extern crate tracing as logger;

use clap::Parser;
use prost::Message;
use std::{cmp::Ordering, collections::HashMap, net::AddrParseError, time::Duration};
use tokio::{sync::mpsc, time};
use tonic::transport::Server;

use cita_cloud_proto::{
    client::CryptoClientTrait, common::Empty,
    controller::consensus2_controller_service_server::Consensus2ControllerServiceServer,
    controller::rpc_service_server::RpcServiceServer, health_check::health_server::HealthServer,
    network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer,
    network::RegisterInfo, status_code::StatusCodeEnum, storage::Regions,
};
use cloud_util::{
    crypto::{hash_data, sign_message},
    metrics::{run_metrics_exporter, MiddlewareLayer},
    network::register_network_msg_handler,
    panic_hook::set_panic_handler,
    storage::{load_data, store_data},
};

use crate::{
    chain::ChainStep,
    config::ControllerConfig,
    controller::Controller,
    event::EventTask,
    genesis::GenesisBlock,
    grpc_client::{
        consensus::reconfigure,
        crypto_client, init_grpc_client, network_client,
        storage::{get_full_block, load_data_maybe_empty},
        storage_client,
    },
    grpc_server::{
        consensus_server::Consensus2ControllerServer, network_server::NetworkMsgHandlerServer,
        rpc_server::RPCServer,
    },
    health_check::HealthCheckServer,
    node_manager::{ChainStatus, ChainStatusInit, NodeAddress},
    protocol::sync_manager::{sync_block_respond::Respond, SyncBlockRespond, SyncBlocks},
    system_config::{
        LOCK_ID_ADMIN, LOCK_ID_BLOCK_INTERVAL, LOCK_ID_BLOCK_LIMIT, LOCK_ID_BUTTON,
        LOCK_ID_CHAIN_ID, LOCK_ID_EMERGENCY_BRAKE, LOCK_ID_QUOTA_LIMIT, LOCK_ID_VALIDATORS,
        LOCK_ID_VERSION,
    },
    util::{clap_about, u64_decode},
};

#[derive(Parser)]
#[clap(version, about = clap_about())]
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
            warn!("unreachable: {:?}", fin);
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCodeEnum> {
    #[cfg(not(windows))]
    tokio::spawn(cloud_util::signal::handle_signals());

    // read consensus-config.toml
    let mut config = ControllerConfig::new(&opts.config_path);

    // init tracer
    cloud_util::tracer::init_tracer(config.domain.clone(), &config.log_config)
        .map_err(|e| println!("tracer init err: {e}"))
        .unwrap();

    init_grpc_client(&config);

    let grpc_port = config.controller_port.to_string();

    info!("controller grpc port: {}", grpc_port);

    info!("health check timeout: {}", config.health_check_timeout);

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
                info!("network service ready");
                break;
            }
            status => warn!(
                "network service not ready: {}. retrying...",
                status.to_string()
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
                            info!("crypto_{} service ready", &crypto_info.name);
                            break;
                        }
                        status => warn!("get crypto info failed: {:?}", status.to_string()),
                    }
                }
            }
        }
        warn!("crypto service not ready: retrying...");
    }

    // load sys_config
    info!("load system config");
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
                    info!("storage service ready, get current height success");
                    if current_block_number_bytes.is_empty() {
                        info!("this is a new chain");
                        current_block_number = 0u64;
                        current_block_hash = genesis.genesis_block_hash().await;
                    } else {
                        info!("this is an old chain");
                        current_block_number = u64_decode(current_block_number_bytes);
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
                Err(e) => warn!("get current height failed: {}", e.to_string()),
            }
        }
        warn!("storage service not ready: retrying...");
    }
    info!(
        "init height: {}, init block hash: 0x{}",
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
                    info!(
                        "update system config by utxo_tx hash: lock_id: {}, utxo_tx hash: 0x{}",
                        lock_id,
                        hex::encode(data_or_tx_hash.clone())
                    );
                    if sys_config
                        .modify_sys_config_by_utxotx_hash(data_or_tx_hash.clone())
                        .await
                        != StatusCodeEnum::Success
                    {
                        panic!("update system config by utxo_tx hash failed: lock_id: {}, utxo_tx hash: 0x{}", lock_id, hex::encode(data_or_tx_hash))
                    }
                } else {
                    info!("update system config by data: lock_id: {}", lock_id);
                    if !sys_config.match_data(lock_id, data_or_tx_hash, true) {
                        panic!("match data failed: lock_id: {lock_id}");
                    }
                }
            }
            Err(StatusCodeEnum::NotFound) => {
                //this lock_id is empty in local, store data from sys_config to local
                info!("update system config by file: lock_id: {}", lock_id);
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
                        warn!(
                            "update system config by file failed: unexpected lock_id: {}",
                            lock_id
                        );
                    }
                };
            }
            Err(e) => panic!("load data failed: {e}. lock_id: {lock_id}"),
        }
    }

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

    config.clone().set_global();
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
                        "inner healthy check: broadcast csi: height: {}, {}th time",
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
                    let mut block_vec = Vec::new();

                    for h in req.start_height..=req.end_height {
                        if let Ok(block) = get_full_block(h).await {
                            block_vec.push(block);
                        } else {
                            warn!("handle SyncBlockReq failed: get block({}) failed", h);
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
                            "send SyncBlockRespond: to origin: {:x}, height: {} - {}",
                            origin, req.start_height, req.end_height
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
                    debug!("receive SyncBlock event");
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
                                        .remove_block(own_status.height + 1)
                                        .await
                                    {
                                        chain.clear_candidate();
                                        match chain.process_block(block).await {
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
                                                    warn!("sync block failed: {}", e.to_string());
                                                    continue;
                                                }
                                                warn!("sync block failed: {}. set remote misbehavior. origin: {}", NodeAddress::from(&addr), e.to_string());
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
                    info!("receive BroadCastCSI event");
                    let status = controller_for_task.get_status().await;

                    let mut chain_status_bytes = Vec::new();
                    status
                        .encode(&mut chain_status_bytes)
                        .map_err(|_| {
                            warn!("process BroadCastCSI failed: encode ChainStatus failed");
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
                                        "node status rollbacked: old height: {}, current height: {}. set it misbehavior. origin: {}",
                                        old_cs.height,
                                        current_cs.height,
                                        &na
                                    );
                                    let _ = controller_for_task
                                        .node_manager
                                        .set_misbehavior_node(na)
                                        .await;
                                }
                                Ordering::Equal => {
                                    warn!(
                                        "node status stale: height: {}. delete it. origin: {}",
                                        old_cs.height, &na
                                    );
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
        warn!("parse grpc listen address failed: {:?} ", e);
        StatusCodeEnum::FatalError
    })?;

    let layer = if config.enable_metrics {
        tokio::spawn(async move {
            run_metrics_exporter(config.metrics_port).await.unwrap();
        });

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    info!("start controller grpc server");
    let http2_keepalive_interval = config.http2_keepalive_interval;
    let http2_keepalive_timeout = config.http2_keepalive_timeout;
    let tcp_keepalive = config.tcp_keepalive;
    if let Some(layer) = layer {
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(layer)
            .add_service(RpcServiceServer::new(RPCServer::new(controller.clone())))
            .add_service(Consensus2ControllerServiceServer::new(
                Consensus2ControllerServer::new(controller.clone()),
            ))
            .add_service(NetworkMsgHandlerServiceServer::new(
                NetworkMsgHandlerServer::new(controller.clone()),
            ))
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve(addr)
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                StatusCodeEnum::FatalError
            })?;
    } else {
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .add_service(RpcServiceServer::new(RPCServer::new(controller.clone())))
            .add_service(Consensus2ControllerServiceServer::new(
                Consensus2ControllerServer::new(controller.clone()),
            ))
            .add_service(NetworkMsgHandlerServiceServer::new(
                NetworkMsgHandlerServer::new(controller.clone()),
            ))
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve(addr)
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                StatusCodeEnum::FatalError
            })?;
    }

    Ok(())
}
