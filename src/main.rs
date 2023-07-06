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
mod crypto;
mod event;
mod grpc_client;
mod grpc_server;
mod health_check;
mod system_config;

#[macro_use]
extern crate tracing as logger;

use clap::Parser;
use std::{collections::HashMap, net::AddrParseError, time::Duration};
use tokio::{sync::mpsc, time};
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;

use cita_cloud_proto::{
    controller::consensus2_controller_service_server::Consensus2ControllerServiceServer,
    controller::rpc_service_server::RpcServiceServer, health_check::health_server::HealthServer,
    network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer,
    network::RegisterInfo, status_code::StatusCodeEnum, storage::Regions,
    CONTROLLER_DESCRIPTOR_SET,
};
use cloud_util::{
    metrics::{run_metrics_exporter, MiddlewareLayer},
    network::register_network_msg_handler,
    storage::load_data,
};

use crate::{
    config::ControllerConfig,
    controller::Controller,
    event::EventTask,
    genesis::GenesisBlock,
    grpc_client::{
        init_grpc_client, network_client, storage::load_data_maybe_empty, storage_client,
    },
    grpc_server::{
        consensus_server::Consensus2ControllerServer, network_server::NetworkMsgHandlerServer,
        rpc_server::RPCServer,
    },
    health_check::HealthCheckServer,
    node_manager::{ChainStatus, NodeAddress},
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
    /// private key path
    #[clap(short = 'p', long = "private_key_path", default_value = "private_key")]
    private_key_path: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::Run(opts) => {
            let fin = run(opts);
            if let Err(e) = fin {
                warn!("unreachable: {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCodeEnum> {
    let rx_signal = cloud_util::graceful_shutdown::graceful_shutdown();

    // read consensus-config.toml
    let config = ControllerConfig::new(&opts.config_path);

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
                        .await?;
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
    sys_config.init(&config).await?;

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
        opts.private_key_path.clone(),
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
                let height = controller_for_healthy.get_status().await.height;
                if current_height == u64::MAX {
                    current_height = height;
                    tick = 0;
                } else if height == current_height && tick >= retry_limit {
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
                    controller_for_healthy.set_sync_state(false).await;
                    retry_limit += tick;
                    tick = 0;
                } else if height < current_height {
                    unreachable!()
                } else if height > current_height {
                    // update current height
                    current_height = height;
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
            controller_for_task
                .handle_event(event_task, &mut old_status)
                .await;
        }
    });

    let addr_str = format!("0.0.0.0:{grpc_port}");
    let addr = addr_str.parse().map_err(|e: AddrParseError| {
        warn!("parse grpc listen address failed: {:?} ", e);
        StatusCodeEnum::FatalError
    })?;

    let layer = if config.enable_metrics {
        tokio::spawn(run_metrics_exporter(config.metrics_port));

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(CONTROLLER_DESCRIPTOR_SET)
        .build()
        .map_err(|e| {
            warn!("register grpc reflection failed: {:?} ", e);
            StatusCodeEnum::FatalError
        })?;

    info!("start controller grpc server");
    let http2_keepalive_interval = config.http2_keepalive_interval;
    let http2_keepalive_timeout = config.http2_keepalive_timeout;
    let tcp_keepalive = config.tcp_keepalive;
    let rx_for_grpc = rx_signal.clone();
    if let Some(layer) = layer {
        Server::builder()
            .accept_http1(true)
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(layer)
            .layer(GrpcWebLayer::new())
            .add_service(reflection)
            .add_service(
                RpcServiceServer::new(RPCServer::new(controller.clone()))
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                Consensus2ControllerServiceServer::new(Consensus2ControllerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                NetworkMsgHandlerServiceServer::new(NetworkMsgHandlerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_for_grpc),
            )
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                StatusCodeEnum::FatalError
            })?;
    } else {
        Server::builder()
            .accept_http1(true)
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(GrpcWebLayer::new())
            .add_service(reflection)
            .add_service(
                RpcServiceServer::new(RPCServer::new(controller.clone()))
                    .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                Consensus2ControllerServiceServer::new(Consensus2ControllerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                NetworkMsgHandlerServiceServer::new(NetworkMsgHandlerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_for_grpc),
            )
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                StatusCodeEnum::FatalError
            })?;
    }

    Ok(())
}
