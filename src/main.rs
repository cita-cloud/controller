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
mod controller;
mod pool;
mod util;

use clap::Clap;
use git_version::git_version;
use log::{debug, info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/rink1969/cita_ng_controller";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of config service.
    #[clap(short = "c", long = "config_port", default_value = "49999")]
    config_port: String,
    /// Sets grpc port of this service.
    #[clap(short = "p", long = "port", default_value = "50004")]
    grpc_port: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("controller-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of config service: {}", opts.config_port);
            info!("grpc port of this service: {}", opts.grpc_port);
            let _ = run(opts);
        }
    }
}

use cita_ng_proto::config::{
    config_service_client::ConfigServiceClient, Endpoint, RegisterEndpointInfo, ServiceId,
};

async fn register_endpoint(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of Controller service is 4
    let request = Request::new(RegisterEndpointInfo {
        id: 4,
        endpoint: Some(Endpoint {
            hostname: "127.0.0.1".to_owned(),
            port,
        }),
    });

    let response = client.register_endpoint(request).await?;

    Ok(response.into_inner().is_success)
}

async fn get_endpoint(id: u64, config_port: String) -> Result<String, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    let request = Request::new(ServiceId { id });

    let response = client.get_endpoint(request).await?;

    Ok(response.into_inner().port)
}

use cita_ng_proto::network::network_service_client::NetworkServiceClient;
use cita_ng_proto::network::RegisterInfo;

async fn register_network_msg_handler(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of Network service is 0
    let request = Request::new(ServiceId { id: 0 });

    let response = client.get_endpoint(request).await?;

    let network_grpc_port = response.into_inner().port;
    let network_addr = format!("http://127.0.0.1:{}", network_grpc_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(RegisterInfo {
        module_name: "controller".to_owned(),
        hostname: "127.0.0.1".to_owned(),
        port,
    });

    let response = client.register_network_msg_handler(request).await?;

    Ok(response.into_inner().is_success)
}

use cita_ng_proto::blockchain::CompactBlock;
use cita_ng_proto::common::{Empty, Hash, SimpleResponse};
use cita_ng_proto::controller::{
    rpc_service_server::RpcService, rpc_service_server::RpcServiceServer, BlockNumber, Flag,
    RawTransaction,
};
use tonic::{transport::Server, Request, Response, Status};

use std::sync::Arc;
use tokio::sync::RwLock;

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
        info!("get_block_number request: {:?}", request);

        let flag = request.into_inner();
        self.controller
            .rpc_get_block_number(flag.flag)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
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
        info!("send_raw_transaction request: {:?}", request);

        let raw_tx = request.into_inner();

        self.controller
            .rpc_send_raw_transaction(raw_tx)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
                |tx_hash| {
                    let reply = Response::new(Hash { hash: tx_hash });
                    Ok(reply)
                },
            )
    }
    async fn get_block_by_hash(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<CompactBlock>, Status> {
        info!("send_raw_transaction request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_block_by_hash(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
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
        info!("get_block_by_number request: {:?}", request);

        let block_number = request.into_inner();

        self.controller
            .rpc_get_block_by_number(block_number.block_number)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
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
        info!("get_block_by_number request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_transaction(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
                |raw_tx| {
                    let reply = Response::new(raw_tx);
                    Ok(reply)
                },
            )
    }
}

use cita_ng_proto::controller::{
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
    async fn get_proposal(&self, request: Request<Empty>) -> Result<Response<Hash>, Status> {
        info!("get_proposal request: {:?}", request);

        self.controller.chain_get_proposal().await.map_or_else(
            |e| Err(Status::internal(e)),
            |hash| {
                let reply = Response::new(Hash { hash });
                Ok(reply)
            },
        )
    }
    async fn check_proposal(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<SimpleResponse>, Status> {
        info!("get_proposal request: {:?}", request);

        let hash = request.into_inner().hash;

        self.controller
            .chain_check_proposal(hash)
            .await
            .map_or_else(
                |e| Err(Status::internal(e)),
                |is_ok| {
                    let reply = Response::new(SimpleResponse { is_success: is_ok });
                    Ok(reply)
                },
            )
    }
    async fn commit_block(&self, request: Request<Hash>) -> Result<Response<Empty>, Status> {
        info!("commit_block request: {:?}", request);

        let hash = request.into_inner().hash;

        self.controller.chain_commit_block(hash).await.map_or_else(
            |e| Err(Status::internal(e)),
            |_| Ok(Response::new(Empty {})),
        )
    }
}

use cita_ng_proto::network::{
    network_msg_handler_service_server::NetworkMsgHandlerService,
    network_msg_handler_service_server::NetworkMsgHandlerServiceServer, NetworkMsg,
};

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
    ) -> Result<Response<SimpleResponse>, Status> {
        info!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "controller" {
            Err(Status::invalid_argument("wrong module"))
        } else {
            self.controller.process_network_msg(msg).await.map_or_else(
                |e| Err(Status::internal(e)),
                |_| {
                    let reply = SimpleResponse { is_success: true };
                    Ok(Response::new(reply))
                },
            )
        }
    }
}

use crate::controller::Controller;
use crate::util::get_block_delay_number;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    let config_port_clone = opts.config_port.clone();
    let grpc_port_clone = opts.grpc_port.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret =
                    register_endpoint(config_port_clone.clone(), grpc_port_clone.clone()).await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register endpoint success!");
                    break;
                }
            }
            warn!("register endpoint failed! Retrying");
            interval.tick().await;
        }
    });

    let config_port_clone = opts.config_port.clone();
    let grpc_port_clone = opts.grpc_port.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            // register endpoint
            {
                let ret = register_network_msg_handler(
                    config_port_clone.clone(),
                    grpc_port_clone.clone(),
                )
                .await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register network msg handler success!");
                    break;
                }
            }
            warn!("register network msg handler failed! Retrying");
            interval.tick().await;
        }
    });

    let consensus_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of consensus service is 1
            let ret = get_endpoint(1, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get consensus endpoint success!");
                consensus_port = port;
                break;
            }
        }
        warn!("get consensus endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("consensus port: {}", consensus_port);

    let network_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of Network service is 0
            let ret = get_endpoint(0, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get network endpoint success!");
                network_port = port;
                break;
            }
        }
        warn!("get network endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("network port: {}", network_port);

    let storage_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of Storage service is 3
            let ret = get_endpoint(3, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get storage endpoint success!");
                storage_port = port;
                break;
            }
        }
        warn!("get storage endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("storage port: {}", storage_port);

    let kms_port;
    let config_port_clone = opts.config_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            // id of kms service is 5
            let ret = get_endpoint(5, config_port_clone.clone()).await;
            if let Ok(port) = ret {
                info!("get kms endpoint success!");
                kms_port = port;
                break;
            }
        }
        warn!("get kms endpoint failed! Retrying");
        interval.tick().await;
    }
    info!("kms port: {}", kms_port);

    let block_delay_number;
    let consensus_port_clone = consensus_port.clone();
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        {
            let ret = get_block_delay_number(consensus_port_clone.clone()).await;
            if let Ok(number) = ret {
                info!("get block delay number success!");
                block_delay_number = number;
                break;
            }
        }
        warn!("get block delay number failed! Retrying");
        interval.tick().await;
    }
    info!("block delay number: {}", block_delay_number);

    let controller = Controller::new(
        consensus_port,
        network_port,
        storage_port,
        kms_port,
        block_delay_number,
    );

    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

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
        .await?;

    Ok(())
}
