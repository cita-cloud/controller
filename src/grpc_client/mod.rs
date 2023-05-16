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

pub(crate) mod consensus;
pub(crate) mod executor;
pub(crate) mod network;
pub(crate) mod storage;

use crate::config::ControllerConfig;
use cita_cloud_proto::{
    client::{ClientOptions, InterceptedSvc},
    consensus::consensus_service_client::ConsensusServiceClient,
    evm::rpc_service_client::RpcServiceClient as EvmServiceClient,
    executor::executor_service_client::ExecutorServiceClient,
    network::network_service_client::NetworkServiceClient,
    retry::RetryClient,
    storage::storage_service_client::StorageServiceClient,
};
use tokio::sync::OnceCell;

pub static CONSENSUS_CLIENT: OnceCell<RetryClient<ConsensusServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static STORAGE_CLIENT: OnceCell<RetryClient<StorageServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static EXECUTOR_CLIENT: OnceCell<RetryClient<ExecutorServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static EVM_CLIENT: OnceCell<RetryClient<EvmServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static NETWORK_CLIENT: OnceCell<RetryClient<NetworkServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();

const CLIENT_NAME: &str = "controller";

// This must be called before access to clients.
pub fn init_grpc_client(config: &ControllerConfig) {
    CONSENSUS_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.consensus_port),
            );
            match client_options.connect_consensus() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
    STORAGE_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.storage_port),
            );
            match client_options.connect_storage() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
    EXECUTOR_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.executor_port),
            );
            match client_options.connect_executor() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
    EVM_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.executor_port),
            );
            match client_options.connect_evm() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
    NETWORK_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.network_port),
            );
            match client_options.connect_network() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
}

pub fn consensus_client() -> RetryClient<ConsensusServiceClient<InterceptedSvc>> {
    CONSENSUS_CLIENT.get().cloned().unwrap()
}

pub fn storage_client() -> RetryClient<StorageServiceClient<InterceptedSvc>> {
    STORAGE_CLIENT.get().cloned().unwrap()
}

pub fn executor_client() -> RetryClient<ExecutorServiceClient<InterceptedSvc>> {
    EXECUTOR_CLIENT.get().cloned().unwrap()
}

pub fn evm_client() -> RetryClient<EvmServiceClient<InterceptedSvc>> {
    EVM_CLIENT.get().cloned().unwrap()
}

pub fn network_client() -> RetryClient<NetworkServiceClient<InterceptedSvc>> {
    NETWORK_CLIENT.get().cloned().unwrap()
}
