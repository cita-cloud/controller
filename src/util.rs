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

use cita_cloud_proto::blockchain::{Block, CompactBlock, RawTransactions};
use cita_cloud_proto::consensus::consensus_service_client::ConsensusServiceClient;
use cita_cloud_proto::executor::executor_service_client::ExecutorServiceClient;
// use cita_cloud_proto::kms::{
//     kms_service_client::KmsServiceClient, HashDataRequest, RecoverSignatureRequest,
//     VerifyDataHashRequest,
// };
use cita_cloud_proto::network::{
    network_service_client::NetworkServiceClient, NetworkStatusResponse,
};
use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, ExtKey};
use log::{info, warn};
use tonic::Request;

use crate::config::{controller_config, ControllerConfig};
use cita_cloud_proto::blockchain::RawTransaction;
use cita_cloud_proto::common::{ConsensusConfiguration, Empty, Proposal, ProposalWithProof};
use cita_cloud_proto::kms::kms_service_client::KmsServiceClient;
use cloud_util::common::get_tx_hash;
use cloud_util::crypto::{hash_data, pk2address, recover_signature};
use cloud_util::storage::load_data;
use prost::Message;
use status_code::StatusCode;
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

pub static CONSENSUS_CLIENT: OnceCell<ConsensusServiceClient<Channel>> = OnceCell::const_new();
pub static STORAGE_CLIENT: OnceCell<StorageServiceClient<Channel>> = OnceCell::const_new();
pub static EXECUTOR_CLIENT: OnceCell<ExecutorServiceClient<Channel>> = OnceCell::const_new();
pub static NETWORK_CLIENT: OnceCell<NetworkServiceClient<Channel>> = OnceCell::const_new();
pub static KMS_CLIENT: OnceCell<KmsServiceClient<Channel>> = OnceCell::const_new();

// This must be called before access to clients.
pub fn init_grpc_client(config: &ControllerConfig) {
    CONSENSUS_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.consensus_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            ConsensusServiceClient::new(channel)
        })
        .unwrap();
    STORAGE_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.storage_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            StorageServiceClient::new(channel)
        })
        .unwrap();
    EXECUTOR_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.executor_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            ExecutorServiceClient::new(channel)
        })
        .unwrap();
    NETWORK_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.network_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            NetworkServiceClient::new(channel)
        })
        .unwrap();
    KMS_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.kms_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            KmsServiceClient::new(channel)
        })
        .unwrap();
}

pub fn consensus_client() -> ConsensusServiceClient<Channel> {
    CONSENSUS_CLIENT.get().cloned().unwrap()
}

pub fn storage_client() -> StorageServiceClient<Channel> {
    STORAGE_CLIENT.get().cloned().unwrap()
}

pub fn executor_client() -> ExecutorServiceClient<Channel> {
    EXECUTOR_CLIENT.get().cloned().unwrap()
}

pub fn network_client() -> NetworkServiceClient<Channel> {
    NETWORK_CLIENT.get().cloned().unwrap()
}

pub fn kms_client() -> KmsServiceClient<Channel> {
    KMS_CLIENT.get().cloned().unwrap()
}

pub async fn reconfigure(consensus_config: ConsensusConfiguration) -> StatusCode {
    let request = Request::new(consensus_config);

    match consensus_client().reconfigure(request).await {
        Ok(response) => StatusCode::from(response.into_inner()),
        Err(e) => {
            warn!("reconfigure failed: {}", e.to_string());
            StatusCode::ConsensusServerNotReady
        }
    }
}

pub async fn check_block(height: u64, data: Vec<u8>, proof: Vec<u8>) -> StatusCode {
    let mut client = consensus_client();

    let proposal = Some(Proposal { height, data });
    let request = Request::new(ProposalWithProof { proposal, proof });

    match client.check_block(request).await {
        Ok(respond) => StatusCode::from(respond.into_inner()),
        Err(e) => {
            warn!("check_block failed: {}", e.to_string());
            StatusCode::ConsensusServerNotReady
        }
    }
}

pub async fn verify_tx_signature(tx_hash: &[u8], signature: &[u8]) -> Result<Vec<u8>, StatusCode> {
    let config = controller_config();
    if signature.len() != config.signature_len as usize {
        warn!(
            "signature len is not correct, item len: {}, correct len: {}",
            signature.len(),
            config.signature_len
        );
        Err(StatusCode::SigLenError)
    } else {
        pk2address(
            kms_client(),
            &recover_signature(kms_client(), signature, tx_hash).await?,
        )
        .await
    }
}

pub async fn verify_tx_hash(tx_hash: &[u8], tx_bytes: &[u8]) -> Result<(), StatusCode> {
    let config = controller_config();
    if tx_hash.len() != config.hash_len as usize {
        warn!(
            "tx_hash len is not correct, item len: {}, correct len: {}",
            tx_hash.len(),
            config.hash_len
        );
        Err(StatusCode::HashLenError)
    } else {
        let computed_hash = hash_data(kms_client(), tx_bytes).await?;
        if tx_hash != computed_hash {
            warn!(
                "tx_hash is not consistent, item hash: {}, computed hash: {}",
                hex::encode(tx_hash),
                hex::encode(&hash_data(kms_client(), tx_bytes).await?)
            );
            Err(StatusCode::HashCheckError)
        } else {
            Ok(())
        }
    }
}

pub async fn load_data_maybe_empty(region: u32, key: Vec<u8>) -> Result<Vec<u8>, StatusCode> {
    let mut client = storage_client();

    let request = Request::new(ExtKey { region, key });

    client.load(request).await.map_or_else(
        |e| {
            warn!("load_data_maybe_empty failed: {:?}", e);
            Err(StatusCode::StorageServerNotReady)
        },
        |response| {
            let value = response.into_inner();
            StatusCode::from(value.status.ok_or(StatusCode::NoneStatusCode)?).is_success()?;
            Ok(value.value)
        },
    )
}

pub async fn get_full_block(
    compact_block: CompactBlock,
    proof: Vec<u8>,
) -> Result<Block, StatusCode> {
    let mut body = Vec::new();
    if let Some(compact_body) = compact_block.body {
        for hash in compact_body.tx_hashes {
            let raw_tx = db_get_tx(&hash).await?;
            body.push(raw_tx);
        }
    }

    Ok(Block {
        version: compact_block.version,
        header: compact_block.header,
        body: Some(RawTransactions { body }),
        proof,
    })
}

pub async fn exec_block(block: Block) -> Result<Vec<u8>, StatusCode> {
    let mut client = executor_client();
    let request = Request::new(block);
    let response = client.exec(request).await.map_err(|e| {
        warn!("exec_block failed: {}", e.to_string());
        StatusCode::ExecuteServerNotReady
    })?;
    Ok(response
        .into_inner()
        .hash
        .ok_or(StatusCode::NoneHashResult)?
        .hash)
}

pub async fn get_network_status() -> Result<NetworkStatusResponse, StatusCode> {
    let mut client = network_client();
    let request = Request::new(Empty {});
    let response = client.get_network_status(request).await.map_err(|e| {
        warn!("get_network_status failed: {}", e.to_string());
        StatusCode::NetworkServerNotReady
    })?;
    Ok(response.into_inner())
}

pub fn print_main_chain(chain: &[Vec<u8>], block_number: u64) {
    for (i, hash) in chain.iter().enumerate() {
        info!(
            "height: {} hash 0x{}",
            i as u64 + block_number + 1,
            hex::encode(&hash)
        );
    }
}

pub async fn db_get_tx(tx_hash: &[u8]) -> Result<RawTransaction, StatusCode> {
    let tx_hash_bytes = tx_hash.to_vec();

    let tx_bytes = load_data(storage_client(), 1, tx_hash_bytes)
        .await
        .map_err(|e| {
            warn!(
                "load tx(0x{} failed, error: {})",
                hex::encode(tx_hash),
                e.to_string()
            );
            StatusCode::NoTransaction
        })?;

    let raw_tx = RawTransaction::decode(tx_bytes.as_slice()).map_err(|_| {
        warn!("db_get_tx: decode RawTransaction failed");
        StatusCode::DecodeError
    })?;

    Ok(raw_tx)
}

pub fn get_tx_hash_list(raw_txs: &RawTransactions) -> Result<Vec<Vec<u8>>, StatusCode> {
    let mut hashes = Vec::new();
    for raw_tx in &raw_txs.body {
        hashes.push(get_tx_hash(raw_tx)?.to_vec())
    }
    Ok(hashes)
}

pub async fn load_tx_info(tx_hash: &[u8]) -> Result<(u64, u64), StatusCode> {
    let tx_hash_bytes = tx_hash.to_vec();

    let height_bytes = load_data(storage_client(), 7, tx_hash_bytes.clone())
        .await
        .map_err(|e| {
            warn!(
                "load tx(0x{}) block height failed, error: {}",
                hex::encode(tx_hash),
                e.to_string()
            );
            StatusCode::NoTxHeight
        })?;

    let tx_index_bytes = load_data(storage_client(), 9, tx_hash_bytes)
        .await
        .map_err(|e| {
            warn!(
                "load tx(0x{}) index failed, error: {}",
                hex::encode(tx_hash),
                e.to_string()
            );
            StatusCode::NoTxIndex
        })?;

    let mut buf: [u8; 8] = [0; 8];

    buf.clone_from_slice(&height_bytes[..8]);
    let block_height = u64::from_be_bytes(buf);

    buf.clone_from_slice(&tx_index_bytes[..8]);
    let tx_index = u64::from_be_bytes(buf);

    Ok((block_height, tx_index))
}

pub async fn get_compact_block(height: u64) -> Result<(CompactBlock, Vec<u8>), StatusCode> {
    let height_bytes = height.to_be_bytes().to_vec();

    let compact_block_bytes = load_data(storage_client(), 10, height_bytes.clone())
        .await
        .map_err(|e| {
            warn!("get compact_block({}) error: {}", height, e.to_string());
            StatusCode::NoBlock
        })?;

    let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
        warn!("get_compact_block: decode CompactBlock failed");
        StatusCode::DecodeError
    })?;

    let proof = load_data(storage_client(), 5, height_bytes)
        .await
        .map_err(|e| {
            warn!("get proof({}) error: {}", height, e.to_string());
            StatusCode::NoProof
        })?;

    Ok((compact_block, proof))
}

#[macro_export]
macro_rules! impl_multicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> Vec<tokio::task::JoinHandle<()>> {
            let nodes = self.node_manager.grab_node().await;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            let mut handle_vec = Vec::new();

            for node in nodes {
                log::debug!(
                    "multicast {} len: {} to 0x{}",
                    $name,
                    buf.len(),
                    hex::encode(&node.address)
                );

                let mut client = crate::util::network_client();

                let origin = self.node_manager.get_origin(&node).await.expect(
                    format!("not get address: 0x{} origin", hex::encode(&node.address)).as_str(),
                );

                let msg = cita_cloud_proto::network::NetworkMsg {
                    module: "controller".to_string(),
                    r#type: $name.into(),
                    origin,
                    msg: buf.clone(),
                };

                let request = tonic::Request::new(msg);

                let handle = tokio::spawn(async move {
                    match client.send_msg(request).await {
                        Ok(_) => {
                            log::debug!("multicast {} ok", $name)
                        }
                        Err(status) => {
                            log::warn!(
                                "multicast {} to 0x{} failed: {:?}",
                                $name,
                                hex::encode(&node.address),
                                status
                            )
                        }
                    }
                });

                handle_vec.push(handle);
            }
            handle_vec
        }
    };
}

// todo change return to handle panic & unwrap
#[macro_export]
macro_rules! impl_unicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, origin: u64, item: $type) -> tokio::task::JoinHandle<()> {
            let mut client = crate::util::network_client();

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            log::debug!("unicast {} len: {} to origin[{}]", $name, buf.len(), origin);

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin,
                msg: buf,
            };

            let request = tonic::Request::new(msg);

            tokio::spawn(async move {
                match client.send_msg(request).await {
                    Ok(_) => {}
                    Err(status) => {
                        log::warn!(
                            "unicast {} to origin[{}] failed: {:?}",
                            $name,
                            origin,
                            status
                        )
                    }
                }
            })
        }
    };
}

#[macro_export]
macro_rules! impl_broadcast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> tokio::task::JoinHandle<()> {
            let mut client = crate::util::network_client();

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            log::debug!("broadcast {} buf len: {}", $name, buf.clone().len());

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin: 0,
                msg: buf,
            };

            let request = tonic::Request::new(msg);

            tokio::spawn(async move {
                match client.broadcast(request).await {
                    Ok(_) => {}
                    Err(status) => {
                        log::warn!("broadcast {} failed: {:?}", $name, status)
                    }
                }
            })
        }
    };
}

pub async fn check_sig(sig: &[u8], msg: &[u8], address: &[u8]) -> Result<(), StatusCode> {
    if recover_signature(kms_client(), sig, msg).await? != address {
        Err(StatusCode::SigCheckError)
    } else {
        Ok(())
    }
}
