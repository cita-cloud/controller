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

use crate::config::{controller_config, ControllerConfig};
use cita_cloud_proto::{
    blockchain::{
        raw_transaction::{Tx, Tx::UtxoTx},
        Block, CompactBlock, RawTransaction, RawTransactions,
    },
    client::{
        ClientOptions, ConsensusClientTrait, ExecutorClientTrait, InterceptedSvc,
        NetworkClientTrait, StorageClientTrait,
    },
    common::{ConsensusConfiguration, Empty, Proof, Proposal, ProposalWithProof, StateRoot},
    consensus::consensus_service_client::ConsensusServiceClient,
    controller::BlockNumber,
    crypto::crypto_service_client::CryptoServiceClient,
    executor::executor_service_client::ExecutorServiceClient,
    network::{network_service_client::NetworkServiceClient, NetworkStatusResponse},
    retry::RetryClient,
    storage::{storage_service_client::StorageServiceClient, ExtKey, Regions},
};
use cloud_util::{
    common::get_tx_hash,
    crypto::{hash_data, recover_signature},
    storage::load_data,
};
use log::warn;
use prost::Message;
use status_code::StatusCode;
use status_code::StatusCode::{NoneRawTx, NoneTransaction};
use tokio::sync::OnceCell;

pub static CONSENSUS_CLIENT: OnceCell<RetryClient<ConsensusServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static STORAGE_CLIENT: OnceCell<RetryClient<StorageServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static EXECUTOR_CLIENT: OnceCell<RetryClient<ExecutorServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static NETWORK_CLIENT: OnceCell<RetryClient<NetworkServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();
pub static CRYPTO_CLIENT: OnceCell<RetryClient<CryptoServiceClient<InterceptedSvc>>> =
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
    CRYPTO_CLIENT
        .set({
            let client_options = ClientOptions::new(
                CLIENT_NAME.to_string(),
                format!("http://127.0.0.1:{}", config.crypto_port),
            );
            match client_options.connect_crypto() {
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

pub fn network_client() -> RetryClient<NetworkServiceClient<InterceptedSvc>> {
    NETWORK_CLIENT.get().cloned().unwrap()
}

pub fn crypto_client() -> RetryClient<CryptoServiceClient<InterceptedSvc>> {
    CRYPTO_CLIENT.get().cloned().unwrap()
}

pub async fn reconfigure(consensus_config: ConsensusConfiguration) -> StatusCode {
    match consensus_client().reconfigure(consensus_config).await {
        Ok(response) => StatusCode::from(response),
        Err(e) => {
            warn!("reconfigure failed: {}", e.to_string());
            StatusCode::ConsensusServerNotReady
        }
    }
}

pub async fn check_block(height: u64, data: Vec<u8>, proof: Vec<u8>) -> StatusCode {
    let proposal = Some(Proposal { height, data });
    let pp = ProposalWithProof { proposal, proof };

    match consensus_client().check_block(pp).await {
        Ok(code) => StatusCode::from(code),
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
        recover_signature(crypto_client(), signature, tx_hash).await
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
        let computed_hash = hash_data(crypto_client(), tx_bytes).await?;
        if tx_hash != computed_hash {
            warn!(
                "tx_hash is not consistent, item hash: {}, computed hash: {}",
                hex::encode(tx_hash),
                hex::encode(&hash_data(crypto_client(), tx_bytes).await?)
            );
            Err(StatusCode::HashCheckError)
        } else {
            Ok(())
        }
    }
}

pub async fn load_data_maybe_empty(region: u32, key: Vec<u8>) -> Result<Vec<u8>, StatusCode> {
    storage_client()
        .load(ExtKey { region, key })
        .await
        .map_or_else(
            |e| {
                warn!("load_data_maybe_empty failed: {:?}", e);
                Err(StatusCode::StorageServerNotReady)
            },
            |value| match StatusCode::from(value.status.ok_or(StatusCode::NoneStatusCode)?) {
                StatusCode::Success => Ok(value.value),
                StatusCode::NotFound => Ok(vec![]),
                statue => Err(statue),
            },
        )
}

pub async fn get_full_block(height: u64) -> Result<Block, StatusCode> {
    let height_bytes = height.to_be_bytes().to_vec();

    let block_bytes = load_data(
        storage_client(),
        i32::from(Regions::FullBlock) as u32,
        height_bytes,
    )
    .await?;

    Block::decode(block_bytes.as_slice()).map_err(|_| {
        warn!("get_full_block: decode Block failed");
        StatusCode::DecodeError
    })
}

pub async fn exec_block(block: Block) -> (StatusCode, Vec<u8>) {
    match executor_client().exec(block).await {
        Ok(hash_respond) => (
            StatusCode::from(
                hash_respond
                    .status
                    .unwrap_or_else(|| StatusCode::NoneStatusCode.into()),
            ),
            hash_respond
                .hash
                .unwrap_or(cita_cloud_proto::common::Hash { hash: vec![] })
                .hash,
        ),
        Err(e) => {
            warn!("exec_block failed: {}", e.to_string());
            (StatusCode::ExecuteServerNotReady, vec![])
        }
    }
}

pub async fn get_network_status() -> Result<NetworkStatusResponse, StatusCode> {
    let network_status_response = network_client()
        .get_network_status(Empty {})
        .await
        .map_err(|e| {
            warn!("get_network_status failed: {}", e.to_string());
            StatusCode::NetworkServerNotReady
        })?;
    Ok(network_status_response)
}

pub async fn db_get_tx(tx_hash: &[u8]) -> Result<RawTransaction, StatusCode> {
    let tx_hash_bytes = tx_hash.to_vec();

    let tx_bytes = load_data(
        storage_client(),
        i32::from(Regions::Transactions) as u32,
        tx_hash_bytes,
    )
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

    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        tx_hash_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "load tx(0x{}) block height failed, error: {}",
            hex::encode(tx_hash),
            e.to_string()
        );
        StatusCode::NoTxHeight
    })?;

    let tx_index_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionIndex) as u32,
        tx_hash_bytes,
    )
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

pub async fn get_height_by_block_hash(hash: Vec<u8>) -> Result<BlockNumber, StatusCode> {
    let block_number = load_data(
        storage_client(),
        i32::from(Regions::BlockHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "get_height_by_block_hash({}) error: {}",
            hex::encode(hash),
            e.to_string()
        );
        StatusCode::NoBlockHeight
    })
    .map(|v| {
        let mut bytes: [u8; 8] = [0; 8];
        bytes.clone_from_slice(&v[..8]);
        u64::from_be_bytes(bytes)
    })?;
    Ok(BlockNumber { block_number })
}

pub async fn get_compact_block(height: u64) -> Result<CompactBlock, StatusCode> {
    let height_bytes = height.to_be_bytes().to_vec();

    let compact_block_bytes = load_data(
        storage_client(),
        i32::from(Regions::CompactBlock) as u32,
        height_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!("get compact_block({}) error: {}", height, e.to_string());
        StatusCode::NoBlock
    })?;

    let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
        warn!("get_compact_block: decode CompactBlock failed");
        StatusCode::DecodeError
    })?;

    Ok(compact_block)
}

pub async fn get_proof(height: u64) -> Result<Proof, StatusCode> {
    let height_bytes = height.to_be_bytes().to_vec();

    let proof = load_data(
        storage_client(),
        i32::from(Regions::Proof) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get proof({}) error: {}", height, e.to_string());
        StatusCode::NoProof
    })?;

    Ok(Proof { proof })
}

pub async fn get_state_root(height: u64) -> Result<StateRoot, StatusCode> {
    let height_bytes = height.to_be_bytes().to_vec();

    let state_root = load_data(
        storage_client(),
        i32::from(Regions::Result) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get state_root({}) error: {}", height, e.to_string());
        StatusCode::NoStateRoot
    })?;

    Ok(StateRoot { state_root })
}

pub async fn get_hash_in_range(mut hash: Vec<u8>, height: u64) -> Result<Vec<u8>, StatusCode> {
    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .unwrap();
    let mut buf: [u8; 8] = [0; 8];
    buf.clone_from_slice(&height_bytes[..8]);
    let mut tx_height = u64::from_be_bytes(buf);
    while tx_height >= height {
        hash = match load_data(
            storage_client(),
            i32::from(Regions::Transactions) as u32,
            hash.clone(),
        )
        .await
        {
            Ok(raw_tx_bytes) => {
                if let UtxoTx(tx) = RawTransaction::decode(raw_tx_bytes.as_slice())
                    .unwrap()
                    .tx
                    .unwrap()
                {
                    tx.transaction.unwrap().pre_tx_hash
                } else {
                    warn!(
                        "tx from utxo_tx_hash{:?} is not utxo_tx",
                        hex::encode(&hash)
                    );
                    return Err(StatusCode::NoneUtxo);
                }
            }
            Err(status) => {
                warn!("load utxo_tx failed: {}", status);
                return Err(StatusCode::NoTransaction);
            }
        };
        if hash == vec![0u8; 33] {
            tx_height = 0;
        } else {
            let height_bytes = load_data(
                storage_client(),
                i32::from(Regions::TransactionHash2blockHeight) as u32,
                hash.clone(),
            )
            .await
            .unwrap();
            let mut buf: [u8; 8] = [0; 8];
            buf.clone_from_slice(&height_bytes[..8]);
            tx_height = u64::from_be_bytes(buf);
        };
    }
    Ok(hash)
}

#[macro_export]
macro_rules! impl_multicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> Vec<tokio::task::JoinHandle<()>> {
            use cita_cloud_proto::client::NetworkClientTrait;

            let nodes = self.node_manager.grab_node().await;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            let mut handle_vec = Vec::new();

            for node in nodes {
                log::debug!("multicast {} len: {} to {}", $name, buf.len(), node);

                let msg = cita_cloud_proto::network::NetworkMsg {
                    module: "controller".to_string(),
                    r#type: $name.into(),
                    origin: node.0,
                    msg: buf.clone(),
                };

                let handle = tokio::spawn(async move {
                    match $crate::util::network_client().send_msg(msg).await {
                        Ok(_) => {
                            log::debug!("multicast {} ok", $name)
                        }
                        Err(status) => {
                            log::warn!("multicast {} to {} failed: {:?}", $name, node, status)
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
            use cita_cloud_proto::client::NetworkClientTrait;

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

            tokio::spawn(async move {
                match $crate::util::network_client().send_msg(msg).await {
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
            use cita_cloud_proto::client::NetworkClientTrait;

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

            tokio::spawn(async move {
                match $crate::util::network_client().broadcast(msg).await {
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
    if recover_signature(crypto_client(), sig, msg).await? != address {
        Err(StatusCode::SigCheckError)
    } else {
        Ok(())
    }
}

pub fn get_tx_quota(raw_tx: &RawTransaction) -> Result<u64, StatusCode> {
    match &raw_tx.tx {
        Some(Tx::NormalTx(normal_tx)) => match normal_tx.transaction {
            Some(ref tx) => Ok(tx.quota),
            None => {
                warn!("tx_quota: found NoneTransaction");
                Err(NoneTransaction)
            }
        },
        Some(Tx::UtxoTx(_)) => Ok(0),
        None => {
            warn!("tx_quota: found NoneRawTx");
            Err(NoneRawTx)
        }
    }
}
