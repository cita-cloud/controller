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
use cita_cloud_proto::common::{proposal_enum, BftProposal, ProposalEnum};
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::{
    blockchain::{
        raw_transaction::{Tx, Tx::UtxoTx},
        Block, CompactBlock, RawTransaction, RawTransactions,
    },
    client::{
        ClientOptions, ConsensusClientTrait, ExecutorClientTrait, InterceptedSvc,
        NetworkClientTrait, StorageClientTrait,
    },
    common::{
        ConsensusConfiguration, Empty, Proof, Proposal, ProposalWithProof, StateRoot,
        TotalNodeNetInfo,
    },
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
use prost::Message;
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

pub async fn reconfigure(consensus_config: ConsensusConfiguration) -> StatusCodeEnum {
    let height = consensus_config.height;
    match consensus_client()
        .reconfigure(consensus_config.clone())
        .await
    {
        Ok(response) => StatusCodeEnum::from(response),
        Err(e) => {
            warn!("reconfigure({}) failed: {}", height, e.to_string());
            StatusCodeEnum::ConsensusServerNotReady
        }
    }
}

pub async fn check_block(height: u64, data: Vec<u8>, proof: Vec<u8>) -> StatusCodeEnum {
    let proposal = Some(Proposal { height, data });
    let pp = ProposalWithProof { proposal, proof };

    match consensus_client().check_block(pp).await {
        Ok(code) => StatusCodeEnum::from(code),
        Err(e) => {
            warn!("check block({}) failed: {}", height, e.to_string());
            StatusCodeEnum::ConsensusServerNotReady
        }
    }
}

pub async fn get_last_stateroot(h: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    let pre_h = h - 1;
    let pre_height_bytes = pre_h.to_be_bytes().to_vec();

    let state_root = load_data(
        storage_client(),
        i32::from(Regions::Result) as u32,
        pre_height_bytes.clone(),
    )
    .await?;

    Ok(state_root)
}

pub async fn assemble_proposal(mut block: Block, height: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    block.proof.clear();
    block.state_root.clear();
    let pre_state_root = get_last_stateroot(height).await?;

    let proposal = ProposalEnum {
        proposal: Some(proposal_enum::Proposal::BftProposal(BftProposal {
            proposal: Some(block),
            pre_state_root,
            pre_proof: vec![],
        })),
    };

    let mut proposal_bytes = Vec::with_capacity(proposal.encoded_len());
    proposal.encode(&mut proposal_bytes).map_err(|_| {
        warn!("encode proposal({}) failed: buffer not sufficient", height);
        StatusCodeEnum::EncodeError
    })?;

    Ok(proposal_bytes)
}

pub async fn verify_tx_signature(
    tx_hash: &[u8],
    signature: &[u8],
) -> Result<Vec<u8>, StatusCodeEnum> {
    let config = controller_config();
    if signature.len() != config.signature_len as usize {
        warn!(
            "verify signature failed: get signature len: {}, correct len: {}. hash: 0x{}",
            signature.len(),
            config.signature_len,
            hex::encode(tx_hash),
        );
        Err(StatusCodeEnum::SigLenError)
    } else {
        recover_signature(crypto_client(), signature, tx_hash).await
    }
}

pub async fn verify_tx_hash(tx_hash: &[u8], tx_bytes: &[u8]) -> Result<(), StatusCodeEnum> {
    let config = controller_config();
    if tx_hash.len() != config.hash_len as usize {
        warn!(
            "verify tx hash failed: get hash len: {}, correct len: {}. hash: 0x{}",
            tx_hash.len(),
            config.hash_len,
            hex::encode(tx_hash),
        );
        Err(StatusCodeEnum::HashLenError)
    } else {
        let computed_hash = hash_data(crypto_client(), tx_bytes).await?;
        if tx_hash != computed_hash {
            warn!(
                "verify tx hash failed: get hash: 0x{}, correct hash: 0x{}",
                hex::encode(tx_hash),
                hex::encode(computed_hash)
            );
            Err(StatusCodeEnum::HashCheckError)
        } else {
            Ok(())
        }
    }
}

pub async fn load_data_maybe_empty(region: u32, key: Vec<u8>) -> Result<Vec<u8>, StatusCodeEnum> {
    storage_client()
        .load(ExtKey { region, key })
        .await
        .map_or_else(
            |e| {
                warn!(
                    "load data maybe empty failed: {:?}. region: {}",
                    e.to_string(),
                    region
                );
                Err(StatusCodeEnum::StorageServerNotReady)
            },
            |value| match StatusCodeEnum::from(value.status.ok_or(StatusCodeEnum::NoneStatusCode)?)
            {
                StatusCodeEnum::Success => Ok(value.value),
                StatusCodeEnum::NotFound => Ok(vec![]),
                statue => Err(statue),
            },
        )
}

pub async fn get_full_block(height: u64) -> Result<Block, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let block_bytes = load_data(
        storage_client(),
        i32::from(Regions::FullBlock) as u32,
        height_bytes,
    )
    .await?;

    Block::decode(block_bytes.as_slice()).map_err(|_| {
        warn!("get full block failed: decode Block failed");
        StatusCodeEnum::DecodeError
    })
}

pub async fn exec_block(block: Block) -> (StatusCodeEnum, Vec<u8>) {
    match executor_client().exec(block).await {
        Ok(hash_respond) => (
            StatusCodeEnum::from(
                hash_respond
                    .status
                    .unwrap_or_else(|| StatusCodeEnum::NoneStatusCode.into()),
            ),
            hash_respond
                .hash
                .unwrap_or(cita_cloud_proto::common::Hash { hash: vec![] })
                .hash,
        ),
        Err(e) => {
            warn!("execute block failed: {}", e.to_string());
            (StatusCodeEnum::ExecuteServerNotReady, vec![])
        }
    }
}

pub async fn get_network_status() -> Result<NetworkStatusResponse, StatusCodeEnum> {
    let network_status_response = network_client()
        .get_network_status(Empty {})
        .await
        .map_err(|e| {
            warn!("get network status failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady
        })?;
    Ok(network_status_response)
}

pub async fn get_peers_info() -> Result<TotalNodeNetInfo, StatusCodeEnum> {
    let peers_info = network_client()
        .get_peers_net_info(Empty {})
        .await
        .map_err(|e| {
            warn!("get peers status failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady
        })?;
    Ok(peers_info)
}

pub async fn db_get_tx(tx_hash: &[u8]) -> Result<RawTransaction, StatusCodeEnum> {
    let tx_hash_bytes = tx_hash.to_vec();

    let tx_bytes = load_data(
        storage_client(),
        i32::from(Regions::Transactions) as u32,
        tx_hash_bytes,
    )
    .await
    .map_err(|e| {
        warn!(
            "db get tx failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTransaction
    })?;

    let raw_tx = RawTransaction::decode(tx_bytes.as_slice()).map_err(|_| {
        warn!(
            "db get tx failed: decode RawTransaction failed. hash: 0x{}",
            hex::encode(tx_hash)
        );
        StatusCodeEnum::DecodeError
    })?;

    Ok(raw_tx)
}

pub fn get_tx_hash_list(raw_txs: &RawTransactions) -> Result<Vec<Vec<u8>>, StatusCodeEnum> {
    let mut hashes = Vec::new();
    for raw_tx in &raw_txs.body {
        hashes.push(get_tx_hash(raw_tx)?.to_vec())
    }
    Ok(hashes)
}

pub async fn load_tx_info(tx_hash: &[u8]) -> Result<(u64, u64), StatusCodeEnum> {
    let tx_hash_bytes = tx_hash.to_vec();

    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        tx_hash_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "load tx height failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTxHeight
    })?;

    let tx_index_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionIndex) as u32,
        tx_hash_bytes,
    )
    .await
    .map_err(|e| {
        warn!(
            "load tx index failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(tx_hash),
        );
        StatusCodeEnum::NoTxIndex
    })?;

    let block_height = u64_decode(height_bytes);
    let tx_index = u64_decode(tx_index_bytes);

    Ok((block_height, tx_index))
}

pub async fn get_height_by_block_hash(hash: Vec<u8>) -> Result<BlockNumber, StatusCodeEnum> {
    let block_number = load_data(
        storage_client(),
        i32::from(Regions::BlockHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .map_err(|e| {
        warn!(
            "get height by block hash failed: {}. hash: 0x{}",
            e.to_string(),
            hex::encode(hash),
        );
        StatusCodeEnum::NoBlockHeight
    })
    .map(u64_decode)?;
    Ok(BlockNumber { block_number })
}

pub async fn get_compact_block(height: u64) -> Result<CompactBlock, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let compact_block_bytes = load_data(
        storage_client(),
        i32::from(Regions::CompactBlock) as u32,
        height_bytes.clone(),
    )
    .await
    .map_err(|e| {
        warn!("get compact block({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoBlock
    })?;

    let compact_block = CompactBlock::decode(compact_block_bytes.as_slice()).map_err(|_| {
        warn!(
            "get compact block({}) failed: decode CompactBlock failed",
            height
        );
        StatusCodeEnum::DecodeError
    })?;

    Ok(compact_block)
}

pub async fn get_proof(height: u64) -> Result<Proof, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let proof = load_data(
        storage_client(),
        i32::from(Regions::Proof) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get proof({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoProof
    })?;

    Ok(Proof { proof })
}

pub async fn get_state_root(height: u64) -> Result<StateRoot, StatusCodeEnum> {
    let height_bytes = height.to_be_bytes().to_vec();

    let state_root = load_data(
        storage_client(),
        i32::from(Regions::Result) as u32,
        height_bytes,
    )
    .await
    .map_err(|e| {
        warn!("get state_root({}) failed: {}", height, e.to_string());
        StatusCodeEnum::NoStateRoot
    })?;

    Ok(StateRoot { state_root })
}

pub async fn get_hash_in_range(mut hash: Vec<u8>, height: u64) -> Result<Vec<u8>, StatusCodeEnum> {
    let height_bytes = load_data(
        storage_client(),
        i32::from(Regions::TransactionHash2blockHeight) as u32,
        hash.clone(),
    )
    .await
    .unwrap();
    let mut tx_height = u64_decode(height_bytes);
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
                        "load utxo_tx failed: not utxo_tx. hash: 0x{}",
                        hex::encode(&hash)
                    );
                    return Err(StatusCodeEnum::NoneUtxo);
                }
            }
            Err(status) => {
                warn!(
                    "load utxo_tx failed: {}. hash: 0x{}",
                    status,
                    hex::encode(&hash)
                );
                return Err(StatusCodeEnum::NoTransaction);
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
            .await?;
            tx_height = u64_decode(height_bytes);
        };
    }
    Ok(hash)
}

pub fn u32_decode(data: Vec<u8>) -> u32 {
    u32::from_be_bytes(data.try_into().unwrap())
}

pub fn u64_decode(data: Vec<u8>) -> u64 {
    u64::from_be_bytes(data.try_into().unwrap())
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
                debug!("multicast {} to {}: len: {}", $name, node, buf.len());

                let msg = cita_cloud_proto::network::NetworkMsg {
                    module: "controller".to_string(),
                    r#type: $name.into(),
                    origin: node.0,
                    msg: buf.clone(),
                };

                let handle = tokio::spawn(async move {
                    match $crate::util::network_client().send_msg(msg).await {
                        Ok(_) => {
                            debug!("multicast {} success", $name)
                        }
                        Err(status) => {
                            warn!("multicast {} to {} failed: {:?}", $name, node, status)
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

            debug!(
                "unicast {} to origin({}): len: {}",
                $name,
                origin,
                buf.len()
            );

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
                        warn!(
                            "unicast {} to origin({}) failed: {:?}",
                            $name, origin, status
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

            debug!("broadcast {}: len: {}", $name, buf.len());

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
                        warn!("broadcast {} failed: {:?}", $name, status)
                    }
                }
            })
        }
    };
}

pub async fn check_sig(sig: &[u8], msg: &[u8], address: &[u8]) -> Result<(), StatusCodeEnum> {
    if recover_signature(crypto_client(), sig, msg).await? != address {
        Err(StatusCodeEnum::SigCheckError)
    } else {
        Ok(())
    }
}

pub fn get_tx_quota(raw_tx: &RawTransaction) -> Result<u64, StatusCodeEnum> {
    match &raw_tx.tx {
        Some(Tx::NormalTx(normal_tx)) => match normal_tx.transaction {
            Some(ref tx) => Ok(tx.quota),
            None => {
                warn!("get tx quota failed: NoneTransaction");
                Err(StatusCodeEnum::NoneTransaction)
            }
        },
        Some(Tx::UtxoTx(_)) => Ok(0),
        None => {
            warn!("get tx quota failed: NoneRawTx");
            Err(StatusCodeEnum::NoneRawTx)
        }
    }
}

pub fn clap_about() -> String {
    let name = env!("CARGO_PKG_NAME").to_string();
    let version = env!("CARGO_PKG_VERSION");
    let authors = env!("CARGO_PKG_AUTHORS");
    name + " " + version + "\n" + authors
}
