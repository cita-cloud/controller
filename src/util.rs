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

use cita_cloud_proto::blockchain::{
    Block, BlockHeader, CompactBlock, CompactBlockBody, RawTransactions,
};
use cita_cloud_proto::consensus::consensus_service_client::ConsensusServiceClient;
use cita_cloud_proto::executor::executor_service_client::ExecutorServiceClient;
use cita_cloud_proto::kms::{
    kms_service_client::KmsServiceClient, HashDataRequest, RecoverSignatureRequest,
    VerifyDataHashRequest,
};
use cita_cloud_proto::network::{
    network_service_client::NetworkServiceClient, NetworkStatusResponse,
};
use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, Content, ExtKey};
use log::{info, warn};
use tonic::Request;

use crate::utxo_set::SystemConfig;
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::RawTransaction;
use cita_cloud_proto::common::{ConsensusConfiguration, Empty, Hash, Proposal, ProposalWithProof};
use prost::Message;
use std::path::Path;
use tokio::fs;
use tonic::Code;

pub fn unix_now() -> u64 {
    let d = ::std::time::UNIX_EPOCH.elapsed().unwrap();
    d.as_secs() * 1_000 + u64::from(d.subsec_millis())
}

pub async fn reconfigure(
    consensus_port: u16,
    height: u64,
    sys_config: SystemConfig,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(ConsensusConfiguration {
        height,
        block_interval: sys_config.block_interval,
        validators: sys_config.validators,
    });

    let response = client.reconfigure(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn check_block(
    consensus_port: u16,
    height: u64,
    data: Vec<u8>,
    proof: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let proposal = Some(Proposal { height, data });
    let request = Request::new(ProposalWithProof { proposal, proof });

    let response = client.check_block(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn verify_tx_signature(
    kms_port: u16,
    tx_hash: Vec<u8>,
    signature: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    let mut client = KmsServiceClient::connect(kms_addr).await?;

    let request = Request::new(RecoverSignatureRequest {
        msg: tx_hash,
        signature,
    });

    match client.recover_signature(request).await {
        Ok(response) => Ok(response.into_inner().address),
        Err(e) => {
            if e.code() == Code::InvalidArgument {
                Ok(vec![])
            } else {
                Err(Box::new(e))
            }
        }
    }
}

pub async fn verify_tx_hash(
    kms_port: u16,
    tx_hash: Vec<u8>,
    tx_bytes: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    let mut client = KmsServiceClient::connect(kms_addr).await?;

    let request = Request::new(VerifyDataHashRequest {
        data: tx_bytes,
        hash: tx_hash,
    });

    let response = client.verify_data_hash(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn hash_data(
    kms_port: u16,
    data: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    let mut client = KmsServiceClient::connect(kms_addr).await?;

    let request = Request::new(HashDataRequest { data });

    let response = client.hash_data(request).await?;
    Ok(response.into_inner().hash)
}

pub async fn store_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(Content { region, key, value });

    let response = client.store(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn load_data(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(ExtKey { region, key });

    let response = client.load(request).await?;

    Ok(response.into_inner().value)
}

pub async fn load_data_maybe_empty(
    storage_port: u16,
    region: u32,
    key: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(ExtKey { region, key });

    match client.load(request).await {
        Ok(response) => Ok(response.into_inner().value),
        Err(e) => {
            if e.code() == Code::NotFound {
                Ok(vec![])
            } else {
                Err(Box::new(e))
            }
        }
    }
}

pub async fn get_full_block(
    compact_block: CompactBlock,
    proof: Vec<u8>,
) -> Result<Block, Box<dyn std::error::Error + Send + Sync>> {
    let mut body = Vec::new();
    if let Some(compact_body) = compact_block.body {
        for hash in compact_body.tx_hashes {
            if let Some(tx) = get_tx(&hash).await {
                body.push(tx);
            } else {
                panic!("can't get tx: {}", hex::encode(hash));
            }
        }
    }

    Ok(Block {
        version: compact_block.version,
        header: compact_block.header,
        body: Some(RawTransactions { body }),
        proof,
    })
}

pub fn full_to_compact(block: Block) -> CompactBlock {
    let mut compact_body = CompactBlockBody { tx_hashes: vec![] };

    if let Some(body) = block.body {
        for raw_tx in body.body {
            match raw_tx.tx {
                Some(Tx::NormalTx(normal_tx)) => {
                    compact_body.tx_hashes.push(normal_tx.transaction_hash)
                }
                Some(Tx::UtxoTx(utxo_tx)) => compact_body.tx_hashes.push(utxo_tx.transaction_hash),
                None => {}
            }
        }
    }

    CompactBlock {
        version: block.version,
        header: block.header,
        body: Some(compact_body),
    }
}

pub async fn header_to_block_hash(
    kms_port: u16,
    block_header: BlockHeader,
) -> Result<Vec<u8>, crate::error::Error> {
    let mut block_header_bytes = Vec::new();
    block_header
        .encode(&mut block_header_bytes)
        .map_err(|_| crate::error::Error::EncodeError(format!("encode block header failed")))?;

    hash_data(kms_port, block_header_bytes)
        .await
        .map_err(crate::error::Error::InternalError)
}

pub async fn exec_block(
    executor_port: u16,
    block: Block,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let executor_addr = format!("http://127.0.0.1:{}", executor_port);
    let mut client = ExecutorServiceClient::connect(executor_addr).await?;

    let request = Request::new(block);

    let response = client.exec(request).await?;
    Ok(response.into_inner().hash)
}

pub async fn get_network_status(
    network_port: u16,
) -> Result<NetworkStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let network_addr = format!("http://127.0.0.1:{}", network_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(Empty {});

    let response = client.get_network_status(request).await?;
    Ok(response.into_inner())
}

pub fn print_main_chain(chain: &[Vec<u8>], block_number: u64) {
    for (i, hash) in chain.iter().enumerate() {
        info!(
            "height: {} hash {}",
            i as u64 + block_number + 1,
            hex::encode(&hash)
        );
    }
}

pub async fn write_tx(tx_hash: &[u8], data: &[u8]) {
    let filename = format!("{}", hex::encode(tx_hash));

    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);
    let _ = fs::write(file_path, data).await;
}

pub fn check_tx_exists(tx_hash: &[u8]) -> bool {
    let filename = hex::encode(tx_hash);
    let new_filename = format!("new_{}", filename);

    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);
    let new_file_path = root_path.join("txs").join(new_filename);

    file_path.exists() || new_file_path.exists()
}

pub async fn get_tx(tx_hash: &[u8]) -> Option<RawTransaction> {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_path = root_path.join("txs").join(filename);

    let ret = fs::read(tx_path).await;
    if ret.is_err() {
        warn!("read tx file failed: {:?}", ret);
        return None;
    }
    let content = ret.unwrap();
    let ret = RawTransaction::decode(content.as_slice());
    if ret.is_err() {
        warn!("decode tx file failed: {:?}", ret);
        return None;
    }
    Some(ret.unwrap())
}

pub fn extract_tx_hash(raw_tx: RawTransaction) -> Option<Hash> {
    match raw_tx.tx {
        Some(Tx::NormalTx(normal_tx)) => Some(Hash {
            hash: normal_tx.transaction_hash,
        }),
        Some(Tx::UtxoTx(utxo_tx)) => Some(Hash {
            hash: utxo_tx.transaction_hash,
        }),
        None => None,
    }
}

pub async fn remove_tx(filename: &str) {
    let root_path = Path::new(".");
    let tx_path = root_path.join("txs").join(filename);
    let _ = fs::remove_file(tx_path).await;
}

pub async fn store_tx_info(tx_hash: &[u8], block_height: u64, tx_index: usize) {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_info_path = root_path.join("tx_infos").join(filename);

    let mut data = block_height.to_be_bytes().to_vec();
    data.extend_from_slice(&tx_index.to_be_bytes().to_vec());

    let _ = fs::write(tx_info_path, data).await;
}

pub async fn load_tx_info(tx_hash: &[u8]) -> Option<(u64, u64)> {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let tx_info_path = root_path.join("tx_infos").join(filename);

    let ret = fs::read(tx_info_path).await;
    if ret.is_err() {
        warn!("read tx info file failed: {:?}", ret);
        return None;
    }
    let data = ret.unwrap();

    if data.len() != 16 {
        warn!("tx info data invalid");
        return None;
    }

    let mut buf: [u8; 8] = [0; 8];

    buf[..8].clone_from_slice(&data[..8]);
    let block_height = u64::from_be_bytes(buf);

    buf[..8].clone_from_slice(&data[8..]);
    let tx_index = u64::from_be_bytes(buf);

    Some((block_height, tx_index))
}

pub async fn write_block(
    height: u64,
    block_header_bytes: &[u8],
    block_body_bytes: &[u8],
    proof_bytes: &[u8],
) {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let block_path = root_path.join("blocks").join(filename);

    let header_len = block_header_bytes.len();
    let body_len = block_body_bytes.len();
    let proof_len = proof_bytes.len();

    let mut bytes = header_len.to_be_bytes().to_vec();
    bytes.extend_from_slice(&body_len.to_be_bytes().to_vec());
    bytes.extend_from_slice(&proof_len.to_be_bytes().to_vec());
    bytes.extend_from_slice(&block_header_bytes);
    bytes.extend_from_slice(&block_body_bytes);
    bytes.extend_from_slice(&proof_bytes);

    let _ = fs::write(block_path, bytes).await;
}

pub fn check_block_exists(height: u64) -> bool {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let file_path = root_path.join("blocks").join(filename);

    file_path.exists()
}

pub async fn get_compact_block(height: u64) -> Option<(CompactBlock, Vec<u8>)> {
    let filename = format!("{}", height);
    let root_path = Path::new(".");
    let block_path = root_path.join("blocks").join(filename);

    let ret = fs::read(block_path).await;
    if ret.is_err() {
        return None;
    }
    let bytes = ret.unwrap();

    let bytes_len = bytes.len();
    if bytes_len <= 24 {
        warn!("get_compact_block {} bad bytes length", height);
        return None;
    }

    let mut len_bytes: [u8; 8] = [0; 8];
    // parse header_len
    len_bytes[..8].clone_from_slice(&bytes[..8]);
    let header_len = usize::from_be_bytes(len_bytes);
    // parse body_len
    len_bytes[..8].clone_from_slice(&bytes[8..16]);
    let body_len = usize::from_be_bytes(len_bytes);
    // parse proof_len
    len_bytes[..8].clone_from_slice(&bytes[16..24]);
    let proof_len = usize::from_be_bytes(len_bytes);

    if bytes_len != 24 + header_len + body_len + proof_len {
        warn!("get_compact_block {} bad bytes total length", height);
        return None;
    }

    let mut start: usize = 24;
    let header_slice = &bytes[start..start + header_len];
    start += header_len;
    let body_slice = &bytes[start..start + body_len];
    start += body_len;
    let proof_slice = &bytes[start..start + proof_len];

    let ret = BlockHeader::decode(header_slice);
    if ret.is_err() {
        warn!("get_compact_block {} decode BlockHeader failed", height);
        return None;
    }
    let block_header = ret.unwrap();

    let ret = CompactBlockBody::decode(body_slice);
    if ret.is_err() {
        warn!(
            "get_compact_block {} decode CompactBlockBody failed",
            height
        );
        return None;
    }
    let block_body = ret.unwrap();

    let block = CompactBlock {
        version: 0,
        header: Some(block_header),
        body: Some(block_body),
    };

    Some((block, proof_slice.to_vec()))
}

#[macro_export]
macro_rules! impl_multicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, port: u16, item: $type) {
            let nodes = self.node_manager.grab_node().await;

            let network_addr = format!("http://127.0.0.1:{}", port);

            let client =
                cita_cloud_proto::network::network_service_client::NetworkServiceClient::connect(
                    network_addr,
                )
                .await
                .unwrap();

            for node in nodes {
                log::info!("send {}: {:?} to {:x?}", $name, item.clone(), node);

                let mut client = client.clone();

                let origin = {
                    match self.node_manager.get_origin(node.clone()).await {
                        Some(id) => id,
                        None => {
                            log::warn!("not get address: {:x?} origin", node);
                            return;
                        }
                    }
                };

                let mut buf = Vec::new();

                item.encode(&mut buf)
                    .expect(&($name.to_string() + " encode failed"));

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
                            log::warn!("multicast {} to {:x?} failed: {:?}", $name, node, status)
                        }
                    }
                });
            }
        }
    };
}

#[macro_export]
macro_rules! impl_unicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, port: u16, origin: u64, item: $type) {
            let node = self.node_manager.get_address(origin).await;

            let network_addr = format!("http://127.0.0.1:{}", port);

            let mut client =
                cita_cloud_proto::network::network_service_client::NetworkServiceClient::connect(
                    network_addr,
                )
                .await
                .unwrap();

            log::info!("send {}: {:?} to {:x?}", $name, item.clone(), node);

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

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
                        log::warn!("unicast {} to {:x?} failed: {:?}", $name, node, status)
                    }
                }
            });
        }
    };
}

#[macro_export]
macro_rules! impl_broadcast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, port: u16, item: $type) {
            let network_addr = format!("http://127.0.0.1:{}", port);

            let mut client =
                cita_cloud_proto::network::network_service_client::NetworkServiceClient::connect(
                    network_addr,
                )
                .await
                .unwrap();

            log::info!("broadcast {}: {:?}", $name, item.clone());

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

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
            });
        }
    };
}

pub fn clean_0x(s: &str) -> &str {
    if s.starts_with("0x") {
        &s[2..]
    } else {
        s
    }
}
