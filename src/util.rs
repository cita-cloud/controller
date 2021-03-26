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

use cita_cloud_proto::blockchain::{BlockHeader, CompactBlock, CompactBlockBody};
use cita_cloud_proto::consensus::{
    consensus_service_client::ConsensusServiceClient, ConsensusConfiguration,
};
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
use cita_cloud_proto::common::{Empty, ProposalWithProof};
use cita_cloud_proto::controller::RawTransaction;
use prost::Message;
use std::io::{Error, ErrorKind};
use std::path::Path;
use tokio::fs;

pub fn unix_now() -> u64 {
    let d = ::std::time::UNIX_EPOCH.elapsed().unwrap();
    d.as_secs() * 1_000 + u64::from(d.subsec_millis())
}

pub async fn reconfigure(
    consensus_port: u16,
    sys_config: SystemConfig,
) -> Result<bool, Box<dyn std::error::Error>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(ConsensusConfiguration {
        block_interval: sys_config.block_interval,
        validators: sys_config.validators,
    });

    let response = client.reconfigure(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn check_block(
    consensus_port: u16,
    proposal: Vec<u8>,
    proof: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(ProposalWithProof { proposal, proof });

    let response = client.check_block(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn verify_tx_signature(
    kms_port: u16,
    tx_hash: Vec<u8>,
    signature: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    let mut client = KmsServiceClient::connect(kms_addr).await?;

    let request = Request::new(RecoverSignatureRequest {
        msg: tx_hash,
        signature,
    });

    let response = client.recover_signature(request).await?;
    Ok(response.into_inner().address)
}

pub async fn verify_tx_hash(
    kms_port: u16,
    tx_hash: Vec<u8>,
    tx_bytes: Vec<u8>,
) -> Result<bool, Box<dyn std::error::Error>> {
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
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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
) -> Result<bool, Box<dyn std::error::Error>> {
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
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(ExtKey { region, key });

    let response = client.load(request).await?;
    let value = response.into_inner().value;
    if value.is_empty() {
        Err(Box::new(Error::new(ErrorKind::Other, "empty value")))
    } else {
        Ok(value)
    }
}

pub async fn exec_block(
    executor_port: u16,
    block: CompactBlock,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let executor_addr = format!("http://127.0.0.1:{}", executor_port);
    let mut client = ExecutorServiceClient::connect(executor_addr).await?;

    let request = Request::new(block);

    let response = client.exec(request).await?;
    Ok(response.into_inner().hash)
}

pub async fn get_network_status(
    network_port: u16,
) -> Result<NetworkStatusResponse, Box<dyn std::error::Error>> {
    let network_addr = format!("http://127.0.0.1:{}", network_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(Empty {});

    let response = client.get_network_status(request).await?;
    Ok(response.into_inner())
}

pub fn print_main_chain(chain: &[Vec<u8>], block_number: u64) {
    info!("main chain:");
    for (i, hash) in chain.iter().enumerate() {
        info!(
            "height: {} hash 0x{:02x}{:02x}{:02x}..{:02x}{:02x}",
            i as u64 + block_number + 1,
            hash[0],
            hash[1],
            hash[2],
            hash[hash.len() - 2],
            hash[hash.len() - 1]
        );
    }
}

pub async fn write_tx(tx_hash: &[u8], data: &[u8]) {
    let filename = hex::encode(tx_hash);

    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);
    let _ = fs::write(file_path, data).await;
}

pub fn check_tx_exists(tx_hash: &[u8]) -> bool {
    let filename = hex::encode(tx_hash);
    let root_path = Path::new(".");
    let file_path = root_path.join("txs").join(filename);

    file_path.exists()
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

pub async fn remove_tx(filename: &str) {
    let root_path = Path::new(".");
    let tx_path = root_path.join("txs").join(filename);
    let _ = fs::remove_file(tx_path).await;
}

pub async fn write_proposal(block_hash: &[u8], data: &[u8]) {
    let filename = hex::encode(block_hash);

    let root_path = Path::new(".");
    let block_path = root_path.join("proposals").join(filename);
    let _ = fs::write(block_path, data).await;
}

pub fn check_proposal_exists(block_hash: &[u8]) -> bool {
    let filename = hex::encode(block_hash);
    let root_path = Path::new(".");
    let file_path = root_path.join("proposals").join(filename);

    file_path.exists()
}

pub async fn get_proposal(block_hash: &[u8]) -> Option<CompactBlock> {
    let filename = hex::encode(block_hash);
    let root_path = Path::new(".");
    let block_path = root_path.join("proposals").join(filename);

    let ret = fs::read(block_path).await;
    if ret.is_err() {
        warn!("read proposal file failed: {:?}", ret);
        return None;
    }
    let content = ret.unwrap();
    let ret = CompactBlock::decode(content.as_slice());
    if ret.is_err() {
        warn!("decode proposal file failed: {:?}", ret);
        return None;
    }
    Some(ret.unwrap())
}

pub async fn remove_proposal(filename: &str) {
    let root_path = Path::new(".");
    let block_path = root_path.join("proposals").join(filename);

    let _ = fs::remove_file(block_path).await;
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

pub async fn get_block(height: u64) -> Option<(CompactBlock, Vec<u8>)> {
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
        warn!("get_block {} bad bytes length", height);
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
        warn!("get_block {} bad bytes total length", height);
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
        warn!("get_block {} decode BlockHeader failed", height);
        return None;
    }
    let block_header = ret.unwrap();

    let ret = CompactBlockBody::decode(body_slice);
    if ret.is_err() {
        warn!("get_block {} decode CompactBlockBody failed", height);
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
