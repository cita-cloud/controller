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

use cita_ng_proto::common::Empty;
use cita_ng_proto::consensus::{
    consensus_service_client::ConsensusServiceClient, ConsensusConfiguration,
};
use cita_ng_proto::executor::executor_service_client::ExecutorServiceClient;
use cita_ng_proto::kms::{
    kms_service_client::KmsServiceClient, HashDataRequest, RecoverSignatureRequest,
    VerifyDataHashRequest,
};
use cita_ng_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use cita_ng_proto::storage::{
    storage_service_client::StorageServiceClient, Content, ExtKey,
};
use log::info;
use tonic::Request;

use cita_ng_proto::blockchain::{BlockHeader, CompactBlock, CompactBlockBody};
use crate::utxo_set::SystemConfig;

pub fn unix_now() -> u64 {
    let d = ::std::time::UNIX_EPOCH.elapsed().unwrap();
    d.as_secs() * 1_000 + u64::from(d.subsec_millis())
}

pub async fn reconfigure(consensus_port: String, sys_config: SystemConfig) -> Result<bool, Box<dyn std::error::Error>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(ConsensusConfiguration {
        block_interval: sys_config.block_interval,
        validators: sys_config.validators,
    });

    let response = client.reconfigure(request).await?;
    Ok(response.into_inner().is_success)
}

pub async fn verify_tx_signature(
    kms_port: String,
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
    kms_port: String,
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

pub async fn broadcast_message(
    network_port: String,
    msg: NetworkMsg,
) -> Result<(), Box<dyn std::error::Error>> {
    let network_addr = format!("http://127.0.0.1:{}", network_port);
    let mut client = NetworkServiceClient::connect(network_addr).await?;

    let request = Request::new(msg);

    let _ = client.broadcast(request).await?;
    Ok(())
}

pub async fn get_block_delay_number(
    consensus_port: String,
) -> Result<u32, Box<dyn std::error::Error>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(Empty {});

    let response = client.get_block_delay_number(request).await?;
    Ok(response.into_inner().block_delay_number)
}

pub async fn hash_data(
    kms_port: String,
    key_id: u64,
    data: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let kms_addr = format!("http://127.0.0.1:{}", kms_port);
    let mut client = KmsServiceClient::connect(kms_addr).await?;

    let request = Request::new(HashDataRequest { key_id, data });

    let response = client.hash_data(request).await?;
    Ok(response.into_inner().hash)
}

pub async fn store_data(
    storage_port: String,
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
    storage_port: String,
    region: u32,
    key: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let storage_addr = format!("http://127.0.0.1:{}", storage_port);
    let mut client = StorageServiceClient::connect(storage_addr).await?;

    let request = Request::new(ExtKey { region, key });

    let response = client.load(request).await?;
    Ok(response.into_inner().value)
}

pub async fn exec_block(
    executor_port: String,
    block: CompactBlock,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let executor_addr = format!("http://127.0.0.1:{}", executor_port);
    let mut client = ExecutorServiceClient::connect(executor_addr).await?;

    let request = Request::new(block);

    let response = client.exec(request).await?;
    Ok(response.into_inner().hash)
}

pub fn print_main_chain(chain: &[Vec<u8>], block_number: u64) {
    info!("main chain:");
    for (i, hash) in chain.iter().enumerate() {
        info!(
            "height: {} hash 0x{:2x}{:2x}{:2x}..{:2x}{:2x}",
            i as u64 + block_number + 1,
            hash[0],
            hash[1],
            hash[2],
            hash[hash.len() - 2],
            hash[hash.len() - 1]
        );
    }
}

pub fn genesis_block() -> CompactBlock {
    let header = BlockHeader {
        prevhash: vec![],
        timestamp: 123456,
        height: 0,
        transactions_root: vec![],
        proposer: vec![],
        proof: vec![],
        executed_block_hash: vec![],
    };
    let body = CompactBlockBody { tx_hashes: vec![] };
    CompactBlock {
        version: 0,
        header: Some(header),
        body: Some(body),
    }
}

pub fn genesis_block_hash() -> Vec<u8> {
    vec![0u8; 32]
}
