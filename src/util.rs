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

use cita_ng_proto::common::{Empty, SimpleResponse};
use cita_ng_proto::consensus::{
    consensus_service_client::ConsensusServiceClient, ConsensusConfiguration,
};
use cita_ng_proto::kms::{
    kms_service_client::KmsServiceClient, HashDateRequest, RecoverSignatureRequest,
    RecoverSignatureResponse, VerifyDataHashRequest,
};
use cita_ng_proto::network::{network_service_client::NetworkServiceClient, NetworkMsg};
use tonic::Request;

pub async fn reconfigure(consensus_port: String) -> Result<bool, Box<dyn std::error::Error>> {
    let consensus_addr = format!("http://127.0.0.1:{}", consensus_port);
    let mut client = ConsensusServiceClient::connect(consensus_addr).await?;

    let request = Request::new(ConsensusConfiguration {
        block_interval: 3,
        validators: vec![vec![0], vec![1]],
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

    let request = Request::new(HashDateRequest { key_id, data });

    let response = client.hash_date(request).await?;
    Ok(response.into_inner().hash)
}
