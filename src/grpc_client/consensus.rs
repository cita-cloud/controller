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

use cita_cloud_proto::{
    client::ConsensusClientTrait,
    common::{ConsensusConfiguration, Proposal, ProposalWithProof},
    status_code::StatusCodeEnum,
};

use super::consensus_client;

pub async fn reconfigure(consensus_config: ConsensusConfiguration) -> StatusCodeEnum {
    let height = consensus_config.height;
    match consensus_client().reconfigure(consensus_config).await {
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
