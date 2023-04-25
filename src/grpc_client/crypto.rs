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

use cita_cloud_proto::status_code::StatusCodeEnum;
use cloud_util::crypto::{hash_data, recover_signature};

use crate::{config::controller_config, grpc_client::crypto_client};

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

pub async fn check_sig(sig: &[u8], msg: &[u8], address: &[u8]) -> Result<(), StatusCodeEnum> {
    if recover_signature(crypto_client(), sig, msg).await? != address {
        Err(StatusCodeEnum::SigCheckError)
    } else {
        Ok(())
    }
}
