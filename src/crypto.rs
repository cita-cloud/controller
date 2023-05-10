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
    blockchain::{BlockHeader, RawTransactions},
    status_code::StatusCodeEnum,
};
use prost::Message;

use crate::config::controller_config;

#[cfg(feature = "sm")]
pub fn hash_data(data: &[u8]) -> Vec<u8> {
    crypto_sm::sm::hash_data(data)
}

#[cfg(feature = "sm")]
pub fn check_transactions(raw_txs: &RawTransactions) -> StatusCodeEnum {
    crypto_sm::sm::check_transactions(raw_txs)
}

#[cfg(feature = "sm")]
pub fn recover_signature(msg: &[u8], signature: &[u8]) -> Result<Vec<u8>, StatusCodeEnum> {
    let pub_key = crypto_sm::sm::recover_signature(msg, signature)?;
    Ok(crypto_sm::sm::pk2address(&pub_key))
}

#[cfg(feature = "eth")]
pub fn hash_data(data: &[u8]) -> Vec<u8> {
    crypto_eth::eth::hash_data(data)
}

#[cfg(feature = "eth")]
pub fn check_transactions(raw_txs: &RawTransactions) -> StatusCodeEnum {
    crypto_eth::eth::check_transactions(raw_txs)
}

#[cfg(feature = "eth")]
pub fn recover_signature(msg: &[u8], signature: &[u8]) -> Result<Vec<u8>, StatusCodeEnum> {
    let pub_key = crypto_eth::eth::recover_signature(msg, signature)?;
    Ok(crypto_eth::eth::pk2address(&pub_key))
}

pub fn get_block_hash(header: Option<&BlockHeader>) -> Result<Vec<u8>, StatusCodeEnum> {
    match header {
        Some(header) => {
            let mut block_header_bytes = Vec::with_capacity(header.encoded_len());
            header.encode(&mut block_header_bytes).map_err(|_| {
                warn!("get_block_hash: encode block header failed");
                StatusCodeEnum::EncodeError
            })?;
            let block_hash = hash_data(&block_header_bytes);
            Ok(block_hash)
        }
        None => Err(StatusCodeEnum::NoneBlockHeader),
    }
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
        recover_signature(tx_hash, signature)
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
        let computed_hash = hash_data(tx_bytes);
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

pub fn check_sig(sig: &[u8], msg: &[u8], address: &[u8]) -> Result<(), StatusCodeEnum> {
    if recover_signature(msg, sig)? != address {
        Err(StatusCodeEnum::SigCheckError)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_test() {
        #[cfg(feature = "sm")]
        let crypto = crypto_sm::crypto::Crypto::new("example/private_key");
        #[cfg(feature = "eth")]
        let crypto = crypto_eth::crypto::Crypto::new("example/private_key");

        // message must be a hash value
        let message = &hash_data("rivtower".as_bytes());
        let signature = crypto.sign_message(message).unwrap();

        #[cfg(feature = "sm")]
        assert_eq!(signature.len(), crypto_sm::sm::SM2_SIGNATURE_BYTES_LEN);
        #[cfg(feature = "eth")]
        assert_eq!(
            signature.len(),
            crypto_eth::eth::SECP256K1_SIGNATURE_BYTES_LEN
        );

        assert_eq!(
            recover_signature(message, &signature).unwrap(),
            hex::decode("14a0d0eb5538d1c5cc69d0f54c18bafec75ba95c").unwrap()
        );
    }
}
