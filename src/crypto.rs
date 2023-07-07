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
    blockchain::{RawTransaction, RawTransactions},
    status_code::StatusCodeEnum,
};

#[cfg(feature = "sm")]
pub use crypto_sm::sm::hash_data;

#[cfg(feature = "sm")]
fn recover_signature(msg: Vec<u8>, signature: Vec<u8>) -> Result<Vec<u8>, StatusCodeEnum> {
    let pub_key = crypto_sm::sm::recover_signature(&msg, &signature)?;
    Ok(crypto_sm::sm::pk2address(&pub_key))
}

#[cfg(feature = "sm")]
use crypto_sm::sm::crypto_check;

#[cfg(feature = "sm")]
use crypto_sm::sm::crypto_check_batch;

#[cfg(feature = "eth")]
pub use crypto_eth::eth::hash_data;

#[cfg(feature = "eth")]
fn recover_signature(msg: &[u8], signature: &[u8]) -> Result<Vec<u8>, StatusCodeEnum> {
    let pub_key = crypto_eth::eth::recover_signature(msg, signature)?;
    Ok(crypto_eth::eth::pk2address(&pub_key))
}

#[cfg(feature = "eth")]
pub use crypto_eth::eth::crypto_check;

#[cfg(feature = "eth")]
pub use crypto_eth::eth::crypto_check_batch;

pub async fn recover_signature_async(
    tx_hash: Vec<u8>,
    signature: Vec<u8>,
) -> Result<Vec<u8>, StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let _ = send.send(recover_signature(tx_hash, signature));
    });
    match recv.await {
        Ok(res) => res,
        Err(e) => {
            warn!("verify signature failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
    }
}

pub async fn crypto_check_async(tx: RawTransaction) -> Result<(), StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let _ = send.send(crypto_check(tx));
    });
    match recv.await {
        Ok(res) => res,
        Err(e) => {
            warn!("crypto check failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
    }
}

pub async fn crypto_check_batch_async(txs: RawTransactions) -> Result<(), StatusCodeEnum> {
    let (send, recv) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = send.send(crypto_check_batch(txs));
    });
    match recv.await {
        Ok(res) => {
            if res.is_success().is_ok() {
                Ok(())
            } else {
                Err(res)
            }
        }
        Err(e) => {
            warn!("crypto check failed: {}", e);
            Err(StatusCodeEnum::SigCheckError)
        }
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
        let message = hash_data("rivtower".as_bytes());
        let signature = crypto.sign_message(&message).unwrap();

        #[cfg(feature = "sm")]
        assert_eq!(signature.len(), crypto_sm::sm::SM2_SIGNATURE_BYTES_LEN);
        #[cfg(feature = "eth")]
        assert_eq!(
            signature.len(),
            crypto_eth::eth::SECP256K1_SIGNATURE_BYTES_LEN
        );

        assert_eq!(
            recover_signature(message, signature).unwrap(),
            hex::decode("14a0d0eb5538d1c5cc69d0f54c18bafec75ba95c").unwrap()
        );
    }
}
