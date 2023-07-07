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

use crate::crypto::hash_data;
use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx, BlockHeader, RawTransaction, RawTransactions},
    status_code::StatusCodeEnum,
};
use cloud_util::common::get_tx_hash;
use prost::Message;

pub fn get_tx_hash_list(raw_txs: &RawTransactions) -> Result<Vec<Vec<u8>>, StatusCodeEnum> {
    let mut hashes = Vec::new();
    for raw_tx in &raw_txs.body {
        hashes.push(get_tx_hash(raw_tx)?.to_vec())
    }
    Ok(hashes)
}

pub fn u32_decode(data: Vec<u8>) -> u32 {
    u32::from_be_bytes(data.try_into().unwrap())
}

pub fn u64_decode(data: Vec<u8>) -> u64 {
    u64::from_be_bytes(data.try_into().unwrap())
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

pub fn clap_about() -> String {
    let name = env!("CARGO_PKG_NAME").to_string();
    let version = env!("CARGO_PKG_VERSION");
    let authors = env!("CARGO_PKG_AUTHORS");
    name + " " + version + "\n" + authors
}
