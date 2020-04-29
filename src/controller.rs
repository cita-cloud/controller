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

use cita_ng_proto::common::Hash;
use cita_ng_proto::blockchain::{CompactBlock, BlockHeader, CompactBlockBody, UtxoTransaction, UnverifiedUtxoTransaction, Witness};
use cita_ng_proto::controller::{RawTransaction, raw_transaction::Tx};
use cita_ng_proto::controller::raw_transaction::Tx::{NormalTx, UtxoTx};
use futures_util::future::TryFutureExt;

#[derive(Clone)]
pub struct Controller {
    consensus_port: String,
    network_port: String,
    storage_port: String,
    kms_port: String,
}

impl Controller {
    pub fn new(consensus_port: String,
               network_port: String,
               storage_port: String,
               kms_port: String) -> Self {
        Controller {
            consensus_port,
            network_port,
            storage_port,
            kms_port
        }
    }

    pub async fn rpc_get_block_number(&self, is_pending: bool) -> Result<u64, String> {
        let latest_pending_block_number = 1u64;
        let delay_block_number = 6u64;
        let block_number = if is_pending {
            latest_pending_block_number
        } else {
            latest_pending_block_number.saturating_sub(delay_block_number)
        };
        Ok(block_number)
    }

    pub async fn rpc_send_raw_transaction(&self, raw_tx: RawTransaction) -> Result<Vec<u8>, String> {
        let tx_hash = vec![0u8];
        Ok(tx_hash)
    }

    pub async fn rpc_get_block_by_hash(&self, hash: Vec<u8>) -> Result<CompactBlock, String> {
        let header = BlockHeader {
            prevhash: vec![],
            timestamp: 123456,
            height: 100,
            transactions_root: vec![],
            proposer: vec![],
            proof: vec![],
            executed_block_hash: vec![],
        };
        let body = CompactBlockBody {
            tx_hashes: vec![],
        };
        let block = CompactBlock {
            version: 0,
            header: Some(header),
            body: Some(body)
        };
        Ok(block)
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, String> {
        Ok(vec![])
    }

    pub async fn rpc_get_block_by_number(&self, block_number: u64) -> Result<CompactBlock, String> {
        self.get_block_hash(block_number).and_then(|block_hash| {
            self.rpc_get_block_by_hash(block_hash)
        }).await
    }

    pub async fn rpc_get_transaction(&self, tx_hash: Vec<u8>) -> Result<RawTransaction, String> {
        let utxo_tx = UtxoTransaction {
            version: 0,
            pre_tx_hash: vec![],
            output: vec![],
            lock_id: 0,
        };
        let witness = Witness {
            transaction_hash: vec![],
            signature: vec![],
            sender: vec![],
        };
        let unverified_utxo_tx = UnverifiedUtxoTransaction {
            transaction: Some(utxo_tx),
            witnesses: vec![witness],
        };
        let raw_tx = RawTransaction {
            tx: Some(UtxoTx(unverified_utxo_tx),
            )
        };

        Ok(raw_tx)
    }
}
