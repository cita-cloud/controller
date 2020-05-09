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

use crate::auth::Authentication;
use crate::chain::Chain;
use crate::pool::Pool;
use crate::util::broadcast_message;
use cita_ng_proto::blockchain::{
    BlockHeader, CompactBlock, CompactBlockBody, UnverifiedUtxoTransaction, UtxoTransaction,
    Witness,
};
use cita_ng_proto::common::Hash;
use cita_ng_proto::controller::raw_transaction::Tx::{NormalTx, UtxoTx};
use cita_ng_proto::controller::{raw_transaction::Tx, RawTransaction};
use cita_ng_proto::network::NetworkMsg;
use futures_util::future::TryFutureExt;
use log::{info, warn};
use prost::Message;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Controller {
    consensus_port: String,
    network_port: String,
    storage_port: String,
    kms_port: String,
    block_delay_number: u32,
    auth: Authentication,
    pool: Arc<RwLock<Pool>>,
    chain: Arc<RwLock<Chain>>,
}

impl Controller {
    pub fn new(
        consensus_port: String,
        network_port: String,
        storage_port: String,
        kms_port: String,
        block_delay_number: u32,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
    ) -> Self {
        let auth = Authentication::new(kms_port.clone());
        let pool = Arc::new(RwLock::new(Pool::new(100)));
        let chain = Chain::new(
            storage_port.clone(),
            network_port.clone(),
            kms_port.clone(),
            block_delay_number,
            current_block_number,
            current_block_hash,
            pool.clone(),
        );
        Controller {
            consensus_port,
            network_port,
            storage_port,
            kms_port,
            block_delay_number,
            auth,
            pool,
            chain: Arc::new(RwLock::new(chain)),
        }
    }

    async fn load_genesis(&mut self) -> Result<(), String> {
        Ok(())
    }

    async fn restore_from_storage(&mut self) -> Result<(), String> {
        Ok(())
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

    pub async fn rpc_send_raw_transaction(
        &self,
        raw_tx: RawTransaction,
    ) -> Result<Vec<u8>, String> {
        let tx_hash = self.auth.check_raw_tx(raw_tx.clone()).await?;

        let mut pool = self.pool.write().await;
        let is_ok = pool.enqueue(raw_tx.clone(), tx_hash.clone());
        if is_ok {
            let mut raw_tx_bytes: Vec<u8> = Vec::new();
            let _ = raw_tx.encode(&mut raw_tx_bytes);
            let msg = NetworkMsg {
                module: "controller".to_owned(),
                r#type: "raw_tx".to_owned(),
                origin: 0,
                msg: raw_tx_bytes,
            };
            let _ = broadcast_message(self.network_port.clone(), msg).await;
            Ok(tx_hash)
        } else {
            Err("dup".to_owned())
        }
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
        let body = CompactBlockBody { tx_hashes: vec![] };
        let block = CompactBlock {
            version: 0,
            header: Some(header),
            body: Some(body),
        };
        Ok(block)
    }

    async fn get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, String> {
        Ok(vec![])
    }

    pub async fn rpc_get_block_by_number(&self, block_number: u64) -> Result<CompactBlock, String> {
        self.get_block_hash(block_number)
            .and_then(|block_hash| self.rpc_get_block_by_hash(block_hash))
            .await
    }

    pub async fn rpc_get_transaction(&self, tx_hash: Vec<u8>) -> Result<RawTransaction, String> {
        let utxo_tx = UtxoTransaction {
            version: 0,
            pre_tx_hash: vec![],
            output: vec![],
            lock_id: 0,
        };
        let witness = Witness {
            signature: vec![],
            sender: vec![],
        };
        let unverified_utxo_tx = UnverifiedUtxoTransaction {
            transaction: Some(utxo_tx),
            transaction_hash: vec![],
            witnesses: vec![witness],
        };
        let raw_tx = RawTransaction {
            tx: Some(UtxoTx(unverified_utxo_tx)),
        };

        Ok(raw_tx)
    }

    pub async fn chain_get_proposal(&self) -> Result<Vec<u8>, String> {
        {
            let chain = self.chain.read().await;
            if let Some(proposal) = chain.get_candidate_block_hash() {
                return Ok(proposal);
            }
        }
        let tx_hash_list = {
            let mut pool = self.pool.write().await;
            pool.package()
        };
        {
            let mut chain = self.chain.write().await;
            chain.add_proposal(tx_hash_list).await
        }

        let chain = self.chain.read().await;
        if let Some(proposal) = chain.get_candidate_block_hash() {
            Ok(proposal)
        } else {
            Err("get proposal error".to_owned())
        }
    }

    pub async fn chain_check_proposal(&self, proposal: &[u8]) -> Result<bool, String> {
        let chain = self.chain.read().await;
        let ret = chain.check_proposal(proposal).await;
        Ok(ret)
    }

    pub async fn chain_commit_block(&self, proposal: &[u8]) -> Result<(), String> {
        let mut chain = self.chain.write().await;
        chain.commit_block(proposal).await;
        Ok(())
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<(), String> {
        match msg.r#type.as_str() {
            "raw_tx" => {
                let raw_tx_bytes = msg.msg;
                if let Ok(raw_tx) = RawTransaction::decode(raw_tx_bytes.as_slice()) {
                    self.rpc_send_raw_transaction(raw_tx).await.map(|_| ())
                } else {
                    Err("Decode raw transaction failed".to_owned())
                }
            }
            "block" => {
                info!("get block from network");
                let block_bytes = msg.msg;
                if let Ok(block) = CompactBlock::decode(block_bytes.as_slice()) {
                    if let Some(block_body) = block.clone().body {
                        let tx_hash_list = block_body.tx_hashes;
                        {
                            let pool = self.pool.read().await;
                            for hash in tx_hash_list.iter() {
                                if pool.get_tx(hash).is_none() {
                                    warn!("block is invalid");
                                    return Err("block is invalid".to_owned());
                                }
                            }
                        }
                        {
                            info!("add block");
                            let mut chain = self.chain.write().await;
                            chain.add_block(block).await;
                        }
                        Ok(())
                    } else {
                        warn!("block body is empty");
                        Err("block body is empty".to_owned())
                    }
                } else {
                    warn!("Decode block failed");
                    Err("Decode block failed".to_owned())
                }
            }
            _ => {
                panic!("unknown network message");
            }
        }
    }
}
