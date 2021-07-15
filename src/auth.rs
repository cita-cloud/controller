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

use crate::util::{get_compact_block, verify_tx_hash, verify_tx_signature};
use crate::utxo_set::{SystemConfig, LOCK_ID_BUTTON, LOCK_ID_VERSION};
use cita_cloud_proto::blockchain::raw_transaction::Tx::{NormalTx, UtxoTx};
use cita_cloud_proto::blockchain::RawTransaction;
use cita_cloud_proto::blockchain::{Transaction, UnverifiedUtxoTransaction, UtxoTransaction};
use prost::Message;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;

pub const BLOCKLIMIT: u64 = 100;

#[derive(Clone)]
pub struct Authentication {
    history_hashes: HashMap<u64, HashSet<Vec<u8>>>,
    current_block_number: u64,
    sys_config: SystemConfig,
}

impl Authentication {
    pub fn new(sys_config: SystemConfig) -> Self {
        Authentication {
            history_hashes: HashMap::new(),
            current_block_number: 0,
            sys_config,
        }
    }

    pub fn get_system_config(&self) -> SystemConfig {
        self.sys_config.clone()
    }

    pub fn update_system_config(&mut self, tx: &UnverifiedUtxoTransaction) -> bool {
        self.sys_config.update(tx, false)
    }

    pub async fn init(&mut self, init_block_number: u64) {
        let begin_block_number = if init_block_number >= BLOCKLIMIT {
            init_block_number - BLOCKLIMIT + 1
        } else {
            1u64
        };

        for h in begin_block_number..(init_block_number + 1) {
            let block = get_compact_block(h).await.unwrap().0;
            let block_body = block.body.unwrap();
            self.history_hashes
                .insert(h, HashSet::from_iter(block_body.tx_hashes));
        }
        self.current_block_number = init_block_number;
    }

    pub fn insert_tx_hash(&mut self, h: u64, hash_list: Vec<Vec<u8>>) {
        self.history_hashes.insert(h, HashSet::from_iter(hash_list));
        if h >= BLOCKLIMIT {
            self.history_hashes.remove(&(h - BLOCKLIMIT));
        }
        if h > self.current_block_number {
            self.current_block_number = h;
        }
    }

    #[allow(clippy::ptr_arg)]
    fn check_tx_hash(&self, tx_hash: &Vec<u8>) -> Result<(), String> {
        for (_h, hash_list) in self.history_hashes.iter() {
            if hash_list.contains(tx_hash) {
                return Err("dup".to_owned());
            }
        }
        Ok(())
    }

    fn check_transaction(&self, tx: &Transaction) -> Result<(), String> {
        if tx.version != self.sys_config.version {
            return Err("Invalid version".to_owned());
        }
        if tx.to.len() != 20 && !tx.to.is_empty() {
            return Err("Invalid to".to_owned());
        }
        if tx.nonce.len() > 128 {
            return Err("Invalid nonce".to_owned());
        }
        if tx.valid_until_block <= self.current_block_number
            || tx.valid_until_block > (self.current_block_number + BLOCKLIMIT)
        {
            return Err("Invalid valid_until_block".to_owned());
        }
        if tx.value.len() != 32 {
            return Err("Invalid value".to_owned());
        }
        if tx.chain_id.len() != 32 || tx.chain_id != self.sys_config.chain_id {
            return Err("Invalid chain_id".to_owned());
        }
        Ok(())
    }

    fn check_utxo_transaction(&self, utxo_tx: &UtxoTransaction) -> Result<(), String> {
        if utxo_tx.version != self.sys_config.version {
            return Err("Invalid version".to_owned());
        }
        let lock_id = utxo_tx.lock_id;
        if !(LOCK_ID_VERSION..LOCK_ID_BUTTON).contains(&lock_id) {
            return Err("Invalid lock_id".to_owned());
        }
        let hash = self.sys_config.utxo_tx_hashes.get(&lock_id).unwrap();
        if hash != &utxo_tx.pre_tx_hash {
            return Err("Invalid pre_tx_hash".to_owned());
        }
        Ok(())
    }

    pub fn check_raw_tx(&self, raw_tx: &RawTransaction) -> Result<Vec<u8>, String> {
        if let Some(tx) = raw_tx.tx.as_ref() {
            match tx {
                NormalTx(normal_tx) => {
                    if normal_tx.witness.is_none() {
                        return Err("witness is none".to_owned());
                    }

                    let witness = normal_tx.witness.as_ref().unwrap();
                    let signature = &witness.signature;
                    let sender = &witness.sender;

                    if self.sys_config.emergency_brake {
                        return Err("forbidden".to_owned());
                    }

                    let mut tx_bytes: Vec<u8> = Vec::new();
                    if let Some(tx) = &normal_tx.transaction {
                        self.check_transaction(&tx)?;
                        let ret = tx.encode(&mut tx_bytes);
                        if ret.is_err() {
                            return Err("encode tx failed".to_owned());
                        }
                    } else {
                        return Err("tx is none".to_owned());
                    }

                    let tx_hash = &normal_tx.transaction_hash;

                    self.check_tx_hash(&tx_hash)?;

                    verify_tx_hash(&tx_hash, &tx_bytes).map_err(|e| e.to_string())?;

                    if &verify_tx_signature(&tx_hash, &signature).map_err(|e| e.to_string())?
                        == sender
                    {
                        Ok(tx_hash.clone())
                    } else {
                        Err("Invalid sender".to_owned())
                    }
                }
                UtxoTx(utxo_tx) => {
                    let witnesses = &utxo_tx.witnesses;

                    // limit witnesses length is 1
                    if witnesses.len() != 1 {
                        return Err("invalid witnesses".to_owned());
                    }

                    // only admin can send utxo tx
                    if witnesses[0].sender != self.sys_config.admin {
                        return Err("forbidden".to_owned());
                    }

                    let mut tx_bytes: Vec<u8> = Vec::new();
                    if let Some(tx) = utxo_tx.transaction.as_ref() {
                        self.check_utxo_transaction(&tx)?;
                        let ret = tx.encode(&mut tx_bytes);
                        if ret.is_err() {
                            return Err("encode utxo tx failed".to_owned());
                        }
                    } else {
                        return Err("utxo tx is none".to_owned());
                    }

                    let tx_hash = &utxo_tx.transaction_hash;
                    verify_tx_hash(&tx_hash, &tx_bytes).map_err(|e| e.to_string())?;

                    for (i, w) in witnesses.into_iter().enumerate() {
                        let signature = &w.signature;
                        let sender = &w.sender;

                        if &verify_tx_signature(&tx_hash, &signature).map_err(|e| e.to_string())?
                            != sender
                        {
                            return Err(format!("Invalid sender index: {}", i));
                        }
                    }
                    Ok(tx_hash.clone())
                }
            }
        } else {
            Err("Invalid raw tx".to_owned())
        }
    }
}
