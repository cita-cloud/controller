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

use cita_ng_proto::blockchain::UnverifiedUtxoTransaction;
use log::warn;
use std::collections::HashMap;

// store related utxo tx hash into global region
// begin from id 1000
// lockid in utxo tx is also from 1000
#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub version: u32,
    pub chain_id: Vec<u8>,
    // address of admin
    pub admin: Vec<u8>,
    pub block_interval: u32,
    pub validators: Vec<Vec<u8>>,
    pub utxo_tx_hashes: HashMap<u64, Vec<u8>>,
}

pub const LOCK_ID_VERSION: u64 = 1_000;
pub const LOCK_ID_CHAIN_ID: u64 = 1_001;
pub const LOCK_ID_ADMIN: u64 = 1_002;
pub const LOCK_ID_BLOCK_INTERVAL: u64 = 1_003;
pub const LOCK_ID_VALIDATORS: u64 = 1_004;
pub const LOCK_ID_BUTTON: u64 = 1_005;

impl SystemConfig {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        for id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
            map.insert(id, vec![0u8; 33]);
        }

        SystemConfig {
            version: 0,
            chain_id: vec![0u8; 32],
            admin: vec![0u8; 21],
            block_interval: 6,
            validators: vec![vec![0; 21], vec![1; 21]],
            utxo_tx_hashes: map,
        }
    }

    pub fn update(&mut self, tx: &UnverifiedUtxoTransaction, is_init: bool) -> bool {
        let tx_hash = tx.transaction_hash.clone();
        let utxo_tx = tx.clone().transaction.unwrap();
        let lock_id = utxo_tx.lock_id;
        let pre_tx_hash = utxo_tx.pre_tx_hash;
        let data = utxo_tx.output;

        if !is_init {
            if let Some(hash) = self.utxo_tx_hashes.get(&lock_id) {
                if hash.to_owned() != pre_tx_hash {
                    return false;
                }
            } else {
                return false;
            }
        }

        let ret = match lock_id {
            LOCK_ID_VERSION => {
                self.version = u32_decode(data);
                true
            }
            LOCK_ID_CHAIN_ID => {
                if data.len() == 32 {
                    self.chain_id = data;
                    true
                } else {
                    warn!("Invalid chain id");
                    false
                }
            }
            LOCK_ID_ADMIN => {
                if data.len() == 21 {
                    self.admin = data;
                    true
                } else {
                    warn!("Invalid admin");
                    false
                }
            }
            LOCK_ID_BLOCK_INTERVAL => {
                self.block_interval = u32_decode(data);
                true
            }
            LOCK_ID_VALIDATORS => {
                let mut validators = Vec::new();
                if data.len() % 21 == 0 {
                    for i in 0..(data.len() / 21) {
                        validators.push(data[i..(i + 1) * 21].to_vec())
                    }
                    true
                } else {
                    warn!("Invalid validators");
                    false
                }
            }
            _ => {
                warn!("Invalid lock_id");
                false
            }
        };

        if ret {
            self.utxo_tx_hashes.insert(lock_id, tx_hash);
        }

        ret
    }
}

fn u32_decode(data: Vec<u8>) -> u32 {
    let mut bytes: [u8; 4] = [0; 4];
    bytes[..4].clone_from_slice(&data[..4]);
    u32::from_be_bytes(bytes)
}
