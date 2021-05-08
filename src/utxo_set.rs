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

use cita_cloud_proto::blockchain::UnverifiedUtxoTransaction;
use log::warn;
use serde_derive::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct SystemConfigFile {
    pub version: u32,
    pub chain_id: String,
    // address of admin
    pub admin: String,
    pub block_interval: u32,
    pub validators: Vec<String>,
}

impl SystemConfigFile {
    pub fn new(init_sys_config_str: &str) -> Self {
        toml::from_str::<SystemConfigFile>(init_sys_config_str)
            .expect("Error while parsing init_sys_config_str")
    }

    pub fn to_system_config(&self) -> SystemConfig {
        let chain_id = hex::decode(&self.chain_id[2..]).expect("parsing chain_id failed!");
        let admin = hex::decode(&self.admin[2..]).expect("parsing admin failed!");
        let mut validators = Vec::new();
        for validator_str in self.validators.iter() {
            let validator = hex::decode(&validator_str[2..]).expect("parsing validator failed!");
            validators.push(validator)
        }
        SystemConfig::new(
            self.version,
            chain_id,
            admin,
            self.block_interval,
            validators,
        )
    }
}

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
    pub emergency_brake: bool,
    pub utxo_tx_hashes: HashMap<u64, Vec<u8>>,
}

pub const LOCK_ID_VERSION: u64 = 1_000;
pub const LOCK_ID_CHAIN_ID: u64 = 1_001;
pub const LOCK_ID_ADMIN: u64 = 1_002;
pub const LOCK_ID_BLOCK_INTERVAL: u64 = 1_003;
pub const LOCK_ID_VALIDATORS: u64 = 1_004;
pub const LOCK_ID_EMERGENCY_BRAKE: u64 = 1_005;
pub const LOCK_ID_BUTTON: u64 = 1_006;

impl SystemConfig {
    pub fn new(
        version: u32,
        chain_id: Vec<u8>,
        admin: Vec<u8>,
        block_interval: u32,
        validators: Vec<Vec<u8>>,
    ) -> Self {
        let mut map = HashMap::new();
        for id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
            map.insert(id, vec![0u8; 33]);
        }

        SystemConfig {
            version,
            chain_id,
            admin,
            block_interval,
            validators,
            emergency_brake: false,
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
                if hash != &pre_tx_hash {
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
                if data.len() == 20 {
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
                if data.len() % 20 == 0 {
                    for i in 0..(data.len() / 20) {
                        validators.push(data[i * 20..(i + 1) * 20].to_vec())
                    }
                    self.validators = validators;
                    true
                } else {
                    warn!("Invalid validators");
                    false
                }
            }
            LOCK_ID_EMERGENCY_BRAKE => {
                self.emergency_brake = !data.is_empty();
                true
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

#[cfg(test)]
mod tests {
    use super::SystemConfigFile;

    #[test]
    fn basic_test() {
        let toml_str = r#"
        version = 0
        chain_id = "0x010203040506"
        admin = "0x060504030201"
        block_interval = 6
        validators = ["0x01010101", "0x02020202"]
        "#;

        let config = SystemConfigFile::new(toml_str);
        let sys_config = config.to_system_config();

        assert_eq!(sys_config.version, 0);
        assert_eq!(sys_config.chain_id, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(sys_config.admin, vec![6, 5, 4, 3, 2, 1]);
        assert_eq!(sys_config.block_interval, 6);
        assert_eq!(
            sys_config.validators,
            vec![vec![1, 1, 1, 1], vec![2, 2, 2, 2]]
        );
    }
}
