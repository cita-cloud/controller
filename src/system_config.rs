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

use crate::config::ControllerConfig;
use crate::util::storage_client;
use cita_cloud_proto::blockchain::{
    raw_transaction::Tx::UtxoTx, RawTransaction, UnverifiedUtxoTransaction,
};
use cita_cloud_proto::controller::SystemConfig as ProtoSystemConfig;
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::storage::Regions;
use cloud_util::{clean_0x, common::read_toml, storage::load_data};
use log::warn;
use prost::Message;
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
    pub block_limit: u64,
    pub quota_limit: u64,
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
    pub block_limit: u64,
    pub quota_limit: u64,
    pub utxo_tx_hashes: HashMap<u64, Vec<u8>>,
    validator_address_len: usize,
    is_danger: bool,
}

pub const LOCK_ID_VERSION: u64 = 1_000;
pub const LOCK_ID_CHAIN_ID: u64 = 1_001;
pub const LOCK_ID_ADMIN: u64 = 1_002;
pub const LOCK_ID_BLOCK_INTERVAL: u64 = 1_003;
pub const LOCK_ID_VALIDATORS: u64 = 1_004;
pub const LOCK_ID_EMERGENCY_BRAKE: u64 = 1_005;
pub const LOCK_ID_BLOCK_LIMIT: u64 = 1_006;
pub const LOCK_ID_QUOTA_LIMIT: u64 = 1_007;
pub const LOCK_ID_BUTTON: u64 = 1_008;

impl SystemConfig {
    pub fn new(config_path: &str) -> Self {
        //generate SystemConfigFile from config.toml
        let sys_config_file: SystemConfigFile = read_toml(config_path, "system_config");
        let config: ControllerConfig = ControllerConfig::new(config_path);

        //convert String to Vec<u8>
        let chain_id =
            hex::decode(clean_0x(&sys_config_file.chain_id)).expect("parsing chain_id failed!");
        let admin = hex::decode(clean_0x(&sys_config_file.admin)).expect("parsing admin failed!");
        let mut validators = Vec::new();
        for validator_str in sys_config_file.validators.iter() {
            let validator =
                hex::decode(clean_0x(validator_str)).expect("parsing validator failed!");
            validators.push(validator)
        }

        //init utxo_tx_hashes
        let mut map = HashMap::new();
        for id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
            map.insert(id, vec![0u8; 33]);
        }

        //return SystemConfig
        SystemConfig {
            version: sys_config_file.version,
            chain_id,
            admin,
            block_interval: sys_config_file.block_interval,
            validators,
            emergency_brake: false,
            utxo_tx_hashes: map,
            block_limit: sys_config_file.block_limit,
            quota_limit: sys_config_file.quota_limit,
            validator_address_len: config.validator_address.len() / 2,
            is_danger: config.is_danger,
        }
    }

    pub fn match_data(&mut self, lock_id: u64, data: Vec<u8>, is_init: bool) -> bool {
        match lock_id {
            LOCK_ID_CHAIN_ID => {
                if is_init {
                    if data.len() == 32 {
                        self.chain_id = data;
                        true
                    } else {
                        warn!("Invalid chain id");
                        false
                    }
                } else {
                    warn!("attempt to change chain_id when not init");
                    false
                }
            }
            LOCK_ID_VERSION => {
                self.version = u32_decode(data);
                true
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
                let l = self.validator_address_len;
                let mut validators = Vec::new();
                if data.len() % l == 0 || self.is_danger {
                    for i in 0..(data.len() / l) {
                        validators.push(data[i * l..(i + 1) * l].to_vec())
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
            LOCK_ID_BLOCK_LIMIT => {
                if is_init {
                    self.block_limit = u64_decode(data);
                    true
                } else {
                    warn!("attempt to change block_limit when not init");
                    false
                }
            }
            LOCK_ID_QUOTA_LIMIT => {
                self.quota_limit = u64_decode(data);
                true
            }
            _ => {
                warn!("Invalid lock_id:{}", lock_id);
                false
            }
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

        let ret = self.match_data(lock_id, data, is_init);

        if ret {
            self.utxo_tx_hashes.insert(lock_id, tx_hash);
        }

        ret
    }

    pub async fn modify_sys_config_by_utxotx_hash(&mut self, utxo_hash: Vec<u8>) -> StatusCodeEnum {
        match load_data(
            storage_client(),
            i32::from(Regions::Transactions) as u32,
            utxo_hash.clone(),
        )
        .await
        {
            Ok(raw_tx_bytes) => {
                let raw_tx = RawTransaction::decode(raw_tx_bytes.as_slice()).unwrap();
                let tx = raw_tx.tx.unwrap();
                if let UtxoTx(utxo_tx) = tx {
                    if self.update(&utxo_tx, true) {
                        StatusCodeEnum::Success
                    } else {
                        StatusCodeEnum::UpdateSystemConfigError
                    }
                } else {
                    warn!(
                        "tx from utxo_tx_hash{:?} is not utxo_tx",
                        hex::encode(&utxo_hash)
                    );
                    StatusCodeEnum::NoneUtxo
                }
            }
            Err(e) => {
                warn!("load utxo_tx failed: {}", e);
                StatusCodeEnum::NoTransaction
            }
        }
    }

    pub fn generate_proto_sys_config(&self) -> ProtoSystemConfig {
        ProtoSystemConfig {
            version: self.version,
            chain_id: self.chain_id.clone(),
            admin: self.admin.clone(),
            block_interval: self.block_interval,
            validators: self.validators.clone(),
            emergency_brake: self.emergency_brake,
            quota_limit: self.quota_limit as u32,
            block_limit: self.block_limit as u32,
            version_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_VERSION)
                .unwrap()
                .to_owned(),
            chain_id_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_CHAIN_ID)
                .unwrap()
                .to_owned(),
            admin_pre_hash: self.utxo_tx_hashes.get(&LOCK_ID_ADMIN).unwrap().to_owned(),
            block_interval_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_BLOCK_INTERVAL)
                .unwrap()
                .to_owned(),
            validators_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_VALIDATORS)
                .unwrap()
                .to_owned(),
            emergency_brake_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_EMERGENCY_BRAKE)
                .unwrap()
                .to_owned(),
            quota_limit_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_QUOTA_LIMIT)
                .unwrap()
                .to_owned(),
            block_limit_pre_hash: self
                .utxo_tx_hashes
                .get(&LOCK_ID_BLOCK_LIMIT)
                .unwrap()
                .to_owned(),
        }
    }
}

fn u32_decode(data: Vec<u8>) -> u32 {
    let mut bytes: [u8; 4] = [0; 4];
    bytes[..4].clone_from_slice(&data[..4]);
    u32::from_be_bytes(bytes)
}

fn u64_decode(data: Vec<u8>) -> u64 {
    let mut bytes: [u8; 8] = [0; 8];
    bytes[..8].clone_from_slice(&data[..8]);
    u64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::SystemConfig;

    #[test]
    fn basic_test() {
        let sys_config = SystemConfig::new("example/config.toml");

        assert_eq!(sys_config.version, 0);
        assert_eq!(sys_config.chain_id, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(sys_config.admin, vec![6, 5, 4, 3, 2, 1]);
        assert_eq!(sys_config.block_interval, 3);
        assert_eq!(
            sys_config.validators,
            vec![vec![1, 1, 1, 1], vec![2, 2, 2, 2]]
        );
        assert_eq!(sys_config.block_limit, 100);
    }
}
