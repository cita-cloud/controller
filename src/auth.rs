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

use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
};

use cita_cloud_proto::{
    blockchain::{
        raw_transaction::Tx::{NormalTx, UtxoTx},
        RawTransaction, RawTransactions, Transaction, UnverifiedUtxoTransaction, UtxoTransaction,
    },
    status_code::StatusCodeEnum,
};

use crate::{
    grpc_client::storage::get_compact_block,
    system_config::{SystemConfig, LOCK_ID_BUTTON, LOCK_ID_VERSION},
};

#[derive(Clone)]
pub struct Authentication {
    history_hashes: HashMap<u64, HashSet<Vec<u8>>>,
    current_block_number: u64,
    sys_config: SystemConfig,
}

// basic method
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
        let begin_block_number = if init_block_number >= self.sys_config.block_limit {
            init_block_number - self.sys_config.block_limit + 1
        } else {
            1u64
        };

        for h in begin_block_number..=init_block_number {
            let block = get_compact_block(h).await.unwrap();
            let block_body = block.body.unwrap();
            self.history_hashes
                .insert(h, HashSet::from_iter(block_body.tx_hashes));
        }
        self.current_block_number = init_block_number;
    }

    pub fn insert_tx_hash(&mut self, h: u64, hash_list: Vec<Vec<u8>>) {
        self.history_hashes.insert(h, HashSet::from_iter(hash_list));
        if h >= self.sys_config.block_limit {
            self.history_hashes
                .remove(&(h - self.sys_config.block_limit));
        }
        if h > self.current_block_number {
            self.current_block_number = h;
        }
    }
}

// tx check method
impl Authentication {
    fn check_chain_availability(&self) -> Result<(), StatusCodeEnum> {
        if self.sys_config.emergency_brake {
            Err(StatusCodeEnum::EmergencyBrake)
        } else {
            Ok(())
        }
    }

    fn check_tx_duplication(&self, tx_hash: &[u8]) -> Result<(), StatusCodeEnum> {
        for (_h, hash_list) in self.history_hashes.iter() {
            if hash_list.contains(tx_hash) {
                return Err(StatusCodeEnum::HistoryDupTx);
            }
        }
        Ok(())
    }

    fn check_normal_tx_fields(&self, tx: &Transaction) -> Result<(), StatusCodeEnum> {
        if tx.version != self.sys_config.version {
            Err(StatusCodeEnum::InvalidVersion)
        } else if tx.to.len() != 20 && !tx.to.is_empty() {
            Err(StatusCodeEnum::InvalidTo)
        } else if tx.nonce.len() > 128 {
            Err(StatusCodeEnum::InvalidNonce)
        } else if tx.valid_until_block <= self.current_block_number
            || tx.valid_until_block > (self.current_block_number + self.sys_config.block_limit)
        {
            Err(StatusCodeEnum::InvalidValidUntilBlock)
        } else if tx.value.len() != 32 {
            Err(StatusCodeEnum::InvalidValue)
        } else if tx.chain_id.len() != 32 || tx.chain_id != self.sys_config.chain_id {
            Err(StatusCodeEnum::InvalidChainId)
        } else if tx.quota > self.sys_config.quota_limit {
            Err(StatusCodeEnum::QuotaUsedExceed)
        } else {
            Ok(())
        }
    }

    fn check_utxo_tx_fields(&self, utxo_tx: &UtxoTransaction) -> Result<(), StatusCodeEnum> {
        if utxo_tx.version != self.sys_config.version {
            return Err(StatusCodeEnum::InvalidVersion);
        }
        let lock_id = utxo_tx.lock_id;
        if !(LOCK_ID_VERSION..LOCK_ID_BUTTON).contains(&lock_id) {
            return Err(StatusCodeEnum::InvalidLockId);
        }
        let hash = self.sys_config.utxo_tx_hashes.get(&lock_id).unwrap();
        if hash != &utxo_tx.pre_tx_hash {
            return Err(StatusCodeEnum::InvalidPreHash);
        }
        Ok(())
    }

    pub fn auth_check(&self, raw_tx: &RawTransaction) -> Result<(), StatusCodeEnum> {
        match raw_tx.tx.as_ref() {
            Some(NormalTx(normal_tx)) => {
                self.check_chain_availability()?;

                let tx_hash = &normal_tx.transaction_hash;
                self.check_tx_duplication(tx_hash)?;

                if let Some(tx) = &normal_tx.transaction {
                    self.check_normal_tx_fields(tx)?;
                } else {
                    return Err(StatusCodeEnum::NoneTransaction);
                }

                Ok(())
            }
            Some(UtxoTx(utxo_tx)) => {
                // check utxo tx sender
                if utxo_tx.witnesses[0].sender != self.sys_config.admin {
                    return Err(StatusCodeEnum::AdminCheckError);
                }

                if let Some(tx) = utxo_tx.transaction.as_ref() {
                    self.check_utxo_tx_fields(tx)?;
                } else {
                    return Err(StatusCodeEnum::NoneUtxo);
                }

                Ok(())
            }
            None => Err(StatusCodeEnum::NoneRawTx),
        }
    }

    pub fn auth_check_batch(&self, raw_txs: &RawTransactions) -> Result<(), StatusCodeEnum> {
        for raw_tx in raw_txs.body.as_slice() {
            self.auth_check(raw_tx)?;
        }
        Ok(())
    }
}
