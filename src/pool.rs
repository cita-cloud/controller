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
    borrow::Borrow,
    cmp::{Eq, PartialEq},
    collections::HashSet,
    hash::{Hash, Hasher},
};

use cita_cloud_proto::blockchain::{raw_transaction::Tx, RawTransaction};

use crate::util::get_tx_quota;

// wrapper type for Hash
#[derive(Clone)]
struct Txn(RawTransaction);

impl Borrow<[u8]> for Txn {
    fn borrow(&self) -> &[u8] {
        get_raw_tx_hash(&self.0)
    }
}

impl PartialEq for Txn {
    fn eq(&self, other: &Self) -> bool {
        get_raw_tx_hash(&self.0) == get_raw_tx_hash(&other.0)
    }
}

impl Eq for Txn {}

impl Hash for Txn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(get_raw_tx_hash(&self.0), state);
    }
}

fn get_raw_tx_hash(raw_tx: &RawTransaction) -> &[u8] {
    match raw_tx.tx.as_ref() {
        Some(Tx::NormalTx(tx)) => &tx.transaction_hash,
        Some(Tx::UtxoTx(utxo)) => &utxo.transaction_hash,
        None => &[],
    }
}

pub struct Pool {
    txns: HashSet<Txn>,
    pool_quota: u64,
    block_limit: u64,
    quota_limit: u64,
}

impl Pool {
    pub fn new(block_limit: u64, quota_limit: u64) -> Self {
        Pool {
            txns: HashSet::new(),
            pool_quota: 1000,
            block_limit,
            quota_limit,
        }
    }

    pub fn insert(&mut self, raw_tx: RawTransaction) -> bool {
        let tx_quota = match get_tx_quota(&raw_tx) {
            Ok(tx_quota) => tx_quota,
            Err(_) => panic!("Error getting tx_quota"),
        };
        let mut ret = false;
        for existing_tx in &self.txns {
            if *existing_tx == Txn(raw_tx.clone()) {
                ret = true;
                break;
            }
        }
        if ret == false {
            self.txns.insert(Txn(raw_tx.clone()));
            ret = true;
        } else {
            ret = false;
        }
        if ret {
            self.pool_quota += tx_quota;
        }
        ret
    }

    pub fn remove(&mut self, tx_hash_list: &[Vec<u8>]) {
        for tx_hash in tx_hash_list {
            let tx_quota = self
                .txns
                .get(tx_hash.as_slice())
                .map(|txn| get_tx_quota(&txn.0).unwrap());
            if let Some(tx_quota) = tx_quota {
                self.pool_quota -= tx_quota;
            }
            self.txns.remove(tx_hash.as_slice());
        }
    }

    pub fn package(&mut self, height: u64) -> (Vec<Vec<u8>>, u64) {
        let block_limit = self.block_limit;
        self.txns
            .retain(|txn| tx_is_valid(&txn.0, height, block_limit));
        let mut quota_limit = self.quota_limit;
        let mut pack_tx = vec![];
        for txn in self.txns.iter().cloned() {
            let tx_quota = get_tx_quota(&txn.0).unwrap();
            if quota_limit >= tx_quota {
                pack_tx.push(get_raw_tx_hash(&txn.0).to_vec());
                quota_limit -= tx_quota;
                // 21000 is a basic tx quota, but the utxo's quota spend is 0
                if quota_limit < 21000 {
                    break;
                }
            }
        }
        (pack_tx, self.quota_limit - quota_limit)
    }

    pub fn pool_status(&self) -> (usize, u64) {
        (self.txns.len(), self.pool_quota)
    }

    pub fn pool_get_tx(&self, tx_hash: &[u8]) -> Option<RawTransaction> {
        self.txns.get(tx_hash).cloned().map(|txn| txn.0)
    }

    pub fn set_block_limit(&mut self, block_limit: u64) {
        self.block_limit = block_limit;
    }

    pub fn set_quota_limit(&mut self, quota_limit: u64) {
        self.quota_limit = quota_limit;
    }
}

fn tx_is_valid(raw_tx: &RawTransaction, height: u64, block_limit: u64) -> bool {
    match raw_tx.tx {
        Some(Tx::NormalTx(ref normal_tx)) => match normal_tx.transaction {
            Some(ref tx) => {
                height < tx.valid_until_block && tx.valid_until_block <= height + block_limit
            }
            None => false,
        },
        Some(Tx::UtxoTx(_)) => true,
        None => false,
    }
}
