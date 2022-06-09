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

use crate::util::tx_quota;
use cita_cloud_proto::blockchain::{raw_transaction::Tx, RawTransaction};
use std::{
    borrow::Borrow,
    cmp::{Eq, PartialEq},
    collections::HashSet,
    hash::{Hash, Hasher},
};

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
    block_limit: u64,
    quota_limit: u64,
}

impl Pool {
    pub fn new(block_limit: u64, quota_limit: u64) -> Self {
        Pool {
            txns: HashSet::new(),
            block_limit,
            quota_limit,
        }
    }

    pub fn enqueue(&mut self, raw_tx: RawTransaction) -> bool {
        self.txns.insert(Txn(raw_tx))
    }

    pub fn update(&mut self, tx_hash_list: &[Vec<u8>]) {
        for tx_hash in tx_hash_list {
            self.txns.remove(tx_hash.as_slice());
        }
    }

    pub fn package(&mut self, height: u64) -> Vec<RawTransaction> {
        self.txns
            .retain(|txn| tx_is_valid(&txn.0, height, self.block_limit));
        let mut quota_limit = self.quota_limit as i64;
        let result = self
            .txns
            .iter()
            .cloned()
            .filter(|item| {
                let quota = tx_quota(&item.0);
                let flag = quota_limit >= quota as i64;
                quota_limit -= quota as i64;
                flag
            })
            .map(|item| item.0)
            .collect();
        let mut quota_limit = self.quota_limit as i64;
        self.txns.retain(|item| {
            let quota = tx_quota(&item.0);
            let flag = quota_limit >= quota as i64;
            quota_limit -= quota as i64;
            !flag
        });
        result
    }

    pub fn len(&self) -> usize {
        self.txns.len()
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
    let valid_until_block = {
        match raw_tx.tx {
            Some(Tx::NormalTx(ref normal_tx)) => match normal_tx.transaction {
                Some(ref tx) => tx.valid_until_block,
                None => return false,
            },
            Some(Tx::UtxoTx(_)) => {
                return true;
            }
            None => return false,
        }
    };

    height < valid_until_block && valid_until_block <= (height + block_limit)
}
