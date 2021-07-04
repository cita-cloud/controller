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

use crate::auth::BLOCKLIMIT;
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::RawTransaction;
use std::collections::{BTreeMap, HashMap};

pub struct Pool {
    package_limit: usize,
    order_set: BTreeMap<u64, Vec<u8>>,
    order: u64,
    txs: HashMap<Vec<u8>, RawTransaction>,
}

impl Pool {
    pub fn new(package_limit: usize) -> Self {
        Pool {
            package_limit,
            order_set: BTreeMap::new(),
            order: 0,
            txs: HashMap::new(),
        }
    }

    fn get_order(&mut self) -> u64 {
        let order = self.order;
        let (new_order, _) = order.overflowing_add(1);
        self.order = new_order;
        order
    }

    pub fn enqueue(&mut self, tx_hash: Vec<u8>, raw_tx: RawTransaction) -> bool {
        if self.txs.contains_key(&tx_hash) {
            false
        } else {
            let order = self.get_order();
            self.order_set.insert(order, tx_hash.clone());
            self.txs.insert(tx_hash, raw_tx);
            true
        }
    }

    pub fn update(&mut self, tx_hash_list: &Vec<Vec<u8>>) {
        self.update_txs(tx_hash_list);
        self.update_order_set(tx_hash_list);
    }

    fn update_order_set(&mut self, tx_hash_list: &Vec<Vec<u8>>) {
        self.order_set = self
            .order_set
            .clone()
            .into_iter()
            .filter(|(_, hash)| !tx_hash_list.contains(hash))
            .collect();
    }

    fn update_txs(&mut self, tx_hash_list: &Vec<Vec<u8>>) {
        self.txs = self
            .txs
            .clone()
            .into_iter()
            .filter(|(hash, _)| !tx_hash_list.contains(hash))
            .collect();
    }

    pub fn package(&mut self, height: u64) -> (Vec<Vec<u8>>, Vec<RawTransaction>) {
        let mut invalid_tx_list = Vec::new();
        let mut tx_list = Vec::new();
        let mut tx_hash_list = Vec::new();

        for (_, hash) in self.order_set.iter() {
            match self.txs.get(hash) {
                Some(raw_tx) => {
                    if tx_is_valid(raw_tx, height) {
                        tx_list.push(raw_tx.clone());
                        tx_hash_list.push(hash.clone());
                    } else {
                        invalid_tx_list.push(hash.clone());
                    }
                    if tx_hash_list.len() >= self.package_limit {
                        break;
                    }
                }
                None => invalid_tx_list.push(hash.clone()),
            }
        }

        self.update(&invalid_tx_list);
        (tx_hash_list, tx_list)
    }

    pub fn len(&self) -> usize {
        self.order_set.len()
    }

    #[allow(dead_code)]
    pub fn is_contain(&self, tx_hash: &[u8]) -> bool {
        self.txs.get(tx_hash).is_some()
    }

    pub fn pool_get_tx(&self, tx_hash: &[u8]) -> Option<RawTransaction> {
        self.txs.get(tx_hash).cloned()
    }
}

fn tx_is_valid(raw_tx: &RawTransaction, height: u64) -> bool {
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

    height < valid_until_block && valid_until_block <= (height + BLOCKLIMIT)
}
