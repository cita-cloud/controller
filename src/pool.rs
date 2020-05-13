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

use cita_ng_proto::controller::RawTransaction;
use std::collections::HashMap;
use log::info;
use rand::Rng;
use std::cmp::Ord;

pub struct Pool {
    package_limit: usize,
    order_set: HashMap<u64, Vec<u8>>,
    order: u64,
    lower_bound: u64,
    txs: HashMap<Vec<u8>, RawTransaction>,
}

impl Pool {
    pub fn new(package_limit: usize) -> Self {
        Pool {
            package_limit,
            order_set: HashMap::new(),
            order: 0,
            lower_bound: 0,
            txs: HashMap::new(),
        }
    }

    fn get_order(&mut self) -> u64 {
        let order = self.order;
        let (new_order, _) = order.overflowing_add(1);
        self.order = new_order;
        order
    }

    pub fn enqueue(&mut self, raw_tx: RawTransaction, tx_hash: Vec<u8>) -> bool {
        let is_ok = !self.txs.contains_key(&tx_hash);
        if is_ok {
            let order = self.get_order();
            self.order_set.insert(order, tx_hash.clone());
            self.txs.insert(tx_hash, raw_tx);
        }
        is_ok
    }

    fn update_low_bound(&mut self) {
        info!("low_bound before update: {}", self.lower_bound);
        let old_low_bound = self.lower_bound;
        for i in old_low_bound..self.order {
            if self.order_set.get(&i).is_some() {
                break;
            }
            self.lower_bound += 1;
        }
        info!("low_bound after update: {}", self.lower_bound);
    }

    pub fn update(&mut self, tx_hash_list: Vec<Vec<u8>>) {
        info!("before update len of pool {}, will update {} tx", self.len(), tx_hash_list.len());
        for hash in tx_hash_list.iter() {
            self.txs.remove(hash);
        }

        let mut new_order_set = HashMap::new();
        for (order, hash) in self.order_set.iter() {
            if !tx_hash_list.contains(hash) {
                new_order_set.insert(*order, hash.to_owned());
            }
        }
        self.order_set = new_order_set;

        info!("after update len of pool {}", self.len());
        self.update_low_bound()
    }

    pub fn package(&mut self) -> Vec<Vec<u8>> {
        let mut tx_hash_list = Vec::new();

        let max_begin = self.order.saturating_sub(self.package_limit as u64);
        let begin_order = if max_begin <= self.lower_bound || self.len() < self.package_limit {
            self.lower_bound
        } else {
            rand::thread_rng().gen_range(self.lower_bound, max_begin)
        };

        for i in begin_order..self.order {
            if let Some(tx_hash) = self.order_set.get(&i) {
                tx_hash_list.push(tx_hash.to_owned());
            }
            if tx_hash_list.len() == self.package_limit {
                break;
            }
        }

        tx_hash_list
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, tx_hash: &[u8]) -> bool {
        self.txs.get(tx_hash).is_some()
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Option<RawTransaction> {
        self.txs.get(tx_hash).map(|tx| tx.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cita_ng_proto::blockchain::{Transaction, UnverifiedTransaction, Witness};
    use cita_ng_proto::controller::{raw_transaction::Tx, RawTransaction};

    pub fn generate_tx(
        transaction_hash: Vec<u8>,
        valid_until_block: u64,
        version: u32,
    ) -> RawTransaction {
        let tx = Transaction {
            version,
            to: Vec::new(),
            nonce: "".to_owned(),
            quota: 21000,
            valid_until_block,
            data: Vec::new(),
            value: Vec::new(),
            chain_id: Vec::new(),
        };
        let witness = Witness {
            signature: Vec::new(),
            sender: Vec::new(),
        };
        let unverified_tx = UnverifiedTransaction {
            transaction: Some(tx),
            transaction_hash,
            witness: Some(witness),
        };

        RawTransaction {
            tx: Some(Tx::NormalTx(unverified_tx)),
        }
    }

    #[test]
    fn basic() {
        let mut p = Pool::new(1);

        let tx1_hash = vec![1];
        let tx1 = generate_tx(tx1_hash.clone(), 99, 0);
        let tx2_hash = vec![1];
        let tx2 = generate_tx(tx2_hash.clone(), 99, 0);
        let tx3_hash = vec![2];
        let tx3 = generate_tx(tx3_hash.clone(), 99, 0);
        let tx4_hash = vec![3];
        let tx4 = generate_tx(tx4_hash.clone(), 5, 0);

        assert_eq!(p.enqueue(tx1.clone(), tx1_hash.clone()), true);
        assert_eq!(p.enqueue(tx2.clone(), tx2_hash.clone()), false);
        assert_eq!(p.enqueue(tx3.clone(), tx3_hash.clone()), true);
        assert_eq!(p.enqueue(tx4.clone(), tx4_hash.clone()), true);

        assert_eq!(p.len(), 3);
        assert!(p.contains(&tx1_hash));
        assert!(p.contains(&tx3_hash));
        assert!(p.contains(&tx4_hash));

        p.update(vec![tx1_hash]);
        assert_eq!(p.len(), 2);
        assert_eq!(p.package(), vec![tx3_hash.clone()]);
        p.update(vec![tx3_hash]);
        assert_eq!(p.package(), vec![tx4_hash]);
        assert_eq!(p.len(), 1);
    }
}
