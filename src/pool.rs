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

use log::info;
use rand::Rng;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct Pool {
    package_limit: usize,
    order_set: HashMap<u64, Vec<u8>>,
    order: u64,
    lower_bound: u64,
    txs: HashSet<Vec<u8>>,
}

impl Pool {
    pub fn new(package_limit: usize) -> Self {
        Pool {
            package_limit,
            order_set: HashMap::new(),
            order: 0,
            lower_bound: 0,
            txs: HashSet::new(),
        }
    }

    fn get_order(&mut self) -> u64 {
        let order = self.order;
        let (new_order, _) = order.overflowing_add(1);
        self.order = new_order;
        order
    }

    pub fn enqueue(&mut self, tx_hash: Vec<u8>) -> bool {
        if self.txs.contains(&tx_hash) {
            false
        } else {
            let order = self.get_order();
            self.order_set.insert(order, tx_hash.clone());
            self.txs.insert(tx_hash);
            true
        }
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
        info!(
            "before update len of pool {}, will update {} tx",
            self.len(),
            tx_hash_list.len()
        );

        let mut new_order_set = HashMap::new();
        let mut new_txs = HashSet::new();
        for (order, hash) in self.order_set.iter() {
            if !tx_hash_list.contains(hash) {
                new_order_set.insert(*order, hash.to_owned());
                new_txs.insert(hash.to_owned());
            }
        }
        self.order_set = new_order_set;
        self.txs = new_txs;

        info!("after update len of pool {}", self.len());
        self.update_low_bound()
    }

    pub fn package(&self) -> Vec<Vec<u8>> {
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
        self.order_set.len()
    }
}
