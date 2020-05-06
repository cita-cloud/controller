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
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug)]
struct TxOrder {
    hash: Vec<u8>,
    order: u64,
}

impl TxOrder {
    fn new(hash: Vec<u8>, order: u64) -> Self {
        TxOrder { hash, order }
    }
}

impl Eq for TxOrder {}

impl PartialEq for TxOrder {
    fn eq(&self, other: &TxOrder) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl PartialOrd for TxOrder {
    fn partial_cmp(&self, other: &TxOrder) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TxOrder {
    fn cmp(&self, b: &TxOrder) -> Ordering {
        self.order.cmp(&b.order)
    }
}

pub struct Pool {
    package_limit: usize,
    order_set: BTreeSet<TxOrder>,
    order: u64,
    txs: HashMap<Vec<u8>, RawTransaction>,
}

impl Pool {
    pub fn new(package_limit: usize) -> Self {
        Pool {
            package_limit,
            order_set: BTreeSet::new(),
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

    pub fn enqueue(&mut self, raw_tx: RawTransaction, tx_hash: Vec<u8>) -> bool {
        let is_ok = !self.txs.contains_key(&tx_hash);
        if is_ok {
            let order = self.get_order();
            let tx_order = TxOrder::new(tx_hash.clone(), order);
            self.order_set.insert(tx_order);
            self.txs.insert(tx_hash, raw_tx);
        }
        is_ok
    }

    pub fn update(&mut self, tx_hash_list: Vec<Vec<u8>>) {
        for hash in tx_hash_list.iter() {
            self.txs.remove(hash);
        }
        self.order_set = self
            .order_set
            .iter()
            .cloned()
            .filter(|order| !tx_hash_list.contains(&order.hash))
            .collect();
    }

    pub fn package(&mut self) -> Vec<Vec<u8>> {
        let mut tx_hash_list = Vec::new();

        let mut iter = self.order_set.iter();
        for _ in 0..self.package_limit {
            let order = iter.next();
            if let Some(order) = order {
                let tx_hash = &order.hash;
                if self.txs.get(tx_hash).is_some() {
                    tx_hash_list.push(tx_hash.to_owned());
                }
            } else {
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
