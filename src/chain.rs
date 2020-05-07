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

use crate::util::{hash_data, broadcast_message, print_main_chain, unix_now};
use cita_ng_proto::blockchain::{BlockHeader, CompactBlock, CompactBlockBody};
use prost::Message;
use std::collections::HashMap;
use cita_ng_proto::network::NetworkMsg;
use log::{info, warn};
use rand::Rng;

pub struct Chain {
    kms_port: String,
    network_port: String,
    storage_port: String,
    block_number: u64,
    block_delay_number: u32,
    fork_tree: Vec<HashMap<Vec<u8>, CompactBlock>>,
    main_chain: Vec<Vec<u8>>,
    candidate_block: Option<(Vec<u8>, CompactBlock)>,
}

impl Chain {
    pub fn new(storage_port: String, network_port: String, kms_port: String, block_delay_number: u32) -> Self {
        let mut fork_tree = Vec::with_capacity(block_delay_number as usize * 2);
        for _ in 0..(block_delay_number as usize * 2) {
            fork_tree.push(HashMap::new());
        }
        Chain {
            kms_port,
            network_port,
            storage_port,
            block_number: 0,
            block_delay_number,
            fork_tree,
            main_chain: Vec::new(),
            candidate_block: None,
        }
    }

    pub fn get_candidate_block_hash(&self) -> Option<Vec<u8>> {
        self.candidate_block
            .as_ref()
            .map(|(hash, _)| hash.to_owned())
    }

    pub async fn add_block(&mut self, block: CompactBlock) {
        // todo check txs dup with blocks before
        let header = block.clone().header.unwrap();
        let block_height = header.height;
        if block_height <= self.block_number {
            warn!("block_height {} too low", block_height);
            return;
        }

        if block_height - self.block_number > self.block_delay_number as u64 * 2 {
            warn!("block_height {} too high", block_height);
            return;
        }

        let mut block_header_bytes = Vec::new();
        header.encode(&mut block_header_bytes);

        let ret = hash_data(self.kms_port.clone(), 1, block_header_bytes).await;
        if let Ok(block_hash) = ret {
            info!("add block 0x{:2x}{:2x}{:2x}..{:2x}{:2x}", block_hash[0],block_hash[1],block_hash[2],block_hash[block_hash.len() - 2], block_hash[block_hash.len() - 1]);
            self.fork_tree[block_height as usize - self.block_number as usize - 1].insert(block_hash, block);
        } else {
            warn!("hash block failed {:?}", ret);
        }
    }

    pub async fn add_proposal(&mut self, tx_hash_list: Vec<Vec<u8>>) {
        // todo filter dup txs with blocks before
        let mut data = Vec::new();
        for hash in tx_hash_list.iter() {
            data.extend_from_slice(hash);
        }
        let transactions_root;
        {
            let ret = hash_data(self.kms_port.clone(), 1, data).await;
            if ret.is_err() {
                return;
            } else {
                transactions_root = ret.unwrap();
            }
        }

        let prevhash = if self.main_chain.is_empty() {
            // genesis block hash
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        } else {
            self.main_chain
                .get(self.main_chain.len() - 1)
                .unwrap()
                .to_owned()
        };
        let height = self.block_number + self.main_chain.len() as u64 + 1;
        info!("proposal {} prevhash 0x{:2x}{:2x}{:2x}..{:2x}{:2x}", height, prevhash[0],prevhash[1],prevhash[2],prevhash[prevhash.len() - 2], prevhash[prevhash.len() - 1]);
        let header = BlockHeader {
            prevhash,
            timestamp: unix_now(),
            height,
            transactions_root,
            proposer: self.kms_port.as_bytes().to_vec(),
            proof: vec![],
            executed_block_hash: vec![],
        };
        let body = CompactBlockBody {
            tx_hashes: tx_hash_list,
        };
        let block = CompactBlock {
            version: 0,
            header: Some(header.clone()),
            body: Some(body),
        };

        let mut block_header_bytes = Vec::new();
        header.encode(&mut block_header_bytes);

        if let Ok(block_hash) = hash_data(self.kms_port.clone(), 1, block_header_bytes).await {
            info!("add proposal 0x{:2x}{:2x}{:2x}..{:2x}{:2x}", block_hash[0],block_hash[1],block_hash[2],block_hash[block_hash.len() - 2], block_hash[block_hash.len() - 1]);
            self.candidate_block = Some((block_hash.clone(), block.clone()));
            self.fork_tree[self.main_chain.len()].insert(block_hash, block.clone());
        }

        {
            let mut block_bytes = Vec::new();
            block.encode(&mut block_bytes);
            let msg = NetworkMsg {
                module: "controller".to_owned(),
                r#type: "block".to_owned(),
                origin: 0,
                msg: block_bytes,
            };
            let _ = broadcast_message(self.network_port.clone(), msg).await;
        }
    }

    pub async fn check_proposal(&self, proposal: &[u8]) -> bool {
        for map in self.fork_tree.iter() {
            if map.contains_key(proposal) {
                return true;
            }
        }
        false
    }

    pub fn commit_block(&mut self, proposal: &[u8]) {
        let commit_block_index;
        let commit_block;
        for (index, map) in self.fork_tree.iter().enumerate() {
            // make sure the block in fork_tree
            if let Some(block) = map.get(proposal) {
                commit_block_index = index;
                commit_block = block.clone();
                // try to backwards found a candidate_chain
                let mut candidate_chain = Vec::new();
                candidate_chain.push(proposal.to_owned());
                let mut prevhash = commit_block.header.unwrap().prevhash;
                for i in 0..commit_block_index {
                    let map = self.fork_tree.get(commit_block_index - i - 1).unwrap();
                    if let Some(block) = map.get(&prevhash) {
                        candidate_chain.push(prevhash.clone());
                        prevhash = block.to_owned().header.unwrap().prevhash;
                    } else {
                        // if failed, return
                        return;
                    }
                }
                // if candidate_chain longer than original main_chain
                let coin : u64 = rand::thread_rng().gen();
                let coin_flag = coin % 100 > 50;
                if candidate_chain.len() > self.main_chain.len() || (candidate_chain.len() == self.main_chain.len() && coin_flag)  {
                    // replace the main_chain
                    candidate_chain.reverse();
                    self.main_chain = candidate_chain;
                    print_main_chain(&self.main_chain, self.block_number);
                    // candidate_block need update
                    self.candidate_block = None;
                    // check if any block has been finalized
                    if self.main_chain.len() > self.block_delay_number as usize {
                        let finalized_blocks_number = self.main_chain.len() - self.block_delay_number as usize;
                        info!("{} blocks finalized", finalized_blocks_number);
                        let new_main_chain = self.main_chain.split_off(finalized_blocks_number);
                        self.main_chain = new_main_chain;
                        self.fork_tree = self.fork_tree.split_off(finalized_blocks_number);
                        self.fork_tree.resize(self.block_delay_number as usize * 2, HashMap::new());
                        self.block_number += finalized_blocks_number as u64;
                    }
                }
                break;
            }
        }
    }
}
