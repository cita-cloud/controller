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

use crate::util::hash_data;
use cita_ng_proto::blockchain::{BlockHeader, CompactBlock, CompactBlockBody};
use prost::Message;
use std::collections::HashMap;

pub struct Chain {
    kms_port: String,
    block_number: u64,
    block_delay_number: u32,
    fork_tree: Vec<HashMap<Vec<u8>, CompactBlock>>,
    main_chain: Vec<Vec<u8>>,
    candidate_block: Option<(Vec<u8>, CompactBlock)>,
}

impl Chain {
    pub fn new(kms_port: String, block_delay_number: u32) -> Self {
        Chain {
            kms_port,
            block_number: 0,
            block_delay_number,
            fork_tree: Vec::new(),
            main_chain: Vec::new(),
            candidate_block: None,
        }
    }

    pub fn get_candidate_block_hash(&self) -> Option<Vec<u8>> {
        self.candidate_block
            .as_ref()
            .map(|(hash, _)| hash.to_owned())
    }

    pub async fn add_proposal(&mut self, tx_hash_list: Vec<Vec<u8>>) {
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
        let header = BlockHeader {
            prevhash,
            timestamp: 123456,
            height: self.block_number + self.main_chain.len() as u64 + 1,
            transactions_root,
            proposer: vec![1, 2, 3, 4],
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
            self.candidate_block = Some((block_hash, block));
        }
    }
}
