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

use crate::util::kms_client;
use cita_cloud_proto::blockchain::{Block, BlockHeader, RawTransactions};
use cloud_util::{clean_0x, common::read_toml, crypto::hash_data};
use prost::Message;
use serde_derive::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct GenesisBlock {
    pub timestamp: u64,
    pub prevhash: String,
}

impl GenesisBlock {
    pub fn new(config_path: &str) -> Self {
        read_toml(config_path, "genesis_block")
    }

    pub fn genesis_block(&self) -> Block {
        let prev_hash =
            hex::decode(clean_0x(&self.prevhash)).expect("parsing prevhash in genesis failed!");
        let header = BlockHeader {
            prevhash: prev_hash,
            timestamp: self.timestamp,
            height: 0,
            transactions_root: vec![0u8; 32],
            proposer: vec![0u8; 32],
        };
        Block {
            version: 0,
            header: Some(header),
            body: Some(RawTransactions { body: Vec::new() }),
            proof: Vec::new(),
        }
    }

    pub async fn genesis_block_hash(&self) -> Vec<u8> {
        let block = self.genesis_block();
        let header = block.header.unwrap();

        let mut block_header_bytes = Vec::new();
        header
            .encode(&mut block_header_bytes)
            .expect("encode block header failed");

        hash_data(kms_client(), &block_header_bytes).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::GenesisBlock;

    #[test]
    fn basic_test() {
        let genesis = GenesisBlock::new("example/config.toml");
        let block = genesis.genesis_block();
        let header = block.header.unwrap();
        assert_eq!(header.timestamp, 123_456);
        assert_eq!(header.prevhash, vec![1, 2, 3, 4, 5, 6]);
    }
}
