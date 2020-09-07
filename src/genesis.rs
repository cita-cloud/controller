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
use cita_cloud_proto::blockchain::{BlockHeader, CompactBlock, CompactBlockBody};
use log::warn;
use prost::Message;
use serde_derive::Deserialize;
use std::time::Duration;
use tokio::time;

#[derive(Debug, Clone, Deserialize)]
pub struct GenesisBlock {
    pub timestamp: u64,
    pub prevhash: String,
}

impl GenesisBlock {
    pub fn new(genesis_block_str: &str) -> Self {
        toml::from_str::<GenesisBlock>(genesis_block_str)
            .expect("Error while parsing genesis_block_str")
    }
    pub fn genesis_block(&self) -> CompactBlock {
        let prevhash =
            hex::decode(&self.prevhash[2..]).expect("parsing prevhash in genesis failed!");
        let header = BlockHeader {
            prevhash,
            timestamp: self.timestamp,
            height: 0,
            transactions_root: vec![0u8; 32],
            proposer: vec![0u8; 32],
        };
        let body = CompactBlockBody { tx_hashes: vec![] };
        CompactBlock {
            version: 0,
            header: Some(header),
            body: Some(body),
        }
    }

    pub async fn genesis_block_hash(&self, kms_port: u16) -> Vec<u8> {
        let block = self.genesis_block();
        let header = block.header.unwrap();

        let mut block_header_bytes = Vec::new();
        header
            .encode(&mut block_header_bytes)
            .expect("encode block header failed");

        let genesis_block_hash;
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let ret = hash_data(kms_port, 1, block_header_bytes.clone()).await;

            if let Ok(block_hash) = ret {
                genesis_block_hash = block_hash;
                break;
            }
            warn!("hash block failed! Retrying");
        }
        genesis_block_hash
    }
}

#[cfg(test)]
mod tests {
    use super::GenesisBlock;

    #[test]
    fn basic_test() {
        let toml_str = r#"
        timestamp = 123456
        prevhash = "0x010203040506"
        "#;

        let genesis = GenesisBlock::new(toml_str);
        let block = genesis.genesis_block();
        let header = block.header.unwrap();
        assert_eq!(header.timestamp, 123_456);
        assert_eq!(header.prevhash, vec![1, 2, 3, 4, 5, 6]);
    }
}
