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

use crate::node_manager::ChainStatus;
use cita_cloud_proto::blockchain::Block;
use cita_cloud_proto::common::Address;
use log::debug;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct SyncManager {
    syncing_block_list: Arc<RwLock<BTreeMap<u64, (Address, Block)>>>,

    sync_config: SyncConfig,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncBlockRequest {
    #[prost(uint64, tag = "1")]
    pub start_height: u64,
    #[prost(uint64, tag = "2")]
    pub end_height: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncBlocks {
    #[prost(message, optional, tag = "1")]
    pub address: ::core::option::Option<Address>,
    #[prost(message, repeated, tag = "2")]
    pub sync_blocks: ::prost::alloc::vec::Vec<Block>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncBlockRespond {
    #[prost(oneof = "sync_block_respond::Respond", tags = "1, 2")]
    pub respond: ::core::option::Option<sync_block_respond::Respond>,
}

/// Nested message and enum types in `SyncBlockRespond`.
pub mod sync_block_respond {
    use cita_cloud_proto::common::Address;

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Respond {
        #[prost(message, tag = "1")]
        NotFulFil(Address),
        #[prost(message, tag = "2")]
        Ok(super::SyncBlocks),
    }
}

#[derive(Copy, Clone, Default)]
pub struct SyncConfig {
    sync_interval: u64,
}

impl SyncManager {
    #[allow(dead_code)]
    pub async fn insert_blocks(&self, remote_address: Address, blocks: Vec<Block>) {
        let mut heights = vec![];
        for block in blocks {
            let header = block.header.clone().unwrap();
            {
                let rd = self.syncing_block_list.read().await;
                if rd.get(&header.height).is_some() {
                    continue;
                }
            }
            heights.push(header.height);
            {
                let mut wr = self.syncing_block_list.write().await;
                wr.insert(header.height, (remote_address.clone(), block));
            }
        }
        debug!("sync: insert_blocks: heights = {:?}", heights);
    }

    #[allow(dead_code)]
    pub async fn pop_blocks(&self, height: u64) -> Option<(Address, Block)> {
        {
            let mut wr = self.syncing_block_list.write().await;
            wr.remove(&height)
        }
    }

    #[allow(dead_code)]
    pub async fn clear(&self) {
        let mut wr = self.syncing_block_list.write().await;
        wr.clear();
    }

    pub async fn get_sync_block_req(
        &self,
        current_height: u64,
        global_status: &ChainStatus,
    ) -> SyncBlockRequest {
        let end_height = {
            if current_height + self.sync_config.sync_interval <= global_status.height {
                current_height + self.sync_config.sync_interval
            } else {
                global_status.height
            }
        };

        SyncBlockRequest {
            start_height: current_height + 1,
            end_height,
        }
    }
}
