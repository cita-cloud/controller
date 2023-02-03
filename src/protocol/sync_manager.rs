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

use crate::{config::ControllerConfig, node_manager::ChainStatus};
use cita_cloud_proto::{blockchain::Block, common::Address};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SyncManager {
    syncing_block_list: Arc<RwLock<BTreeMap<u64, (Address, Block)>>>,

    sync_config: SyncConfig,
}

#[derive(Clone, Eq, PartialEq, ::prost::Message)]
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
        MissBlock(Address),
        #[prost(message, tag = "2")]
        Ok(super::SyncBlocks),
    }
}

#[derive(Clone, Eq, PartialEq, ::prost::Message)]
pub struct SyncTxRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub tx_hash: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncTxRespond {
    #[prost(oneof = "sync_tx_respond::Respond", tags = "1, 2")]
    pub respond: ::core::option::Option<sync_tx_respond::Respond>,
}

/// Nested message and enum types in `SyncTxRespond`.
pub mod sync_tx_respond {
    use cita_cloud_proto::blockchain::RawTransaction;
    use cita_cloud_proto::common::Address;

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Respond {
        #[prost(message, tag = "1")]
        MissTx(Address),
        #[prost(message, tag = "2")]
        Ok(RawTransaction),
    }
}

#[derive(Copy, Clone)]
pub struct SyncConfig {
    sync_interval: u64,
    sync_req: u64,
}

impl SyncConfig {
    fn new(config: &ControllerConfig) -> Self {
        Self {
            sync_interval: config.sync_interval,
            sync_req: config.sync_req,
        }
    }
}

impl SyncManager {
    pub fn new(config: &ControllerConfig) -> Self {
        Self {
            syncing_block_list: Arc::new(RwLock::new(BTreeMap::new())),
            sync_config: SyncConfig::new(config),
        }
    }

    pub async fn insert_blocks(
        &self,
        remote_address: Address,
        blocks: Vec<Block>,
        own_height: u64,
    ) -> usize {
        let mut heights = vec![];
        {
            let mut wr = self.syncing_block_list.write().await;
            for block in blocks {
                let height = block.header.clone().unwrap().height;
                if own_height >= height || wr.get(&height).is_some() {
                    continue;
                }

                heights.push(height);
                wr.insert(height, (remote_address.clone(), block));
            }
        }
        if !heights.is_empty() {
            info!(
                "sync: insert_blocks: heights = {:?} from node(0x{})",
                heights,
                hex::encode(&remote_address.address)
            );
        }

        heights.len()
    }

    pub async fn pop_block(&self, height: u64) -> Option<(Address, Block)> {
        let mut wr = self.syncing_block_list.write().await;
        wr.remove(&height)
    }

    pub async fn contains_block(&self, height: u64) -> bool {
        let rd = self.syncing_block_list.read().await;
        rd.contains_key(&height)
    }

    pub async fn remove_blocks(&self, heights: Vec<u64>) {
        {
            let mut wr = self.syncing_block_list.write().await;
            for height in heights {
                wr.remove(&height);
            }
        }
    }

    pub async fn clear_node_block(
        &self,
        node: &Address,
        own_status: &ChainStatus,
    ) -> Option<Vec<(u64, u64)>> {
        let mut range_vec = Vec::new();
        let mut start = u64::MAX;
        let mut end = u64::MAX;
        let mut remove_heights = Vec::new();
        let mut eazy = false;

        {
            let rd = self.syncing_block_list.read().await;
            for (&height, (addr, _)) in rd.iter() {
                if node == addr {
                    remove_heights.push(height);
                    if height > own_status.height && !eazy {
                        eazy = true;
                        start = height;
                    }
                    end = height;
                } else if start != u64::MAX && eazy {
                    eazy = false;
                    range_vec.push((start, end));
                    info!("clear_node_block: push range_vec: ({}, {})", start, end);
                }
            }
            if start != u64::MAX && end != u64::MAX {
                range_vec.push((start, end));
                info!("clear_node_block: push range_vec: ({}, {})", start, end);
            }
        }

        self.remove_blocks(remove_heights).await;

        Some(range_vec)
    }

    pub async fn clear(&self) {
        let mut wr = self.syncing_block_list.write().await;
        wr.clear();
    }

    pub async fn get_sync_block_req(
        &self,
        current_height: u64,
        global_status: &ChainStatus,
    ) -> Option<SyncBlockRequest> {
        let current_height = {
            let rd = self.syncing_block_list.read().await;
            if rd.contains_key(&(current_height + 1)) {
                return None;
            } else {
                current_height
            }
        };

        let end_height = {
            if current_height + self.sync_config.sync_interval <= global_status.height {
                current_height + self.sync_config.sync_interval
            } else if current_height >= global_status.height {
                return None;
            } else {
                global_status.height
            }
        };

        info!(
            "SyncBlockRequest: start {}, end {}",
            current_height + 1,
            end_height
        );

        Some(SyncBlockRequest {
            start_height: current_height + 1,
            end_height,
        })
    }

    pub fn re_sync_block_req(
        &self,
        height_range: (u64, u64),
        global_status: &ChainStatus,
    ) -> Option<Vec<SyncBlockRequest>> {
        let mut req_vec = Vec::new();

        let mut height_range = {
            if global_status.height >= height_range.1 && global_status.height >= height_range.0 {
                height_range
            } else if global_status.height < height_range.1 {
                (height_range.0, global_status.height)
            } else {
                return None;
            }
        };

        for _ in 0..self.sync_config.sync_req {
            let start_height = height_range.0;
            let end_height = {
                if height_range.0 + self.sync_config.sync_interval <= height_range.1 {
                    height_range.0 + self.sync_config.sync_interval
                } else {
                    height_range.1
                }
            };
            req_vec.push(SyncBlockRequest {
                start_height,
                end_height,
            });

            height_range.0 = end_height + 1;

            if height_range.0 > height_range.1 {
                break;
            }
        }

        Some(req_vec)
    }
}
