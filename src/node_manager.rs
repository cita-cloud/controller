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

use crate::error::Error;
use crate::error::Error::BannedNode;
use cita_cloud_proto::common::{Address, Hash};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub struct ChainStatusWithFlag {
    pub status: ChainStatus,
    pub broadcast_or_not: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChainStatus {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub chain_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub height: u64,
    #[prost(message, optional, tag = "4")]
    pub hash: ::core::option::Option<Hash>,
    #[prost(message, optional, tag = "5")]
    pub address: ::core::option::Option<Address>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChainStatusRespond {
    #[prost(oneof = "chain_status_respond::Respond", tags = "1, 2")]
    pub respond: ::core::option::Option<chain_status_respond::Respond>,
}

/// Nested message and enum types in `ChainStatusRespond`.
pub mod chain_status_respond {

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Respond {
        #[prost(message, tag = "1")]
        NotSameChain(cita_cloud_proto::common::Address),
        #[prost(message, tag = "2")]
        Ok(super::ChainStatus),
    }
}

#[derive(Clone)]
pub struct MisbehaviorStatus {
    ban_times: u32,
    start_time: SystemTime,
    address: Address,
}

#[derive(Copy, Clone)]
pub struct NodeConfig {
    grab_node_num: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig { grab_node_num: 5 }
    }
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct NodeAddress([u8; 20]);

impl From<Address> for NodeAddress {
    fn from(addr: Address) -> Self {
        let mut slice = [0; 20];
        slice.copy_from_slice(addr.address.as_slice());
        Self(slice)
    }
}

impl NodeAddress {
    fn to_addr(&self) -> Address {
        Address {
            address: self.0.to_vec(),
        }
    }
}

#[derive(Clone, Default)]
pub struct NodeManager {
    pub node_origin: Arc<RwLock<HashMap<NodeAddress, u64>>>,

    pub nodes: Arc<RwLock<HashMap<NodeAddress, ChainStatus>>>,

    pub misbehavior_nodes: Arc<RwLock<HashMap<NodeAddress, MisbehaviorStatus>>>,

    pub ban_nodes: Arc<RwLock<HashSet<NodeAddress>>>,

    pub node_config: NodeConfig,
}

impl NodeManager {
    pub async fn set_origin(&self, node: Address, origin: u64) -> Option<u64> {
        let na = node.into();
        {
            let mut wr = self.node_origin.write().await;
            wr.insert(na, origin)
        }
    }

    pub async fn get_origin(&self, node: Address) -> Option<u64> {
        let na = node.into();
        {
            let rd = self.node_origin.read().await;
            rd.get(&na).cloned()
        }
    }

    pub async fn get_address(&self, session_id: u64) -> Option<Address> {
        let map = { self.node_origin.read().await.clone() };

        for (na, origin) in map.into_iter() {
            if origin == session_id {
                let addr = na.to_addr();
                return Some(addr);
            }
        }

        None
    }

    pub async fn in_node(&self, node: Address) -> bool {
        let na = node.into();
        {
            let rd = self.nodes.read().await;
            rd.contains_key(&na)
        }
    }

    pub async fn delete_node(&self, node: Address) -> Option<ChainStatus> {
        let na = node.into();
        {
            let mut wr = self.nodes.write().await;
            wr.remove(&na)
        }
    }

    pub async fn set_node(
        &self,
        node: Address,
        chain_status: ChainStatus,
    ) -> Result<Option<ChainStatus>, Error> {
        if self.in_ban_node(node.clone()).await {
            return Err(Error::BannedNode);
        }

        if self.in_misbehavior_node(node.clone()).await {
            return Err(Error::MisbehaveNode);
        }
        let na = node.into();
        if {
            let rd = self.nodes.read().await;
            let status = rd.get(&na);
            status.is_none() || status.unwrap().height < chain_status.height
        } {
            let mut wr = self.nodes.write().await;
            Ok(wr.insert(na, chain_status))
        } else {
            Err(Error::EarlyStatus)
        }
    }

    pub async fn grab_node(&self) -> Vec<Address> {
        let mut keys: Vec<Address> = {
            let rd = self.nodes.read().await;
            rd.keys().map(|na| na.to_addr()).collect()
        };

        keys.shuffle(&mut thread_rng());

        keys.truncate(self.node_config.grab_node_num);
        keys
    }

    pub async fn in_misbehavior_node(&self, node: Address) -> bool {
        let na = node.into();
        {
            let rd = self.misbehavior_nodes.read().await;
            rd.contains_key(&na)
        }
    }

    pub async fn delete_misbehavior_node(
        &self,
        misbehavior_node: Address,
    ) -> Option<MisbehaviorStatus> {
        let na = misbehavior_node.into();
        {
            let mut wr = self.misbehavior_nodes.write().await;
            wr.remove(&na)
        }
    }

    #[allow(dead_code)]
    pub async fn set_misbehavior_node(
        &self,
        node: Address,
        misbehavior_status: MisbehaviorStatus,
    ) -> Result<Option<MisbehaviorStatus>, Error> {
        if self.in_node(node.clone()).await {
            self.delete_node(node.clone()).await;
        }

        if self.in_ban_node(node.clone()).await {
            return Err(BannedNode);
        }

        let na = node.into();
        {
            let mut wr = self.misbehavior_nodes.write().await;
            Ok(wr.insert(na, misbehavior_status))
        }
    }

    pub async fn in_ban_node(&self, node: Address) -> bool {
        let na = node.into();
        {
            let rd = self.ban_nodes.read().await;
            rd.contains(&na)
        }
    }

    #[allow(dead_code)]
    pub async fn delete_ban_node(&self, ban_node: Address) -> bool {
        let na = ban_node.into();
        {
            let mut wr = self.ban_nodes.write().await;
            wr.remove(&na)
        }
    }

    pub async fn set_ban_node(&self, node: Address) -> Result<bool, Error> {
        if self.in_node(node.clone()).await {
            self.delete_node(node.clone()).await;
        }

        if self.in_misbehavior_node(node.clone()).await {
            self.delete_misbehavior_node(node.clone()).await;
        }

        let na = node.into();
        {
            let mut wr = self.ban_nodes.write().await;
            Ok(wr.insert(na))
        }
    }
}
