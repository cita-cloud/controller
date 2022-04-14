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

use crate::util::{check_sig, get_compact_block, kms_client};
use cita_cloud_proto::common::{Address, Hash};
use cloud_util::{
    common::h160_address_check,
    crypto::{get_block_hash, hash_data},
};
use prost::Message;
use rand::{seq::SliceRandom, thread_rng};
use status_code::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;

#[derive(Debug)]
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

impl ChainStatus {
    pub async fn check(&self, own_status: &ChainStatus) -> Result<(), StatusCode> {
        h160_address_check(self.address.as_ref())?;

        if self.chain_id != own_status.chain_id || self.version != own_status.version {
            log::warn!(
                "ChainStatus check error: {:?}",
                StatusCode::VersionOrIdCheckError
            );
            Err(StatusCode::VersionOrIdCheckError)
        } else {
            self.check_hash(own_status).await?;
            Ok(())
        }
    }

    pub async fn check_hash(&self, own_status: &ChainStatus) -> Result<(), StatusCode> {
        if own_status.height >= self.height {
            let compact_block = get_compact_block(self.height).await.map(|t| t.0)?;
            if get_block_hash(kms_client(), compact_block.header.as_ref()).await?
                != self.hash.clone().unwrap().hash
            {
                log::warn!(
                    "ChainStatus check_hash error: {:?}",
                    StatusCode::HashCheckError
                );
                Err(StatusCode::HashCheckError)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChainStatusInit {
    #[prost(message, optional, tag = "1")]
    pub chain_status: ::core::option::Option<ChainStatus>,
    #[prost(bytes = "vec", tag = "2")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}

impl ChainStatusInit {
    pub async fn check(&self, own_status: &ChainStatus) -> Result<(), StatusCode> {
        let chain_status = self
            .chain_status
            .clone()
            .ok_or(StatusCode::NoneChainStatus)?;

        let mut chain_status_bytes = Vec::new();
        chain_status.encode(&mut chain_status_bytes).map_err(|_| {
            log::warn!("ChainStatusInit: check: encode ChainStatus failed");
            StatusCode::EncodeError
        })?;

        let msg_hash = hash_data(kms_client(), &chain_status_bytes).await?;
        check_sig(
            &self.signature,
            &msg_hash,
            &self
                .chain_status
                .as_ref()
                .ok_or(StatusCode::NoneChainStatus)?
                .address
                .as_ref()
                .ok_or(StatusCode::NoProvideAddress)?
                .address,
        )
        .await?;

        chain_status.check(own_status).await?;

        Ok(())
    }
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
    }
}

#[derive(Clone, Copy)]
pub struct MisbehaviorStatus {
    ban_times: u32,
    start_time: SystemTime,
}

impl Default for MisbehaviorStatus {
    fn default() -> Self {
        Self {
            ban_times: 0,
            start_time: SystemTime::now(),
        }
    }
}

impl MisbehaviorStatus {
    fn update(mut self) -> Self {
        self.ban_times += 1;
        self.start_time = SystemTime::now();
        self
    }

    fn free(&self) -> bool {
        let elapsed = self
            .start_time
            .elapsed()
            .expect("Clock may have gone backwards");
        elapsed >= Duration::from_secs(30) * 2u32.pow(self.ban_times)
    }
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

impl From<&Address> for NodeAddress {
    fn from(addr: &Address) -> Self {
        let mut slice = [0; 20];
        slice.copy_from_slice(addr.address.as_slice());
        Self(slice)
    }
}

impl NodeAddress {
    fn to_addr(self) -> Address {
        Address {
            address: self.to_vec(),
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.0.to_vec()
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
    pub async fn set_origin(&self, node: &Address, origin: u64) -> Option<u64> {
        let na: NodeAddress = node.into();
        if let Some(addr) = self.get_address(origin).await {
            if &addr == node {
                // same node set again, do nothing
                log::info!("set origin: same node({}) set again", origin);
                return None;
            } else {
                // delete repeat origin link
                log::warn!("set origin: exist repeat origin: {}", origin);
                self.delete_origin(&addr).await;
                self.delete_node(&addr).await;
            }
        }
        log::info!("set origin[{}] to node: 0x{}", origin, hex::encode(&na.0));
        let mut wr = self.node_origin.write().await;
        wr.insert(na, origin)
    }

    pub async fn delete_origin(&self, node: &Address) {
        let na: NodeAddress = node.into();
        log::info!("delete origin of node: 0x{}", hex::encode(&na.0));
        let mut wr = self.node_origin.write().await;
        wr.remove(&na);
    }

    pub async fn get_origin(&self, node: &Address) -> Option<u64> {
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

    pub async fn in_node(&self, node: &Address) -> bool {
        let na: NodeAddress = node.into();
        {
            let rd = self.nodes.read().await;
            rd.contains_key(&na)
        }
    }

    pub async fn delete_node(&self, node: &Address) -> Option<ChainStatus> {
        let na: NodeAddress = node.into();
        log::info!("delete node: 0x{}", hex::encode(&na.0));
        {
            let mut wr = self.nodes.write().await;
            wr.remove(&na)
        }
    }

    pub async fn set_node(
        &self,
        node: &Address,
        chain_status: ChainStatus,
    ) -> Result<Option<ChainStatus>, StatusCode> {
        if self.in_ban_node(node).await {
            return Err(StatusCode::BannedNode);
        }

        if self.in_misbehavior_node(node).await && !self.try_delete_misbehavior_node(node).await {
            return Err(StatusCode::MisbehaveNode);
        }
        let na: NodeAddress = node.into();

        let status = {
            let rd = self.nodes.read().await;
            rd.get(&na).cloned()
        };

        if status.is_none() || status.unwrap().height < chain_status.height {
            log::info!("update node: 0x{}", hex::encode(&na.0));
            let mut wr = self.nodes.write().await;
            Ok(wr.insert(na, chain_status))
        } else {
            Err(StatusCode::EarlyStatus)
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

    pub async fn pick_node(&self) -> (Address, ChainStatus) {
        let mut out_addr = Address { address: vec![] };
        let mut out_status = ChainStatus {
            version: 0,
            chain_id: vec![],
            height: 0,
            hash: None,
            address: None,
        };
        let rd = self.nodes.read().await;
        for (na, status) in rd.iter() {
            if status.height > out_status.height {
                out_status = status.clone();
                out_addr = Address {
                    address: na.0.to_vec(),
                };
            }
        }

        (out_addr, out_status)
    }

    pub async fn in_misbehavior_node(&self, node: &Address) -> bool {
        let na = node.into();
        {
            let rd = self.misbehavior_nodes.read().await;
            rd.contains_key(&na)
        }
    }

    pub async fn try_delete_misbehavior_node(&self, misbehavior_node: &Address) -> bool {
        let na: NodeAddress = misbehavior_node.into();
        let res = {
            let rd = self.misbehavior_nodes.read().await;
            rd.get(&na).unwrap().free()
        };
        if res {
            self.delete_misbehavior_node(misbehavior_node).await;
            true
        } else {
            false
        }
    }

    pub async fn delete_misbehavior_node(
        &self,
        misbehavior_node: &Address,
    ) -> Option<MisbehaviorStatus> {
        let na: NodeAddress = misbehavior_node.into();
        log::info!("delete misbehavior node: 0x{}", hex::encode(&na.0));
        {
            let mut wr = self.misbehavior_nodes.write().await;
            wr.remove(&na)
        }
    }

    pub async fn set_misbehavior_node(
        &self,
        node: &Address,
    ) -> Result<Option<MisbehaviorStatus>, StatusCode> {
        self.delete_origin(node).await;

        if self.in_node(node).await {
            self.delete_node(node).await;
        }

        if self.in_ban_node(node).await {
            return Err(StatusCode::BannedNode);
        }

        let na: NodeAddress = node.into();
        log::info!("set misbehavior node: 0x{}", hex::encode(&na.0));
        if let Some(mis_status) = {
            let rd = self.misbehavior_nodes.read().await;
            rd.get(&na).cloned()
        } {
            let mut wr = self.misbehavior_nodes.write().await;
            Ok(wr.insert(na, mis_status.update()))
        } else {
            let mut wr = self.misbehavior_nodes.write().await;
            Ok(wr.insert(na, MisbehaviorStatus::default()))
        }
    }

    pub async fn in_ban_node(&self, node: &Address) -> bool {
        let na = node.into();
        {
            let rd = self.ban_nodes.read().await;
            rd.contains(&na)
        }
    }

    #[allow(dead_code)]
    pub async fn delete_ban_node(&self, ban_node: &Address) -> bool {
        let na: NodeAddress = ban_node.into();
        log::info!("delete ban node: 0x{}", hex::encode(&na.0));
        {
            let mut wr = self.ban_nodes.write().await;
            wr.remove(&na)
        }
    }

    pub async fn set_ban_node(&self, node: &Address) -> Result<bool, StatusCode> {
        self.delete_origin(node).await;

        if self.in_node(node).await {
            self.delete_node(node).await;
        }

        if self.in_misbehavior_node(node).await {
            self.delete_misbehavior_node(node).await;
        }

        let na: NodeAddress = node.into();
        log::info!("set ban node: 0x{}", hex::encode(&na.0));
        {
            let mut wr = self.ban_nodes.write().await;
            Ok(wr.insert(na))
        }
    }

    pub async fn check_address_origin(
        &self,
        node: &Address,
        origin: u64,
    ) -> Result<bool, StatusCode> {
        let record_origin = {
            let na = node.into();
            self.node_origin.read().await.get(&na).cloned()
        };

        if record_origin.is_none() {
            Ok(false)
        } else if record_origin != Some(origin) {
            let e = StatusCode::AddressOriginCheckError;
            log::warn!(
                "check_address_origin: node(0x{}) {:?} ",
                hex::encode(&node.address),
                e
            );
            Err(e)
        } else {
            Ok(true)
        }
    }
}
