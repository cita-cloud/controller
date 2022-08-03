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

use crate::util::{check_sig, crypto_client, get_compact_block};
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
    fmt::Display,
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
            if get_block_hash(crypto_client(), compact_block.header.as_ref()).await?
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

        let msg_hash = hash_data(crypto_client(), &chain_status_bytes).await?;
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
pub struct NodeAddress(pub u64);

impl Display for NodeAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "NodeAddress({})",
            hex::encode(self.0.to_be_bytes())
        ))
    }
}

impl From<&Address> for NodeAddress {
    fn from(address: &Address) -> Self {
        let tmp = &address.address[0..8];
        NodeAddress(u64::from_be_bytes(tmp.try_into().unwrap()))
    }
}

#[test]
fn test_node_address_display() {
    assert_eq!(
        &format!("{}", NodeAddress(8585939877295017053)),
        "NodeAddress(772762d40107cc5d)"
    );
}

#[derive(Clone, Default)]
pub struct NodeManager {
    pub nodes: Arc<RwLock<HashMap<NodeAddress, ChainStatus>>>,

    pub misbehavior_nodes: Arc<RwLock<HashMap<NodeAddress, MisbehaviorStatus>>>,

    pub ban_nodes: Arc<RwLock<HashSet<NodeAddress>>>,

    pub node_config: NodeConfig,
}

impl NodeManager {
    pub async fn in_node(&self, na: &NodeAddress) -> bool {
        let rd = self.nodes.read().await;
        rd.contains_key(na)
    }

    pub async fn delete_node(&self, na: &NodeAddress) -> Option<ChainStatus> {
        log::info!("delete node: {}", na);
        {
            let mut wr = self.nodes.write().await;
            wr.remove(na)
        }
    }

    pub async fn set_node(
        &self,
        na: &NodeAddress,
        chain_status: ChainStatus,
    ) -> Result<Option<ChainStatus>, StatusCode> {
        if self.in_ban_node(na).await {
            return Err(StatusCode::BannedNode);
        }

        if self.in_misbehavior_node(na).await && !self.try_delete_misbehavior_node(na).await {
            return Err(StatusCode::MisbehaveNode);
        }

        let status = {
            let rd = self.nodes.read().await;
            rd.get(na).cloned()
        };

        if status.is_none() || status.unwrap().height < chain_status.height {
            log::info!("update node: {}", na);
            let mut wr = self.nodes.write().await;
            Ok(wr.insert(*na, chain_status))
        } else {
            Err(StatusCode::EarlyStatus)
        }
    }

    pub async fn grab_node(&self) -> Vec<NodeAddress> {
        let mut keys: Vec<NodeAddress> = {
            let rd = self.nodes.read().await;
            rd.keys().copied().collect()
        };

        keys.shuffle(&mut thread_rng());

        keys.truncate(self.node_config.grab_node_num);
        keys
    }

    pub async fn pick_node(&self) -> (NodeAddress, ChainStatus) {
        let mut out_addr = NodeAddress(0);
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
                out_addr = *na;
            }
        }

        (out_addr, out_status)
    }

    pub async fn in_misbehavior_node(&self, na: &NodeAddress) -> bool {
        let rd = self.misbehavior_nodes.read().await;
        rd.contains_key(na)
    }

    pub async fn try_delete_misbehavior_node(&self, misbehavior_node: &NodeAddress) -> bool {
        let res = {
            let rd = self.misbehavior_nodes.read().await;
            rd.get(misbehavior_node).unwrap().free()
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
        misbehavior_node: &NodeAddress,
    ) -> Option<MisbehaviorStatus> {
        log::info!("delete misbehavior node: {}", misbehavior_node);
        {
            let mut wr = self.misbehavior_nodes.write().await;
            wr.remove(misbehavior_node)
        }
    }

    pub async fn set_misbehavior_node(
        &self,
        node: &NodeAddress,
    ) -> Result<Option<MisbehaviorStatus>, StatusCode> {
        if self.in_node(node).await {
            self.delete_node(node).await;
        }

        if self.in_ban_node(node).await {
            log::warn!("set misbehavior node: the node have been banned");
            return Err(StatusCode::BannedNode);
        }

        log::info!("set misbehavior node: {}", node);
        if let Some(mis_status) = {
            let rd = self.misbehavior_nodes.read().await;
            rd.get(node).cloned()
        } {
            let mut wr = self.misbehavior_nodes.write().await;
            Ok(wr.insert(*node, mis_status.update()))
        } else {
            let mut wr = self.misbehavior_nodes.write().await;
            Ok(wr.insert(*node, MisbehaviorStatus::default()))
        }
    }

    pub async fn in_ban_node(&self, node: &NodeAddress) -> bool {
        let rd = self.ban_nodes.read().await;
        rd.contains(node)
    }

    #[allow(dead_code)]
    pub async fn delete_ban_node(&self, ban_node: &NodeAddress) -> bool {
        log::info!("delete ban node: {}", ban_node);
        {
            let mut wr = self.ban_nodes.write().await;
            wr.remove(ban_node)
        }
    }

    pub async fn set_ban_node(&self, node: &NodeAddress) -> Result<bool, StatusCode> {
        if self.in_node(node).await {
            self.delete_node(node).await;
        }

        if self.in_misbehavior_node(node).await {
            self.delete_misbehavior_node(node).await;
        }

        log::info!("set ban node: {}", node);
        {
            let mut wr = self.ban_nodes.write().await;
            Ok(wr.insert(*node))
        }
    }

    pub async fn check_address_origin(
        &self,
        node: &NodeAddress,
        origin: NodeAddress,
    ) -> Result<bool, StatusCode> {
        if !self.nodes.read().await.contains_key(node) {
            return Ok(false);
        }

        if node == &origin {
            Ok(true)
        } else {
            let e = StatusCode::AddressOriginCheckError;
            log::warn!("check_address_origin: node({}) {:?} ", node, e);
            Err(e)
        }
    }
}
