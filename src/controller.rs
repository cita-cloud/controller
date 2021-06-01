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

use crate::auth::Authentication;
use crate::chain::{Chain, ChainStep};
use crate::error::Error;
use crate::event::EventTask;
use crate::node_manager::{
    chain_status_respond::Respond, ChainStatus, ChainStatusRespond, NodeManager,
};
use crate::pool::Pool;
use crate::protocol::sync_manager::{SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager};
use crate::sync::Notifier;
use crate::util::{check_block, check_block_exists, check_tx_exists, extract_tx_hash, full_to_compact, get_compact_block, get_full_block, get_network_status, get_tx, hash_data, header_to_block_hash, load_data, load_tx_info, remove_tx, write_block, write_tx};
use crate::utxo_set::SystemConfig;
use crate::GenesisBlock;
use crate::{impl_broadcast, impl_multicast, impl_unicast};
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::{Block, RawTransaction, RawTransactions};
use cita_cloud_proto::blockchain::{CompactBlock, CompactBlockBody};
use cita_cloud_proto::common::{Address, ConsensusConfiguration, Hash, SimpleResponse};
use cita_cloud_proto::network::NetworkMsg;
use log::{info, warn};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;

#[derive(Debug)]
pub enum ControllerMsgType {
    ChainStatusType,
    ChainStatusRespondType,
    SyncBlockType,
    SyncBlockRespondType,
    SendTxType,
    SendProposalType,
    Noop,
}

impl From<&str> for ControllerMsgType {
    fn from(s: &str) -> Self {
        match s {
            "chain_status" => Self::ChainStatusType,
            "chain_status_respond" => Self::ChainStatusRespondType,
            "sync_block" => Self::SyncBlockType,
            "sync_block_respond" => Self::SyncBlockRespondType,
            "send_txs" => Self::SendTxType,
            "send_proposal" => Self::SendProposalType,
            _ => Self::Noop,
        }
    }
}

impl ::std::fmt::Display for ControllerMsgType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<ControllerMsgType> for &str {
    fn from(t: ControllerMsgType) -> Self {
        match t {
            ControllerMsgType::ChainStatusType => "chain_status",
            ControllerMsgType::ChainStatusRespondType => "chain_status_respond",
            ControllerMsgType::SyncBlockType => "sync_block",
            ControllerMsgType::SyncBlockRespondType => "sync_block_respond",
            ControllerMsgType::SendTxType => "send_txs",
            ControllerMsgType::SendProposalType => "send_proposal",
            ControllerMsgType::Noop => "noop",
        }
    }
}

#[derive(Clone)]
pub struct Controller {
    pub network_port: u16,
    storage_port: u16,
    auth: Arc<RwLock<Authentication>>,
    pool: Arc<RwLock<Pool>>,
    chain: Arc<RwLock<Chain>>,
    blocks_notifier: Arc<Notifier>,

    consensus_port: u16,

    kms_port: u16,

    pub local_address: Address,

    current_status: Arc<RwLock<ChainStatus>>,

    global_status: Arc<RwLock<(Address, ChainStatus)>>,

    node_manager: NodeManager,

    sync_manager: SyncManager,
}

impl Controller {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        consensus_port: u16,
        network_port: u16,
        storage_port: u16,
        kms_port: u16,
        executor_port: u16,
        block_delay_number: u32,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        sys_config: SystemConfig,
        genesis: GenesisBlock,
        blocks_notifier: Arc<Notifier>,
        key_id: u64,
        node_address: Vec<u8>,
        task_sender: crossbeam::channel::Sender<EventTask>,
    ) -> Self {
        let auth = Arc::new(RwLock::new(Authentication::new(
            kms_port,
            storage_port,
            sys_config,
        )));
        let pool = Arc::new(RwLock::new(Pool::new(1500)));
        let chain = Arc::new(RwLock::new(Chain::new(
            storage_port,
            kms_port,
            executor_port,
            consensus_port,
            block_delay_number,
            current_block_number,
            current_block_hash,
            pool.clone(),
            auth.clone(),
            genesis,
            key_id,
            node_address.clone(),
            task_sender,
        )));
        Controller {
            network_port,
            storage_port,
            auth,
            pool,
            chain,
            blocks_notifier,
            consensus_port,
            kms_port,
            local_address: Address {
                address: node_address,
            },
            current_status: Arc::new(RwLock::new(ChainStatus::default())),
            global_status: Arc::new(RwLock::new((
                Address {
                    address: Vec::new(),
                },
                ChainStatus::default(),
            ))),
            node_manager: NodeManager::default(),
            sync_manager: SyncManager::default(),
        }
    }

    pub async fn init(&self, init_block_number: u64, sys_config: SystemConfig) {
        {
            let chain = self.chain.write().await;
            chain.init(init_block_number).await;
        }
        {
            let mut auth = self.auth.write().await;
            auth.init(init_block_number).await;
        }
        let status = self
            .init_status(init_block_number, sys_config)
            .await
            .unwrap();
        self.set_status(status.clone()).await;
        self.broadcast_chain_status(self.network_port, status).await;

        self.proc_sync_notify().await;
    }

    pub async fn proc_sync_notify(&self) {
        let blocks_notifier_clone = self.blocks_notifier.clone();
        let chain_clone = self.chain.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(30, 0));
            loop {
                interval.tick().await;
                let current_block_number = {
                    let chain = chain_clone.read().await;
                    chain.get_block_number(false)
                };
                {
                    blocks_notifier_clone.list(current_block_number);
                }
            }
        });

        let blocks_notifier_clone = self.blocks_notifier.clone();
        tokio::spawn(async move {
            blocks_notifier_clone.watch().await;
        });

        let blocks_notifier_clone = self.blocks_notifier.clone();
        let chain_clone = self.chain.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(12, 0));
            loop {
                interval.tick().await;
                {
                    while let Some(event) = blocks_notifier_clone.queue.pop() {
                        if event.filename.as_str().parse::<u64>().is_ok() {
                            {
                                let mut chain = chain_clone.write().await;
                                chain.proc_sync_block().await;
                                continue;
                            }
                        }
                        warn!("sync block invalid {}", event.filename.as_str());
                    }
                }
            }
        });
    }

    pub async fn rpc_get_block_number(&self, is_pending: bool) -> Result<u64, String> {
        let chain = self.chain.read().await;
        let block_number = chain.get_block_number(is_pending);
        Ok(block_number)
    }

    pub async fn rpc_send_raw_transaction(
        &self,
        raw_tx: RawTransaction,
        broadcast: bool,
    ) -> Result<Vec<u8>, String> {
        let tx_hash = {
            let auth = self.auth.read().await;
            auth.check_raw_tx(raw_tx.clone()).await?
        };

        let is_exists = check_tx_exists(tx_hash.as_slice());
        if !is_exists {
            let mut raw_tx_bytes: Vec<u8> = Vec::new();
            let _ = raw_tx.encode(&mut raw_tx_bytes);
            write_tx(tx_hash.as_slice(), raw_tx_bytes.as_slice()).await;

            let raw_txs = RawTransactions { body: vec![raw_tx] };

            let mut pool = self.pool.write().await;
            let is_ok = pool.enqueue(tx_hash.clone());
            if is_ok {
                if broadcast {
                    self.multicast_send_txs(self.network_port, raw_txs).await;
                }
                Ok(tx_hash)
            } else {
                remove_tx(hex::encode(tx_hash).as_str()).await;
                Err("dup".to_owned())
            }
        } else {
            Err("dup".to_owned())
        }

    }

    pub async fn rpc_get_block_by_hash(&self, hash: Vec<u8>) -> Result<CompactBlock, String> {
        let block_number = load_data(self.storage_port, 8, hash)
            .await
            .map_err(|_| "load block number failed".to_owned())
            .map(|v| {
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&v[..8]);
                u64::from_be_bytes(bytes)
            })?;
        self.rpc_get_block_by_number(block_number).await
    }

    pub async fn rpc_get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, String> {
        load_data(self.storage_port, 4, block_number.to_be_bytes().to_vec())
            .await
            .map_err(|_| "load block hash failed".to_owned())
    }

    pub async fn rpc_get_tx_block_number(&self, tx_hash: Vec<u8>) -> Result<u64, String> {
        if let Some((block_number, _)) = load_tx_info(tx_hash.as_slice()).await {
            Ok(block_number)
        } else {
            Err("load tx info failed".to_owned())
        }
    }

    pub async fn rpc_get_tx_index(&self, tx_hash: Vec<u8>) -> Result<u64, String> {
        if let Some((_, tx_index)) = load_tx_info(tx_hash.as_slice()).await {
            Ok(tx_index)
        } else {
            Err("load tx info failed".to_owned())
        }
    }

    pub async fn rpc_get_peer_count(&self) -> Result<u64, String> {
        get_network_status(self.network_port)
            .await
            .map_err(|_| "get network status failed".to_owned())
            .map(|status| status.peer_count)
    }

    pub async fn rpc_get_block_by_number(&self, block_number: u64) -> Result<CompactBlock, String> {
        let chain = self.chain.read().await;
        let ret = chain.get_block_by_number(block_number).await;
        if ret.is_none() {
            Err("can't find block by number".to_owned())
        } else {
            Ok(ret.unwrap())
        }
    }

    pub async fn rpc_get_transaction(&self, tx_hash: Vec<u8>) -> Result<RawTransaction, String> {
        let ret = get_tx(&tx_hash).await;
        if let Some(raw_tx) = ret {
            Ok(raw_tx)
        } else {
            Err("can't get transaction".to_owned())
        }
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, String> {
        let auth = self.auth.read().await;
        let sys_config = auth.get_system_config();
        Ok(sys_config)
    }

    pub async fn chain_get_proposal(&self) -> Result<(u64, Vec<u8>), Error> {
        if let Some(block) = {
            let mut chain = self.chain.write().await;
            chain.add_proposal().await
        } {
            self.multicast_send_proposal(self.network_port, block).await;
        }
        {
            let chain = self.chain.read().await;
            chain.get_proposal().await
        }
    }

    pub async fn chain_check_proposal(&self, height: u64, proposal: &[u8]) -> Result<bool, Error> {
        let chain = self.chain.read().await;
        chain.check_proposal(height, proposal).await
    }

    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<ConsensusConfiguration, Error> {
        let mut chain = self.chain.write().await;

        let (_, status) = self.get_global_status().await;

        if status.height >= height {
            let rd = self.chain.read().await;
            let config = rd.get_system_config().await;
            return Ok(ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            });
        }

        chain.commit_block(height, proposal, proof).await
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<SimpleResponse, Error> {
        info!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::ChainStatusType
                    ))
                })?;

                let controller_clone = self.clone();
                tokio::spawn(async move {
                    let own_status = {
                        let rd = controller_clone.current_status.read().await;
                        rd.clone()
                    };

                    if own_status.chain_id != chain_status.chain_id
                        || own_status.version != chain_status.version
                    {
                        let chain_status_respond = ChainStatusRespond {
                            respond: Some(Respond::NotSameChain(
                                controller_clone.local_address.clone(),
                            )),
                        };

                        controller_clone
                            .unicast_chain_status_respond(
                                controller_clone.network_port,
                                msg.origin,
                                chain_status_respond,
                            )
                            .await;
                    }

                    if own_status.height >= chain_status.height {
                        let own_old_compact_block = {
                            let rd = controller_clone.chain.read().await;
                            rd.get_block_by_number(chain_status.height)
                                .await
                                .expect("a specified block not get!")
                        };

                        let own_old_block_hash = header_to_block_hash(
                            controller_clone.kms_port,
                            own_old_compact_block.header.unwrap(),
                        )
                        .await
                        .unwrap();

                        if let Some(ext_hash) = chain_status.hash.clone() {
                            if ext_hash.hash != own_old_block_hash {
                                let chain_status_respond = ChainStatusRespond {
                                    respond: Some(Respond::NotSameChain(
                                        controller_clone.local_address.clone(),
                                    )),
                                };

                                controller_clone
                                    .unicast_chain_status_respond(
                                        controller_clone.network_port,
                                        msg.origin,
                                        chain_status_respond,
                                    )
                                    .await;
                            }
                        }
                    }

                    let node = chain_status.address.clone().unwrap();
                    match controller_clone
                        .node_manager
                        .set_node(node.clone(), chain_status)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("{}", e.to_string());
                            return;
                        }
                    }
                    controller_clone
                        .node_manager
                        .set_origin(node, msg.origin)
                        .await;

                    let chain_status_respond = ChainStatusRespond {
                        respond: Some(Respond::Ok(own_status)),
                    };

                    controller_clone
                        .unicast_chain_status_respond(
                            controller_clone.network_port,
                            msg.origin,
                            chain_status_respond,
                        )
                        .await;
                });
            }

            ControllerMsgType::ChainStatusRespondType => {
                let chain_status_respond =
                    ChainStatusRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        Error::DecodeError(format!(
                            "decode {} msg failed",
                            ControllerMsgType::ChainStatusRespondType
                        ))
                    })?;

                match chain_status_respond.respond {
                    Some(respond) => match respond {
                        Respond::NotSameChain(node) => {
                            self.node_manager.set_ban_node(node.clone()).await?;
                            self.delete_global_status(node).await;
                        }
                        Respond::Ok(chain_status) => {
                            if chain_status.address.is_some() {
                                let _ = self.try_update_global_status(
                                    chain_status.address.clone().unwrap(),
                                    chain_status,
                                    msg.origin,
                                );
                            }
                        }
                    },
                    None => {}
                }
            }

            ControllerMsgType::SyncBlockType => {
                let sync_block_request =
                    SyncBlockRequest::decode(msg.msg.as_slice()).map_err(|_| {
                        Error::DecodeError(format!(
                            "decode {} msg failed",
                            ControllerMsgType::SyncBlockType
                        ))
                    })?;

                let controller = self.clone();
                tokio::spawn(async move {
                    let mut block_vec = Vec::new();

                    for h in sync_block_request.start_height..=sync_block_request.end_height {
                        if let Some((compact_block, proof)) = get_compact_block(h).await {
                            let full_block = get_full_block(compact_block, proof).await.unwrap();
                            block_vec.push(full_block);
                        } else {
                            let sync_block_respond = SyncBlockRespond {
                                respond: Some(crate::protocol::sync_manager::sync_block_respond::Respond::NotFulFil(controller.local_address.clone()))
                            };
                            controller
                                .unicast_sync_block_respond(
                                    controller.network_port,
                                    msg.origin,
                                    sync_block_respond,
                                )
                                .await;
                        }
                    }

                    let sync_block = SyncBlocks {
                        address: Some(controller.local_address.clone()),
                        sync_blocks: block_vec,
                    };
                    let sync_block_respond = SyncBlockRespond {
                        respond: Some(
                            crate::protocol::sync_manager::sync_block_respond::Respond::Ok(
                                sync_block,
                            ),
                        ),
                    };
                    controller
                        .unicast_sync_block_respond(
                            controller.network_port,
                            msg.origin,
                            sync_block_respond,
                        )
                        .await;
                });
            }

            ControllerMsgType::SyncBlockRespondType => {
                let sync_block_respond =
                    SyncBlockRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        Error::DecodeError(format!(
                            "decode {} msg failed",
                            ControllerMsgType::SyncBlockRespondType
                        ))
                    })?;

                let controller = self.clone();
                tokio::spawn(async move {
                    match sync_block_respond.respond {
                        // todo add counter, after reaching a number, misbehave this node
                        Some(
                            crate::protocol::sync_manager::sync_block_respond::Respond::NotFulFil(
                                _,
                            ),
                        ) => {}
                        None => {}
                        Some(crate::protocol::sync_manager::sync_block_respond::Respond::Ok(
                            sync_blocks,
                        )) => {
                            // todo handle error
                            match controller.handle_sync_blocks(sync_blocks).await {
                                Ok(_) => {}
                                Err(e) => {
                                    warn!("{}", e.to_string());
                                    return;
                                }
                            }
                        }
                    }
                });
            }

            ControllerMsgType::SendTxType => {
                let send_txs = RawTransactions::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SendTxType
                    ))
                })?;

                for raw_tx in send_txs.body {
                    self.rpc_send_raw_transaction(raw_tx, false)
                        .await
                        .map_err(Error::ExpectError)?;
                }
            }

            ControllerMsgType::SendProposalType => {
                let block = Block::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SendProposalType
                    ))
                })?;

                if let Some(header) = block.header.clone() {
                    let mut block_header_bytes = Vec::new();
                    header
                        .encode(&mut block_header_bytes)
                        .map_err(|_| Error::EncodeError(format!("encode block header failed")))?;

                    let block_hash = hash_data(self.kms_port, block_header_bytes)
                        .await
                        .map_err(Error::InternalError)?;

                    let controller_clone = self.clone();
                    tokio::spawn(async move {
                        {
                            let compact_block = full_to_compact(block.clone());
                            {
                                let mut wr = controller_clone.chain.write().await;
                                if !wr.add_remote_proposal(compact_block).await {
                                    warn!("add remote proposal: {} failed", hex::encode(block_hash))
                                }
                            }
                            if let Some(body) = block.body {
                                for raw_tx in body.body {
                                    match controller_clone.rpc_send_raw_transaction(raw_tx, false).await {
                                        Ok(_) => {},
                                        Err(e) => warn!("process_network_msg: rpc_send_raw_transaction: error: {}", e)
                                    }
                                }
                            }
                        }
                    });
                }
            }

            ControllerMsgType::Noop => match self.node_manager.get_address(msg.origin).await {
                Some(address) => {
                    self.node_manager.set_ban_node(address.clone()).await?;
                    self.delete_global_status(address).await;
                }
                None => {}
            },
        }

        Ok(SimpleResponse { is_success: true })
    }

    impl_broadcast!(broadcast_chain_status, ChainStatus, "chain_status");

    impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    impl_multicast!(multicast_send_proposal, Block, "send_proposal");
    impl_multicast!(multicast_send_txs, RawTransactions, "send_txs");

    impl_unicast!(unicast_sync_block, SyncBlockRequest, "sync_block");
    impl_unicast!(
        unicast_sync_block_respond,
        SyncBlockRespond,
        "sync_block_respond"
    );
    impl_unicast!(
        unicast_chain_status_respond,
        ChainStatusRespond,
        "chain_status_respond"
    );

    async fn get_global_status(&self) -> (Address, ChainStatus) {
        let rd = self.global_status.read().await;
        rd.clone()
    }

    async fn update_global_status(&self, node: Address, status: ChainStatus) {
        let mut wr = self.global_status.write().await;
        *wr = (node, status);
    }

    async fn delete_global_status(&self, node: Address) -> bool {
        if {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            gs.0 == node
        } {
            let mut wr = self.global_status.write().await;
            *wr = (Address { address: vec![] }, ChainStatus::default());
            true
        } else {
            false
        }
    }

    async fn try_update_global_status(
        &self,
        node: Address,
        status: ChainStatus,
        origin: u64,
    ) -> Result<bool, Error> {
        self.node_manager
            .set_node(node.clone(), status.clone())
            .await?;
        self.node_manager.set_origin(node.clone(), origin).await;

        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        if status.height > old_status.1.height && status.height >= own_status.height {
            self.update_global_status(node, status.clone()).await;
            self.try_sync_block().await;

            return Ok(true);
        }

        Ok(false)
    }

    async fn init_status(&self, height: u64, config: SystemConfig) -> Result<ChainStatus, Error> {
        let compact_block = {
            if height == 0 {
                let rd = self.chain.read().await;
                rd.get_genesis_block()
            } else {
                get_compact_block(height)
                    .await
                    .ok_or(Error::NoBlock(height))?
                    .0
            }
        };
        let mut header_bytes = Vec::new();
        compact_block
            .header
            .unwrap()
            .encode(&mut header_bytes)
            .map_err(|_| Error::EncodeError(format!("encode compact block error")))?;
        let block_hash = hash_data(self.kms_port, header_bytes)
            .await
            .map_err(Error::InternalError)?;

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash { hash: block_hash }),
            address: Some(self.local_address.clone()),
        })
    }

    async fn get_status(&self) -> ChainStatus {
        let rd = self.current_status.read().await;
        rd.clone()
    }

    async fn set_status(&self, status: ChainStatus) {
        let mut wr = self.current_status.write().await;
        *wr = status;
    }

    pub async fn update_from_chain(&self, status: ChainStatus) -> bool {
        let old_status = {
            let rd = self.current_status.read().await;
            rd.clone()
        };

        if old_status.height < status.height {
            let mut wr = self.current_status.write().await;
            wr.height = status.height;
            wr.hash = status.hash;
            true
        } else {
            false
        }
    }

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, Error> {
        let mut block_header_bytes = Vec::new();
        // write tx
        for sync_block in sync_blocks.sync_blocks.clone() {
            let block_height: u64;
            if let Some(ref header) = sync_block.header {
                header
                    .encode(&mut block_header_bytes)
                    .map_err(|_| Error::EncodeError("encode block header failed".to_string()))?;

                if !check_block_exists(header.height) {
                    return Err(Error::DupBlock(header.height));
                }

                block_height = header.height;
            } else {
                return Err(Error::NoneBlockHeader);
            }

            let mut proposal = Vec::new();
            sync_block
                .encode(&mut proposal)
                .map_err(|_| Error::EncodeError("encode proposal failed".to_string()))?;

            check_block(
                self.consensus_port,
                block_height,
                proposal,
                sync_block.proof.clone(),
            )
            .await
            .map_err(|e| Error::InternalError(e))?;

            let mut compact_block_body = CompactBlockBody { tx_hashes: vec![] };

            if let Some(body) = sync_block.body {
                for tx in body.body {
                    self.rpc_send_raw_transaction(tx.clone(), false)
                        .await
                        .map_err(|_| {
                            Error::DupTransaction(
                                extract_tx_hash(tx.clone()).expect("could not get tx hash"),
                            )
                        })?;

                    match tx.tx {
                        Some(Tx::NormalTx(normal_tx)) => {
                            compact_block_body
                                .tx_hashes
                                .push(normal_tx.transaction_hash);
                        }
                        Some(Tx::UtxoTx(utxo_tx)) => {
                            compact_block_body.tx_hashes.push(utxo_tx.transaction_hash);
                        }
                        None => {}
                    }
                }
            }

            let mut compact_block_body_bytes = Vec::new();

            compact_block_body
                .encode(&mut compact_block_body_bytes)
                .map_err(|_| Error::EncodeError("encode compact block body failed".to_string()))?;

            write_block(
                block_height,
                block_header_bytes.as_slice(),
                compact_block_body_bytes.as_slice(),
                sync_block.proof.as_slice(),
            )
            .await;
        }

        Ok(sync_blocks.sync_blocks.len())
    }

    pub async fn try_sync_block(&self) {
        let current_height = { self.current_status.read().await.height };

        let (global_address, global_status) = { self.global_status.read().await.clone() };

        let origin = { self.node_manager.get_origin(global_address).await.unwrap() };

        match {
            let chain = self.chain.read().await;
            chain.next_step(&global_status).await
        } {
            ChainStep::SyncStep => {
                let sync_req = self
                    .sync_manager
                    .get_sync_block_req(current_height, &global_status)
                    .await;
                self.unicast_sync_block(self.network_port, origin, sync_req)
                    .await;
            }
            _ => {}
        }
    }
}
