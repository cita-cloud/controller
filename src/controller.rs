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

use crate::{
    auth::Authentication,
    chain::{Chain, ChainStep},
    config::ControllerConfig,
    event::EventTask,
    node_manager::{
        chain_status_respond::Respond, ChainStatus, ChainStatusInit, ChainStatusRespond,
        NodeManager,
    },
    pool::Pool,
    protocol::sync_manager::{
        SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager, SyncTxRequest, SyncTxRespond,
    },
    util::*,
    utxo_set::SystemConfig,
    GenesisBlock, {impl_broadcast, impl_multicast, impl_unicast},
};
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    common::{
        proposal_enum::Proposal, Address, ConsensusConfiguration, Empty, Hash, Hashes, NodeInfo,
        NodeNetInfo, ProposalEnum, TotalNodeInfo,
    },
    network::NetworkMsg,
};
use cloud_util::{
    clean_0x,
    common::{get_tx_hash, h160_address_check},
    crypto::{get_block_hash, hash_data, sign_message},
    storage::load_data,
};
use prost::Message;
use status_code::StatusCode;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, RwLock},
    time,
};
use tonic::{Request, Response};

#[derive(Debug)]
pub enum ControllerMsgType {
    ChainStatusInitType,
    ChainStatusInitRequestType,
    ChainStatusType,
    ChainStatusRespondType,
    SyncBlockType,
    SyncBlockRespondType,
    SyncTxType,
    SyncTxRespondType,
    SendTxType,
    SendTxsType,
    Noop,
}

impl From<&str> for ControllerMsgType {
    fn from(s: &str) -> Self {
        match s {
            "chain_status_init" => Self::ChainStatusInitType,
            "chain_status_init_req" => Self::ChainStatusInitRequestType,
            "chain_status" => Self::ChainStatusType,
            "chain_status_respond" => Self::ChainStatusRespondType,
            "sync_block" => Self::SyncBlockType,
            "sync_block_respond" => Self::SyncBlockRespondType,
            "sync_tx" => Self::SyncTxType,
            "sync_tx_respond" => Self::SyncTxRespondType,
            "send_tx" => Self::SendTxType,
            "send_txs" => Self::SendTxsType,
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
            ControllerMsgType::ChainStatusInitType => "chain_status_init",
            ControllerMsgType::ChainStatusInitRequestType => "chain_status_init_req",
            ControllerMsgType::ChainStatusType => "chain_status",
            ControllerMsgType::ChainStatusRespondType => "chain_status_respond",
            ControllerMsgType::SyncBlockType => "sync_block",
            ControllerMsgType::SyncBlockRespondType => "sync_block_respond",
            ControllerMsgType::SyncTxType => "sync_tx",
            ControllerMsgType::SyncTxRespondType => "sync_tx_respond",
            ControllerMsgType::SendTxType => "send_tx",
            ControllerMsgType::SendTxsType => "send_txs",
            ControllerMsgType::Noop => "noop",
        }
    }
}

#[derive(Clone)]
pub struct Controller {
    pub(crate) config: ControllerConfig,

    pub(crate) auth: Arc<RwLock<Authentication>>,

    pool: Arc<RwLock<Pool>>,

    pub(crate) chain: Arc<RwLock<Chain>>,

    pub(crate) local_address: Address,

    current_status: Arc<RwLock<ChainStatus>>,

    global_status: Arc<RwLock<(Address, ChainStatus)>>,

    pub(crate) node_manager: NodeManager,

    pub(crate) sync_manager: SyncManager,

    pub(crate) task_sender: mpsc::Sender<EventTask>,
    // sync state flag
    is_sync: Arc<RwLock<bool>>,
}

impl Controller {
    pub fn new(
        config: ControllerConfig,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        sys_config: SystemConfig,
        genesis: GenesisBlock,
        task_sender: mpsc::Sender<EventTask>,
    ) -> Self {
        let node_address = hex::decode(clean_0x(&config.node_address)).unwrap();
        log::info!("node address: {}", &config.node_address);

        h160_address_check(Some(&Address {
            address: node_address.clone(),
        }))
        .unwrap();

        let own_status = ChainStatus {
            version: sys_config.version,
            chain_id: sys_config.chain_id.clone(),
            height: 0,
            hash: Some(Hash {
                hash: current_block_hash.clone(),
            }),
            address: Some(Address {
                address: node_address.clone(),
            }),
        };

        let pool = Arc::new(RwLock::new(Pool::new(
            config.package_limit as usize,
            sys_config.block_limit,
        )));
        let auth = Arc::new(RwLock::new(Authentication::new(sys_config)));
        let chain = Arc::new(RwLock::new(Chain::new(
            current_block_number,
            current_block_hash,
            pool.clone(),
            auth.clone(),
            genesis,
            &config.wal_path,
        )));

        Controller {
            sync_manager: SyncManager::new(&config),
            node_manager: NodeManager::default(),

            config,
            auth,
            pool,
            chain,
            local_address: Address {
                address: node_address,
            },
            current_status: Arc::new(RwLock::new(own_status)),
            global_status: Arc::new(RwLock::new((
                Address {
                    address: Vec::new(),
                },
                ChainStatus::default(),
            ))),
            task_sender,
            is_sync: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn init(&self, init_block_number: u64, sys_config: SystemConfig) {
        let sys_config_clone = sys_config.clone();
        let mut consensus_config = ConsensusConfiguration {
            height: init_block_number,
            block_interval: sys_config_clone.block_interval,
            validators: sys_config_clone.validators,
        };
        if let Some((new_consensus_config, status)) = {
            let mut chain = self.chain.write().await;
            chain
                .init(init_block_number, self.config.server_retry_interval)
                .await;
            chain.init_auth(init_block_number).await;
            chain.load_wal_log().await
        } {
            self.set_status(status.clone()).await;
            consensus_config = new_consensus_config;
            log::info!("wal redo status_height: {}", status.height);
        } else {
            let status = self
                .init_status(init_block_number, sys_config)
                .await
                .unwrap();
            self.set_status(status.clone()).await;
            log::info!("init_block_number: {}", init_block_number);
        }
        // send configuration to consensus
        let mut server_retry_interval =
            time::interval(Duration::from_secs(self.config.server_retry_interval));
        loop {
            server_retry_interval.tick().await;
            {
                if reconfigure(consensus_config.clone())
                    .await
                    .is_success()
                    .is_ok()
                {
                    log::info!("reconfigure consensus success!");
                    break;
                } else {
                    log::warn!("reconfigure consensus failed! Retrying")
                }
            }
        }
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
    ) -> Result<Vec<u8>, StatusCode> {
        let tx_hash = get_tx_hash(&raw_tx)?.to_vec();

        {
            let auth = self.auth.read().await;
            auth.check_raw_tx(&raw_tx).await?;
        }

        let res = {
            let mut pool = self.pool.write().await;
            pool.enqueue(raw_tx.clone())
        };
        if res {
            if broadcast {
                self.multicast_send_tx(raw_tx).await;
            }
            Ok(tx_hash)
        } else {
            log::warn!(
                "rpc_send_raw_transaction: found dup tx(0x{}) in pool",
                hex::encode(&tx_hash)
            );
            Err(StatusCode::DupTransaction)
        }
    }

    pub async fn batch_transactions(
        &self,
        raw_txs: RawTransactions,
        broadcast: bool,
    ) -> Result<Hashes, StatusCode> {
        match kms_client().check_transactions(raw_txs.clone()).await {
            Ok(response) => StatusCode::from(response.into_inner()).is_success()?,
            Err(e) => {
                log::warn!(
                    "batch_transactions: check_transactions failed: {}",
                    e.to_string()
                );
                return Err(StatusCode::KmsServerNotReady);
            }
        }

        let mut hashes = Vec::new();
        {
            let auth = self.auth.read().await;
            let mut pool = self.pool.write().await;
            auth.check_transactions(&raw_txs)?;
            for raw_tx in raw_txs.body.clone() {
                let hash = get_tx_hash(&raw_tx)?.to_vec();
                if pool.enqueue(raw_tx) {
                    hashes.push(Hash { hash })
                }
            }
        }
        if broadcast {
            self.multicast_send_txs(raw_txs).await;
        }
        Ok(Hashes { hashes })
    }

    pub async fn rpc_get_block_by_hash(&self, hash: Vec<u8>) -> Result<CompactBlock, StatusCode> {
        let block_number = load_data(storage_client(), 8, hash.clone())
            .await
            .map_err(|e| {
                log::warn!(
                    "load block(0x{})'s height failed, error: {}",
                    hex::encode(&hash),
                    e.to_string()
                );
                StatusCode::NoBlockHeight
            })
            .map(|v| {
                let mut bytes: [u8; 8] = [0; 8];
                bytes.clone_from_slice(&v[..8]);
                u64::from_be_bytes(bytes)
            })?;
        self.rpc_get_block_by_number(block_number).await
    }

    pub async fn rpc_get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, StatusCode> {
        load_data(storage_client(), 4, block_number.to_be_bytes().to_vec())
            .await
            .map_err(|e| {
                log::warn!(
                    "load block({})'s hash failed, error: {}",
                    block_number,
                    e.to_string()
                );
                StatusCode::NoBlockHeight
            })
    }

    pub async fn rpc_get_tx_block_number(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCode> {
        load_tx_info(&tx_hash).await.map(|t| t.0)
    }

    pub async fn rpc_get_tx_index(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCode> {
        load_tx_info(&tx_hash).await.map(|t| t.1)
    }

    pub async fn rpc_get_peer_count(&self) -> Result<u64, StatusCode> {
        get_network_status().await.map(|status| status.peer_count)
    }

    pub async fn rpc_get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<CompactBlock, StatusCode> {
        get_compact_block(block_number).await.map(|t| t.0)
    }

    pub async fn rpc_get_block_detail_by_number(
        &self,
        block_number: u64,
    ) -> Result<Block, StatusCode> {
        get_full_block(block_number).await
    }

    pub async fn rpc_get_transaction(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<RawTransaction, StatusCode> {
        db_get_tx(&tx_hash).await
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, StatusCode> {
        let auth = self.auth.read().await;
        let sys_config = auth.get_system_config();
        Ok(sys_config)
    }

    pub async fn rpc_add_node(
        &self,
        request: Request<NodeNetInfo>,
    ) -> Response<cita_cloud_proto::common::StatusCode> {
        let res = network_client()
            .add_node(request)
            .await
            .unwrap_or_else(|e| {
                log::warn!("rpc_add_node failed: {}", e.to_string());
                Response::new(StatusCode::NetworkServerNotReady.into())
            });

        let controller_for_add = self.clone();
        let code = StatusCode::from(res.get_ref().code);
        if code == StatusCode::Success || code == StatusCode::AddExistedPeer {
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(
                    controller_for_add.config.server_retry_interval,
                ))
                .await;
                controller_for_add
                    .task_sender
                    .send(EventTask::BroadCastCSI)
                    .await
                    .unwrap();
            });
        }
        res
    }

    pub async fn rpc_get_peers_info(
        &self,
        request: Request<Empty>,
    ) -> Result<TotalNodeInfo, StatusCode> {
        let mut tnis = Vec::new();
        let tnni = network_client()
            .get_peers_net_info(request)
            .await
            .map_err(|e| {
                log::warn!("rpc_get_peers_info failed: {}", e.to_string());
                StatusCode::NetworkServerNotReady
            })?
            .into_inner();

        for record_node in self.node_manager.node_origin.read().await.iter() {
            for node_info in tnni.nodes.iter() {
                if *record_node.1 == node_info.origin {
                    tnis.push(NodeInfo {
                        address: record_node.0.to_vec(),
                        net_info: Some(node_info.clone()),
                    })
                }
            }
        }

        Ok(TotalNodeInfo { nodes: tnis })
    }

    pub async fn chain_get_proposal(&self) -> Result<(u64, Vec<u8>, StatusCode), StatusCode> {
        if self.get_sync_state().await {
            return Err(StatusCode::NodeInSyncMode);
        }

        let mut chain = self.chain.write().await;
        chain
            .add_proposal(
                &self.get_global_status().await.1,
                self.local_address.address.clone(),
            )
            .await?;
        chain.get_proposal().await
    }

    pub async fn chain_check_proposal(&self, height: u64, data: &[u8]) -> Result<(), StatusCode> {
        let proposal_enum = ProposalEnum::decode(data).map_err(|_| {
            log::warn!("chain_check_proposal: decode ProposalEnum failed");
            StatusCode::DecodeError
        })?;

        let ret = {
            let chain = self.chain.read().await;
            chain.check_proposal(height, proposal_enum.clone()).await
        };

        match ret {
            Ok(_) => match proposal_enum.proposal {
                Some(Proposal::BftProposal(bft_proposal)) => {
                    let config = self.config.clone();
                    let block = bft_proposal.proposal.ok_or(StatusCode::NoneProposal)?;
                    if block
                        .body
                        .as_ref()
                        .ok_or(StatusCode::NoneBlockBody)?
                        .body
                        .len()
                        > config.package_limit as usize
                    {
                        return Err(StatusCode::TransactionsExceed);
                    }
                    let block_hash = get_block_hash(kms_client(), block.header.as_ref()).await?;

                    // todo re-enter check
                    let res = {
                        log::info!(
                            "chain_check_proposal: add remote proposal(0x{})",
                            hex::encode(&block_hash)
                        );
                        let mut chain = self.chain.write().await;
                        !chain.is_own(&block_hash)
                            && chain
                                .add_remote_proposal(&block_hash, block.clone())
                                .await?
                    };
                    if res {
                        self.batch_transactions(
                            block.body.ok_or(StatusCode::NoneBlockBody)?,
                            false,
                        )
                        .await?;

                        log::info!("chain_check_proposal: finished");
                    } else {
                        log::info!("the proposal is own");
                    }
                }
                None => return Err(StatusCode::NoneProposal),
            },
            Err(StatusCode::ProposalTooHigh) => {
                self.multicast_chain_status(self.get_status().await).await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate();
                }
                self.try_sync_block().await;
            }
            _ => {}
        }

        ret
    }

    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<ConsensusConfiguration, StatusCode> {
        let status = self.get_status().await;

        if status.height >= height {
            let rd = self.auth.read().await;
            let config = rd.get_system_config();
            return Ok(ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            });
        }

        let res = {
            let mut chain = self.chain.write().await;
            chain.commit_block(height, proposal, proof).await
        };

        match res {
            Ok((config, mut status)) => {
                status.address = Some(self.local_address.clone());
                self.set_status(status.clone()).await;
                self.multicast_chain_status(status).await;
                self.try_sync_block().await;
                Ok(config)
            }
            Err(StatusCode::ProposalTooHigh) => {
                self.multicast_chain_status(self.get_status().await).await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate();
                }
                self.try_sync_block().await;
                Err(StatusCode::ProposalTooHigh)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<(), StatusCode> {
        log::debug!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusInitType => {
                let chain_status_init =
                    ChainStatusInit::decode(msg.msg.as_slice()).map_err(|_| {
                        log::warn!(
                            "process_network_msg: decode {} msg failed",
                            ControllerMsgType::ChainStatusInitType
                        );
                        StatusCode::DecodeError
                    })?;

                let own_status = self.get_status().await;
                match chain_status_init.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCode::VersionOrIdCheckError | StatusCode::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                let node = chain_status_init
                                    .chain_status
                                    .clone()
                                    .ok_or(StatusCode::NoneChainStatus)?
                                    .address
                                    .ok_or(StatusCode::NoProvideAddress)?;
                                self.delete_global_status(&node).await;
                                self.node_manager.set_ban_node(&node).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let status = chain_status_init
                    .chain_status
                    .ok_or(StatusCode::NoneChainStatus)?;
                let node = status.address.clone().ok_or(StatusCode::NoProvideAddress)?;
                self.node_manager.set_origin(&node, msg.origin).await;

                match self.node_manager.set_node(&node, status.clone()).await {
                    Ok(None) => {
                        let chain_status_init = self.make_csi(own_status).await?;
                        self.unicast_chain_status_init(msg.origin, chain_status_init)
                            .await;
                    }
                    Ok(Some(_)) => {
                        if own_status.height > status.height {
                            self.unicast_chain_status(msg.origin, own_status).await;
                        }
                    }
                    Err(status_code) => {
                        if status_code == StatusCode::EarlyStatus
                            && own_status.height < status.height
                        {
                            {
                                let mut chain = self.chain.write().await;
                                chain.clear_candidate();
                            }
                            let chain_status_init = self.make_csi(own_status).await?;
                            self.unicast_chain_status_init(msg.origin, chain_status_init)
                                .await;
                            self.try_update_global_status(&node, status).await?;
                        }
                        return Err(status_code);
                    }
                }
                self.try_update_global_status(&node, status).await?;
            }
            ControllerMsgType::ChainStatusInitRequestType => {
                let chain_status_init = self.make_csi(self.get_status().await).await?;

                self.unicast_chain_status_init(msg.origin, chain_status_init)
                    .await;
            }
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    log::warn!("decode {} msg failed", ControllerMsgType::ChainStatusType);
                    StatusCode::DecodeError
                })?;

                let own_status = self.get_status().await;
                match chain_status.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCode::VersionOrIdCheckError | StatusCode::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                let node = chain_status.address.clone().unwrap();
                                self.delete_global_status(&node).await;
                                self.node_manager.set_ban_node(&node).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let node = chain_status.address.clone().unwrap();
                match self
                    .node_manager
                    .check_address_origin(&node, msg.origin)
                    .await
                {
                    Ok(true) => {
                        self.node_manager
                            .set_node(&node, chain_status.clone())
                            .await?;
                        self.try_update_global_status(&node, chain_status).await?;
                    }
                    // give Ok or Err for process_network_msg is same
                    Err(StatusCode::AddressOriginCheckError) | Ok(false) => {
                        self.unicast_chain_status_init_req(msg.origin, own_status)
                            .await;
                    }
                    Err(e) => return Err(e),
                }
            }

            ControllerMsgType::ChainStatusRespondType => {
                let chain_status_respond =
                    ChainStatusRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        log::warn!(
                            "decode {} msg failed",
                            ControllerMsgType::ChainStatusRespondType
                        );
                        StatusCode::DecodeError
                    })?;

                if let Some(respond) = chain_status_respond.respond {
                    match respond {
                        Respond::NotSameChain(node) => {
                            h160_address_check(Some(&node))?;
                            self.delete_global_status(&node).await;
                            self.node_manager.set_ban_node(&node).await?;
                        }
                    }
                }
            }

            ControllerMsgType::SyncBlockType => {
                let sync_block_request =
                    SyncBlockRequest::decode(msg.msg.as_slice()).map_err(|_| {
                        log::warn!("decode {} msg failed", ControllerMsgType::SyncBlockType);
                        StatusCode::DecodeError
                    })?;

                log::info!(
                    "get sync_block_req: {}-{}",
                    sync_block_request.start_height,
                    sync_block_request.end_height
                );
                self.task_sender
                    .send(EventTask::SyncBlockReq(sync_block_request, msg.origin))
                    .await
                    .unwrap();
            }

            ControllerMsgType::SyncBlockRespondType => {
                let sync_block_respond =
                    SyncBlockRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        log::warn!(
                            "decode {} msg failed",
                            ControllerMsgType::SyncBlockRespondType
                        );
                        StatusCode::DecodeError
                    })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_block_respond::Respond;

                tokio::spawn(async move {
                    match sync_block_respond.respond {
                        // todo check origin
                        Some(Respond::MissBlock(node)) => {
                            controller_clone.delete_global_status(&node).await;
                            controller_clone
                                .node_manager
                                .set_misbehavior_node(&node)
                                .await
                                .unwrap();
                        }
                        Some(Respond::Ok(sync_blocks)) => {
                            // todo handle error
                            match controller_clone
                                .handle_sync_blocks(sync_blocks.clone())
                                .await
                            {
                                Ok(_) => {
                                    if controller_clone
                                        .sync_manager
                                        .contains_block(
                                            controller_clone.get_status().await.height + 1,
                                        )
                                        .await
                                    {
                                        controller_clone
                                            .task_sender
                                            .send(EventTask::SyncBlock)
                                            .await
                                            .unwrap();
                                    }
                                }
                                Err(StatusCode::ProvideAddressError)
                                | Err(StatusCode::NoProvideAddress) => {
                                    log::warn!(
                                        "sync_block_respond error, origin: {}, message: given address error",
                                        msg.origin,
                                    );
                                }
                                Err(e) => {
                                    log::warn!(
                                        "sync_block_respond error, origin: {}, message: {}",
                                        msg.origin,
                                        e.to_string()
                                    );

                                    controller_clone
                                        .node_manager
                                        .set_misbehavior_node(sync_blocks.address.as_ref().unwrap())
                                        .await
                                        .unwrap();
                                    controller_clone
                                        .delete_global_status(sync_blocks.address.as_ref().unwrap())
                                        .await;
                                }
                            }
                        }
                        None => {}
                    }
                });
            }

            ControllerMsgType::SyncTxType => {
                let sync_tx = SyncTxRequest::decode(msg.msg.as_slice()).map_err(|_| {
                    log::warn!("decode {} msg failed", ControllerMsgType::SyncTxType);
                    StatusCode::DecodeError
                })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                tokio::spawn(async move {
                    if let Ok(raw_tx) = {
                        let rd = controller_clone.chain.read().await;
                        rd.chain_get_tx(&sync_tx.tx_hash).await
                    } {
                        controller_clone
                            .unicast_sync_tx_respond(
                                msg.origin,
                                SyncTxRespond {
                                    respond: Some(Respond::Ok(raw_tx)),
                                },
                            )
                            .await;
                    }
                });
            }

            ControllerMsgType::SyncTxRespondType => {
                let sync_tx_respond = SyncTxRespond::decode(msg.msg.as_slice()).map_err(|_| {
                    log::warn!("decode {} msg failed", ControllerMsgType::SyncTxRespondType);
                    StatusCode::DecodeError
                })?;

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                match sync_tx_respond.respond {
                    Some(Respond::MissTx(node)) => {
                        self.node_manager.set_misbehavior_node(&node).await?;
                        self.delete_global_status(&node).await;
                    }
                    Some(Respond::Ok(raw_tx)) => {
                        self.rpc_send_raw_transaction(raw_tx, false).await?;
                    }
                    None => {}
                }
            }

            ControllerMsgType::SendTxType => {
                let send_tx = RawTransaction::decode(msg.msg.as_slice()).map_err(|_| {
                    log::warn!("decode {} msg failed", ControllerMsgType::SendTxType);
                    StatusCode::DecodeError
                })?;

                self.rpc_send_raw_transaction(send_tx, false).await?;
            }

            ControllerMsgType::SendTxsType => {
                let body = RawTransactions::decode(msg.msg.as_slice()).map_err(|_| {
                    log::warn!("decode {} msg failed", ControllerMsgType::SendTxsType);
                    StatusCode::DecodeError
                })?;

                self.batch_transactions(body, false).await?;
            }

            ControllerMsgType::Noop => {
                if let Some(address) = self.node_manager.get_address(msg.origin).await {
                    self.delete_global_status(&address).await;
                    self.node_manager.set_ban_node(&address).await?;
                }
            }
        }

        Ok(())
    }

    impl_broadcast!(
        broadcast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );

    impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    impl_multicast!(multicast_send_tx, RawTransaction, "send_tx");
    impl_multicast!(multicast_send_txs, RawTransactions, "send_txs");
    // impl_multicast!(multicast_sync_tx, SyncTxRequest, "sync_tx");
    // impl_multicast!(multicast_sync_block, SyncBlockRequest, "sync_block");

    impl_unicast!(unicast_chain_status, ChainStatus, "chain_status");
    impl_unicast!(
        unicast_chain_status_init_req,
        ChainStatus,
        "chain_status_init_req"
    );
    impl_unicast!(
        unicast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_unicast!(unicast_sync_block, SyncBlockRequest, "sync_block");
    impl_unicast!(
        unicast_sync_block_respond,
        SyncBlockRespond,
        "sync_block_respond"
    );
    impl_unicast!(unicast_sync_tx_respond, SyncTxRespond, "sync_tx_respond");
    impl_unicast!(
        unicast_chain_status_respond,
        ChainStatusRespond,
        "chain_status_respond"
    );

    pub async fn get_global_status(&self) -> (Address, ChainStatus) {
        let rd = self.global_status.read().await;
        rd.clone()
    }

    pub async fn update_global_status(&self, node: Address, status: ChainStatus) {
        let mut wr = self.global_status.write().await;
        *wr = (node, status);
    }

    async fn delete_global_status(&self, node: &Address) -> bool {
        let res = {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            &gs.0 == node
        };
        if res {
            let mut wr = self.global_status.write().await;
            *wr = (Address { address: vec![] }, ChainStatus::default());
            true
        } else {
            false
        }
    }

    async fn try_update_global_status(
        &self,
        node: &Address,
        status: ChainStatus,
    ) -> Result<bool, StatusCode> {
        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        if status.height > old_status.1.height && status.height >= own_status.height {
            log::info!(
                "update global status node(0x{}) height({})",
                hex::encode(&node.address),
                status.height
            );
            let global_height = status.height;
            self.update_global_status(node.to_owned(), status).await;
            if global_height > own_status.height {
                self.try_sync_block().await;
            }
            if (!self.get_sync_state().await || global_height % self.config.force_sync_epoch == 0)
                && self
                    .sync_manager
                    .contains_block(own_status.height + 1)
                    .await
            {
                self.task_sender.send(EventTask::SyncBlock).await.unwrap();
            }

            return Ok(true);
        }

        // request block if own height behind remote's
        if status.height > own_status.height {
            self.try_sync_block().await;
        }

        Ok(false)
    }

    async fn init_status(
        &self,
        height: u64,
        config: SystemConfig,
    ) -> Result<ChainStatus, StatusCode> {
        let compact_block = get_compact_block(height).await?.0;

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash {
                hash: get_block_hash(kms_client(), compact_block.header.as_ref()).await?,
            }),
            address: Some(self.local_address.clone()),
        })
    }

    pub async fn get_status(&self) -> ChainStatus {
        let rd = self.current_status.read().await;
        rd.clone()
    }

    pub async fn set_status(&self, mut status: ChainStatus) {
        if h160_address_check(status.address.as_ref()).is_err() {
            status.address = Some(self.local_address.clone())
        }

        let mut wr = self.current_status.write().await;
        *wr = status;
    }

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, StatusCode> {
        h160_address_check(sync_blocks.address.as_ref())?;

        let own_height = self.get_status().await.height;
        Ok(self
            .sync_manager
            .insert_blocks(
                sync_blocks.address.ok_or(StatusCode::NoProvideAddress)?,
                sync_blocks.sync_blocks,
                own_height,
            )
            .await)
    }

    pub async fn try_sync_block(&self) {
        let (_, global_status) = self.get_global_status().await;
        // sync mode will return exclude global_height % self.config.force_sync_epoch == 0
        if self.get_sync_state().await && global_status.height % self.config.force_sync_epoch != 0 {
            return;
        }

        let mut current_height = self.get_status().await.height;
        let controller_clone = self.clone();
        tokio::spawn(async move {
            for _ in 0..controller_clone.config.sync_req {
                let (global_address, global_status) = controller_clone.get_global_status().await;

                if let Err(e) = h160_address_check(Some(&global_address)) {
                    log::debug!("try_sync_block: global_address error: {:?}", e);
                    return;
                }

                let origin = controller_clone
                    .node_manager
                    .get_origin(&global_address)
                    .await
                    .unwrap();

                // try read chain state, if can't get chain default online state
                let res = {
                    if let Ok(chain) = controller_clone.chain.try_read() {
                        chain.next_step(&global_status).await
                    } else {
                        ChainStep::BusyState
                    }
                };

                match res {
                    ChainStep::SyncStep => {
                        if let Some(sync_req) = controller_clone
                            .sync_manager
                            .get_sync_block_req(current_height, &global_status)
                            .await
                        {
                            current_height = sync_req.end_height;
                            controller_clone
                                .unicast_sync_block(origin, sync_req.clone())
                                .await
                                .await
                                .unwrap();
                            if sync_req.start_height == sync_req.end_height {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                    ChainStep::OnlineStep => {
                        controller_clone.set_sync_state(false).await;
                        controller_clone.sync_manager.clear().await;
                        return;
                    }
                    ChainStep::BusyState => return,
                }
            }
        });
    }

    pub async fn sync_block(&self) -> Result<(), StatusCode> {
        let mut current_height = self.get_status().await.height;
        for _ in 0..self.config.sync_req {
            let (global_address, global_status) = self.get_global_status().await;

            if let Err(e) = h160_address_check(Some(&global_address)) {
                log::warn!("sync_block: global_address error: {:?}", e);
                return Err(e);
            }

            let origin = self.node_manager.get_origin(&global_address).await.unwrap();

            let res = {
                let chain = self.chain.read().await;
                chain.next_step(&global_status).await
            };

            match res {
                ChainStep::SyncStep => {
                    if let Some(sync_req) = self
                        .sync_manager
                        .get_sync_block_req(current_height, &global_status)
                        .await
                    {
                        current_height = sync_req.end_height;
                        self.unicast_sync_block(origin, sync_req.clone())
                            .await
                            .await
                            .unwrap();
                        if sync_req.start_height == sync_req.end_height {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                ChainStep::OnlineStep => {
                    self.set_sync_state(false).await;
                    self.sync_manager.clear().await;
                    break;
                }
                ChainStep::BusyState => unreachable!(),
            }
        }
        Ok(())
    }

    pub async fn make_csi(&self, own_status: ChainStatus) -> Result<ChainStatusInit, StatusCode> {
        let mut chain_status_bytes = Vec::new();
        own_status.encode(&mut chain_status_bytes).map_err(|_| {
            log::warn!("process_network_msg: encode ChainStatus failed");
            StatusCode::EncodeError
        })?;
        let msg_hash = hash_data(kms_client(), &chain_status_bytes).await?;
        let signature = sign_message(kms_client(), self.config.key_id, &msg_hash).await?;

        Ok(ChainStatusInit {
            chain_status: Some(own_status),
            signature,
        })
    }

    pub async fn get_sync_state(&self) -> bool {
        *self.is_sync.read().await
    }

    pub async fn set_sync_state(&self, state: bool) {
        let mut wr = self.is_sync.write().await;
        *wr = state;
    }
}
