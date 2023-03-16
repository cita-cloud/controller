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
        NodeAddress, NodeManager,
    },
    pool::Pool,
    protocol::sync_manager::{
        SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager, SyncTxRequest, SyncTxRespond,
    },
    system_config::SystemConfig,
    util::*,
    GenesisBlock, {impl_broadcast, impl_multicast, impl_unicast},
};
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    client::{CryptoClientTrait, NetworkClientTrait},
    common::{
        proposal_enum::Proposal, Address, ConsensusConfiguration, Empty, Hash, Hashes, NodeNetInfo,
        NodeStatus, PeerStatus, Proof, ProposalEnum, StateRoot,
    },
    controller::BlockNumber,
    network::NetworkMsg,
    storage::Regions,
};
use cloud_util::{
    clean_0x,
    common::{get_tx_hash, h160_address_check},
    crypto::{get_block_hash, hash_data, sign_message},
    storage::load_data,
    unix_now,
    wal::Wal,
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, RwLock},
    time,
};

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
        write!(f, "{self:?}")
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

    pub(crate) validator_address: Address,

    current_status: Arc<RwLock<ChainStatus>>,

    global_status: Arc<RwLock<(NodeAddress, ChainStatus)>>,

    pub(crate) node_manager: NodeManager,

    pub(crate) sync_manager: SyncManager,

    pub(crate) task_sender: mpsc::Sender<EventTask>,
    // sync state flag
    is_sync: Arc<RwLock<bool>>,

    pub(crate) forward_pool: Arc<RwLock<RawTransactions>>,

    pub initial_sys_config: SystemConfig,

    pub init_block_number: u64,
}

impl Controller {
    pub async fn new(
        config: ControllerConfig,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        sys_config: SystemConfig,
        genesis: GenesisBlock,
        task_sender: mpsc::Sender<EventTask>,
        initial_sys_config: SystemConfig,
    ) -> Self {
        let node_address = hex::decode(clean_0x(&config.node_address)).unwrap();
        info!("node address: {}", &config.node_address);
        //
        let validator_address = hex::decode(&clean_0x(&config.validator_address)[..40]).unwrap();
        info!("validator address: {}", &config.validator_address[..40]);

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
            sys_config.block_limit,
            sys_config.quota_limit,
        )));
        let auth = Arc::new(RwLock::new(Authentication::new(sys_config)));
        let chain = Arc::new(RwLock::new(Chain::new(
            current_block_number,
            current_block_hash,
            pool.clone(),
            auth.clone(),
            genesis,
            Wal::create(&config.wal_path).await.unwrap(),
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
            validator_address: Address {
                address: validator_address,
            },
            current_status: Arc::new(RwLock::new(own_status)),
            global_status: Arc::new(RwLock::new((NodeAddress(0), ChainStatus::default()))),
            task_sender,
            is_sync: Arc::new(RwLock::new(false)),
            forward_pool: Arc::new(RwLock::new(RawTransactions { body: vec![] })),
            initial_sys_config,
            init_block_number: current_block_number,
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
            info!(
                "wal redo success: height: {}, hash: 0x{}",
                status.height,
                hex::encode(status.hash.unwrap().hash)
            );
        } else {
            let status = self
                .init_status(init_block_number, sys_config)
                .await
                .unwrap();
            self.set_status(status.clone()).await;
        }
        // send configuration to consensus
        let mut server_retry_interval =
            time::interval(Duration::from_secs(self.config.server_retry_interval));
        tokio::spawn(async move {
            loop {
                server_retry_interval.tick().await;
                {
                    if reconfigure(consensus_config.clone())
                        .await
                        .is_success()
                        .is_ok()
                    {
                        info!("consensus service ready");
                        break;
                    } else {
                        warn!("consensus service not ready: retrying...")
                    }
                }
            }
        });
    }

    pub async fn rpc_get_block_number(&self, _is_pending: bool) -> Result<u64, String> {
        let block_number = self.get_status().await.height;
        Ok(block_number)
    }

    pub async fn rpc_send_raw_transaction(
        &self,
        raw_tx: RawTransaction,
        broadcast: bool,
    ) -> Result<Vec<u8>, StatusCodeEnum> {
        let tx_hash = get_tx_hash(&raw_tx)?.to_vec();

        {
            let auth = self.auth.read().await;
            auth.check_raw_tx(&raw_tx).await?;
        }

        let res = {
            let mut pool = self.pool.write().await;
            pool.insert(raw_tx.clone())
        };
        if res {
            if broadcast {
                let mut f_pool = self.forward_pool.write().await;
                f_pool.body.push(raw_tx);
                if f_pool.body.len() > self.config.count_per_batch {
                    self.broadcast_send_txs(f_pool.clone()).await;
                    f_pool.body.clear();
                }
            }
            Ok(tx_hash)
        } else {
            warn!(
                "rpc send raw transaction failed: tx already in pool. hash: 0x{}",
                hex::encode(&tx_hash)
            );
            Err(StatusCodeEnum::DupTransaction)
        }
    }

    pub async fn batch_transactions(
        &self,
        raw_txs: RawTransactions,
        broadcast: bool,
    ) -> Result<Hashes, StatusCodeEnum> {
        match crypto_client().check_transactions(raw_txs.clone()).await {
            Ok(code) => StatusCodeEnum::from(code).is_success()?,
            Err(e) => {
                warn!(
                    "batch transactions failed: check_transactions failed: {}",
                    e.to_string()
                );
                return Err(StatusCodeEnum::CryptoServerNotReady);
            }
        }

        let mut hashes = Vec::new();
        {
            let auth = self.auth.read().await;
            let mut pool = self.pool.write().await;
            auth.check_transactions(&raw_txs)?;
            for raw_tx in raw_txs.body.clone() {
                let hash = get_tx_hash(&raw_tx)?.to_vec();
                if pool.insert(raw_tx) {
                    hashes.push(Hash { hash })
                }
            }
        }
        if broadcast {
            self.broadcast_send_txs(raw_txs).await;
        }
        Ok(Hashes { hashes })
    }

    pub async fn rpc_get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, StatusCodeEnum> {
        load_data(
            storage_client(),
            i32::from(Regions::BlockHash) as u32,
            block_number.to_be_bytes().to_vec(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block({}) hash failed: {}",
                block_number,
                e.to_string()
            );
            StatusCodeEnum::NoBlockHeight
        })
    }

    pub async fn rpc_get_tx_block_number(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.0)
    }

    pub async fn rpc_get_tx_index(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.1)
    }

    pub async fn rpc_get_height_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<BlockNumber, StatusCodeEnum> {
        get_height_by_block_hash(hash).await
    }

    pub async fn rpc_get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        get_compact_block(block_number).await
    }

    pub async fn rpc_get_block_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        let block_number = load_data(
            storage_client(),
            i32::from(Regions::BlockHash2blockHeight) as u32,
            hash.clone(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block height failed: {}. hash: 0x{}",
                e.to_string(),
                hex::encode(&hash),
            );
            StatusCodeEnum::NoBlockHeight
        })
        .map(u64_decode)?;
        self.rpc_get_block_by_number(block_number).await
    }

    pub async fn rpc_get_state_root_by_number(
        &self,
        block_number: u64,
    ) -> Result<StateRoot, StatusCodeEnum> {
        get_state_root(block_number).await
    }

    pub async fn rpc_get_proof_by_number(
        &self,
        block_number: u64,
    ) -> Result<Proof, StatusCodeEnum> {
        get_proof(block_number).await
    }

    pub async fn rpc_get_block_detail_by_number(
        &self,
        block_number: u64,
    ) -> Result<Block, StatusCodeEnum> {
        get_full_block(block_number).await
    }

    pub async fn rpc_get_transaction(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<RawTransaction, StatusCodeEnum> {
        match db_get_tx(&tx_hash).await {
            Ok(tx) => Ok(tx),
            Err(e) => {
                let pool = self.pool.read().await;
                match pool.pool_get_tx(&tx_hash) {
                    Some(tx) => Ok(tx),
                    None => Err(e),
                }
            }
        }
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, StatusCodeEnum> {
        let auth = self.auth.read().await;
        let sys_config = auth.get_system_config();
        Ok(sys_config)
    }

    pub async fn rpc_add_node(&self, info: NodeNetInfo) -> cita_cloud_proto::common::StatusCode {
        let res = network_client().add_node(info).await.unwrap_or_else(|e| {
            warn!("rpc add node failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady.into()
        });

        let controller_for_add = self.clone();
        let code = StatusCodeEnum::from(res.clone());
        if code == StatusCodeEnum::Success || code == StatusCodeEnum::AddExistedPeer {
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

    pub async fn rpc_get_node_status(&self, _: Empty) -> Result<NodeStatus, StatusCodeEnum> {
        let peers_count = get_network_status().await?.peer_count;
        let peers_netinfo = get_peers_info().await?;
        let mut peers_status = vec![];
        for p in peers_netinfo.nodes {
            let na = NodeAddress(p.origin);
            let (address, height) = self
                .node_manager
                .nodes
                .read()
                .await
                .get(&na)
                .map_or((vec![], 0), |c| {
                    (c.address.clone().unwrap().address, c.height)
                });
            peers_status.push(PeerStatus {
                height,
                address,
                node_net_info: Some(p),
            });
        }
        let chain_status = self.get_status().await;
        let self_status = Some(PeerStatus {
            height: chain_status.height,
            address: chain_status.address.unwrap().address,
            node_net_info: None,
        });

        let node_status = NodeStatus {
            is_sync: self.get_sync_state().await,
            version: env!("CARGO_PKG_VERSION").to_string(),
            self_status,
            peers_count,
            peers_status,
            is_danger: self.config.is_danger,
            init_block_number: self.init_block_number,
        };
        Ok(node_status)
    }

    pub async fn chain_get_proposal(&self) -> Result<(u64, Vec<u8>), StatusCodeEnum> {
        if self.get_sync_state().await {
            return Err(StatusCodeEnum::NodeInSyncMode);
        }

        let mut chain = self.chain.write().await;
        chain
            .add_proposal(
                &self.get_global_status().await.1,
                self.validator_address.address.clone(),
            )
            .await?;
        chain.get_proposal().await
    }

    pub async fn chain_check_proposal(
        &self,
        proposal_height: u64,
        data: &[u8],
    ) -> Result<(), StatusCodeEnum> {
        let proposal_enum = ProposalEnum::decode(data).map_err(|_| {
            warn!("check proposal failed: decode ProposalEnum failed");
            StatusCodeEnum::DecodeError
        })?;
        let Proposal::BftProposal(bft_proposal) =
            proposal_enum.proposal.ok_or(StatusCodeEnum::NoneProposal)?;
        let block = &bft_proposal.proposal.ok_or(StatusCodeEnum::NoneProposal)?;
        let header = block
            .header
            .as_ref()
            .ok_or(StatusCodeEnum::NoneBlockHeader)?;
        let block_hash = get_block_hash(crypto_client(), block.header.as_ref()).await?;
        let block_height = header.height;

        //check height is consistent
        if block_height != proposal_height {
            warn!(
                "check proposal({}) failed: proposal_height: {}, block_height: {}",
                proposal_height, proposal_height, block_height,
            );
            return Err(StatusCodeEnum::ProposalCheckError);
        }

        let ret = {
            let chain = self.chain.read().await;
            //if proposal is own, skip check_proposal
            if chain.is_own(data) && chain.is_candidate(&block_hash) {
                info!(
                    "check own proposal({}): skip check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
                return Ok(());
            } else {
                info!(
                    "check remote proposal({}): start check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
            }
            //check height
            chain.check_proposal(block_height).await
        };

        match ret {
            Ok(_) => {
                let sys_config = self.rpc_get_system_config().await?;
                let pre_height_bytes = (block_height - 1).to_be_bytes().to_vec();

                //check pre_state_root in proposal
                let pre_state_root = load_data(
                    storage_client(),
                    i32::from(Regions::Result) as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                if bft_proposal.pre_state_root != pre_state_root {
                    warn!(
                            "check proposal({}) failed: pre_state_root: 0x{}, local pre_state_root: 0x{}",
                            block_height,
                            hex::encode(&bft_proposal.pre_state_root),
                            hex::encode(&pre_state_root),
                        );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check proposer in block header
                let proposer = header.proposer.as_slice();
                if sys_config.validators.iter().all(|v| &v[..20] != proposer) {
                    warn!(
                        "check proposal({}) failed: proposer: {} not in validators {:?}",
                        block_height,
                        hex::encode(proposer),
                        sys_config
                            .validators
                            .iter()
                            .map(hex::encode)
                            .collect::<Vec<String>>(),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }
                //check timestamp in block header
                let pre_compact_block_bytes = load_data(
                    storage_client(),
                    i32::from(Regions::CompactBlock) as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                let pre_header = CompactBlock::decode(pre_compact_block_bytes.as_slice())
                    .map_err(|_| {
                        warn!(
                            "check proposal({}) failed: decode CompactBlock failed",
                            block_height
                        );
                        StatusCodeEnum::DecodeError
                    })?
                    .header
                    .ok_or(StatusCodeEnum::NoneBlockHeader)?;
                let timestamp = header.timestamp;
                let left_bounds = pre_header.timestamp;
                let right_bounds = unix_now() + sys_config.block_interval as u64 * 1000;
                if timestamp < left_bounds || timestamp > right_bounds {
                    warn!(
                        "check proposal({}) failed: timestamp: {} must be in range of {} - {}",
                        block_height, timestamp, left_bounds, right_bounds,
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check quota and transaction_root
                let mut total_quota = 0;
                let mut transantion_data = Vec::new();
                let body = &block
                    .body
                    .as_ref()
                    .ok_or(StatusCodeEnum::NoneBlockBody)?
                    .body;
                let tx_count = body.len();
                for tx in body {
                    total_quota += get_tx_quota(tx)?;
                    if total_quota > sys_config.quota_limit {
                        return Err(StatusCodeEnum::QuotaUsedExceed);
                    }

                    transantion_data.extend_from_slice(get_tx_hash(tx)?);
                }
                let transactions_root = hash_data(crypto_client(), &transantion_data).await?;
                if transactions_root != header.transactions_root {
                    warn!(
                        "check proposal({}) failed: header transactions_root: {}, controller calculate: {}",
                        block_height, hex::encode(&header.transactions_root), hex::encode(&transactions_root),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check transactions in block body
                self.batch_transactions(
                    block.body.to_owned().ok_or(StatusCodeEnum::NoneBlockBody)?,
                    false,
                )
                .await?;

                // add remote proposal
                {
                    let mut chain = self.chain.write().await;
                    chain.add_remote_proposal(&block_hash).await;
                }
                info!(
                    "check proposal({}) success: tx count: {}, quota: {}, block_hash: 0x{}",
                    block_height,
                    tx_count,
                    total_quota,
                    hex::encode(&block_hash)
                );
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
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

    #[instrument(skip_all)]
    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<ConsensusConfiguration, StatusCodeEnum> {
        let status = self.get_status().await;

        if status.height >= height {
            let config = self.rpc_get_system_config().await?;
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
                self.broadcast_chain_status(status).await;
                self.try_sync_block().await;
                Ok(config)
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate();
                }
                self.try_sync_block().await;
                Err(StatusCodeEnum::ProposalTooHigh)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<(), StatusCodeEnum> {
        debug!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusInitType => {
                let chain_status_init =
                    ChainStatusInit::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusInitType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let own_status = self.get_status().await;
                match chain_status_init.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
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
                                    .ok_or(StatusCodeEnum::NoneChainStatus)?
                                    .address
                                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                                self.delete_global_status(&NodeAddress::from(&node)).await;
                                self.node_manager
                                    .set_ban_node(&NodeAddress::from(&node))
                                    .await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let status = chain_status_init
                    .chain_status
                    .ok_or(StatusCodeEnum::NoneChainStatus)?;
                let node = status
                    .address
                    .clone()
                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                let node_orign = NodeAddress::from(&node);

                match self
                    .node_manager
                    .set_node(&node_orign, status.clone())
                    .await
                {
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
                        if status_code == StatusCodeEnum::EarlyStatus
                            && own_status.height < status.height
                        {
                            {
                                let mut chain = self.chain.write().await;
                                chain.clear_candidate();
                            }
                            let chain_status_init = self.make_csi(own_status).await?;
                            self.unicast_chain_status_init(msg.origin, chain_status_init)
                                .await;
                            self.try_update_global_status(&node_orign, status).await?;
                        }
                        return Err(status_code);
                    }
                }
                self.try_update_global_status(&node_orign, status).await?;
            }
            ControllerMsgType::ChainStatusInitRequestType => {
                let chain_status_init = self.make_csi(self.get_status().await).await?;

                self.unicast_chain_status_init(msg.origin, chain_status_init)
                    .await;
            }
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process ChainStatusType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                let own_status = self.get_status().await;
                let node = chain_status.address.clone().unwrap();
                let node_orign = NodeAddress::from(&node);
                match chain_status.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                self.delete_global_status(&node_orign).await;
                                self.node_manager.set_ban_node(&node_orign).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                match self
                    .node_manager
                    .check_address_origin(&node_orign, NodeAddress(msg.origin))
                    .await
                {
                    Ok(true) => {
                        self.node_manager
                            .set_node(&node_orign, chain_status.clone())
                            .await?;
                        self.try_update_global_status(&node_orign, chain_status)
                            .await?;
                    }
                    // give Ok or Err for process_network_msg is same
                    Err(StatusCodeEnum::AddressOriginCheckError) | Ok(false) => {
                        self.unicast_chain_status_init_req(msg.origin, own_status)
                            .await;
                    }
                    Err(e) => return Err(e),
                }
            }

            ControllerMsgType::ChainStatusRespondType => {
                let chain_status_respond =
                    ChainStatusRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                if let Some(respond) = chain_status_respond.respond {
                    match respond {
                        Respond::NotSameChain(node) => {
                            h160_address_check(Some(&node))?;
                            let node_orign = NodeAddress::from(&node);
                            warn!(
                                "process ChainStatusRespondType failed: remote check chain_status failed: NotSameChain. ban remote node. origin: {}", node_orign
                            );
                            self.delete_global_status(&node_orign).await;
                            self.node_manager.set_ban_node(&node_orign).await?;
                        }
                    }
                }
            }

            ControllerMsgType::SyncBlockType => {
                let sync_block_request =
                    SyncBlockRequest::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockType failed: decode failed",);
                        StatusCodeEnum::DecodeError
                    })?;

                info!(
                    "get SyncBlockRequest: from origin: {:x}, height: {} - {}",
                    msg.origin, sync_block_request.start_height, sync_block_request.end_height
                );
                self.task_sender
                    .send(EventTask::SyncBlockReq(sync_block_request, msg.origin))
                    .await
                    .unwrap();
            }

            ControllerMsgType::SyncBlockRespondType => {
                let sync_block_respond =
                    SyncBlockRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_block_respond::Respond;

                tokio::spawn(async move {
                    match sync_block_respond.respond {
                        // todo check origin
                        Some(Respond::MissBlock(node)) => {
                            let node_orign = NodeAddress::from(&node);
                            controller_clone.delete_global_status(&node_orign).await;
                            controller_clone
                                .node_manager
                                .set_misbehavior_node(&node_orign)
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
                                Err(StatusCodeEnum::ProvideAddressError)
                                | Err(StatusCodeEnum::NoProvideAddress) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: message address error. origin: {:x}",
                                        msg.origin
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: {}. origin: {:x}",
                                        e.to_string(),
                                        msg.origin
                                    );
                                    let node = sync_blocks.address.as_ref().unwrap();
                                    let node_orign = NodeAddress::from(node);
                                    controller_clone
                                        .node_manager
                                        .set_misbehavior_node(&node_orign)
                                        .await
                                        .unwrap();
                                    controller_clone.delete_global_status(&node_orign).await;
                                }
                            }
                        }
                        None => {}
                    }
                });
            }

            ControllerMsgType::SyncTxType => {
                let sync_tx = SyncTxRequest::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SyncTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
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
                    warn!("process SyncTxRespondType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                match sync_tx_respond.respond {
                    Some(Respond::MissTx(node)) => {
                        let node_orign = NodeAddress::from(&node);
                        self.node_manager.set_misbehavior_node(&node_orign).await?;
                        self.delete_global_status(&node_orign).await;
                    }
                    Some(Respond::Ok(raw_tx)) => {
                        self.rpc_send_raw_transaction(raw_tx, false).await?;
                    }
                    None => {}
                }
            }

            ControllerMsgType::SendTxType => {
                let send_tx = RawTransaction::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.rpc_send_raw_transaction(send_tx, false).await?;
            }

            ControllerMsgType::SendTxsType => {
                let body = RawTransactions::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxsType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.batch_transactions(body, false).await?;
            }

            ControllerMsgType::Noop => {
                warn!("process Noop failed: unexpected");
                self.delete_global_status(&NodeAddress(msg.origin)).await;
                self.node_manager
                    .set_ban_node(&NodeAddress(msg.origin))
                    .await?;
            }
        }

        Ok(())
    }

    impl_broadcast!(
        broadcast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_multicast!(broadcast_chain_status, ChainStatus, "chain_status");

    // impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    // impl_multicast!(multicast_send_tx, RawTransaction, "send_tx");
    // impl_multicast!(multicast_send_txs, RawTransactions, "send_txs");
    impl_broadcast!(broadcast_send_txs, RawTransactions, "send_txs");
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

    pub async fn get_global_status(&self) -> (NodeAddress, ChainStatus) {
        let rd = self.global_status.read().await;
        rd.clone()
    }

    pub async fn update_global_status(&self, node: NodeAddress, status: ChainStatus) {
        let mut wr = self.global_status.write().await;
        *wr = (node, status);
    }

    async fn delete_global_status(&self, node: &NodeAddress) -> bool {
        let res = {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            &gs.0 == node
        };
        if res {
            let mut wr = self.global_status.write().await;
            *wr = (NodeAddress(0), ChainStatus::default());
            true
        } else {
            false
        }
    }

    async fn try_update_global_status(
        &self,
        node: &NodeAddress,
        status: ChainStatus,
    ) -> Result<bool, StatusCodeEnum> {
        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        if status.height > old_status.1.height && status.height >= own_status.height {
            info!(
                "update global status: origin: {}, height: {}, hash: 0x{}",
                node,
                status.height,
                hex::encode(status.hash.clone().unwrap().hash)
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
    ) -> Result<ChainStatus, StatusCodeEnum> {
        let compact_block = get_compact_block(height).await?;

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash {
                hash: get_block_hash(crypto_client(), compact_block.header.as_ref()).await?,
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

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, StatusCodeEnum> {
        h160_address_check(sync_blocks.address.as_ref())?;

        let own_height = self.get_status().await.height;
        Ok(self
            .sync_manager
            .insert_blocks(
                sync_blocks
                    .address
                    .ok_or(StatusCodeEnum::NoProvideAddress)?,
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
                            if !controller_clone.get_sync_state().await {
                                controller_clone.set_sync_state(true).await;
                            }
                            current_height = sync_req.end_height;
                            controller_clone
                                .unicast_sync_block(global_address.0, sync_req.clone())
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

    pub async fn sync_block(&self) -> Result<(), StatusCodeEnum> {
        let mut current_height = self.get_status().await.height;
        for _ in 0..self.config.sync_req {
            let (global_address, global_status) = self.get_global_status().await;

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
                        if !self.get_sync_state().await {
                            self.set_sync_state(true).await;
                        }
                        current_height = sync_req.end_height;
                        self.unicast_sync_block(global_address.0, sync_req.clone())
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

    pub async fn make_csi(
        &self,
        own_status: ChainStatus,
    ) -> Result<ChainStatusInit, StatusCodeEnum> {
        let mut chain_status_bytes = Vec::new();
        own_status.encode(&mut chain_status_bytes).map_err(|_| {
            warn!("make csi failed: encode ChainStatus failed");
            StatusCodeEnum::EncodeError
        })?;
        let msg_hash = hash_data(crypto_client(), &chain_status_bytes).await?;
        let signature = sign_message(crypto_client(), &msg_hash).await?;

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
