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
    chain_status_respond::Respond, ChainStatus, ChainStatusInit, ChainStatusRespond, NodeManager,
};
use crate::pool::Pool;
use crate::protocol::sync_manager::{
    SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager, SyncTxRequest, SyncTxRespond,
};
use crate::util::*;
use crate::utxo_set::SystemConfig;
use crate::{impl_broadcast, impl_multicast, impl_unicast};
use crate::{GenesisBlock, DEFAULT_PACKAGE_LIMIT};
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    common::{
        proposal_enum::Proposal, Address, ConsensusConfiguration, Hash, ProposalEnum,
        SimpleResponse,
    },
    network::NetworkMsg,
};
use log::warn;
use prost::Message;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

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
    SendProposalType,
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
            ControllerMsgType::ChainStatusInitType => "chain_status_init",
            ControllerMsgType::ChainStatusInitRequestType => "chain_status_init_req",
            ControllerMsgType::ChainStatusType => "chain_status",
            ControllerMsgType::ChainStatusRespondType => "chain_status_respond",
            ControllerMsgType::SyncBlockType => "sync_block",
            ControllerMsgType::SyncBlockRespondType => "sync_block_respond",
            ControllerMsgType::SyncTxType => "sync_tx",
            ControllerMsgType::SyncTxRespondType => "sync_tx_respond",
            ControllerMsgType::SendTxType => "send_tx",
            ControllerMsgType::SendProposalType => "send_proposal",
            ControllerMsgType::Noop => "noop",
        }
    }
}

#[derive(Clone)]
pub struct Controller {
    pub(crate) network_port: u16,
    storage_port: u16,

    auth: Arc<RwLock<Authentication>>,

    pool: Arc<RwLock<Pool>>,

    pub(crate) chain: Arc<RwLock<Chain>>,

    consensus_port: u16,

    kms_port: u16,

    pub(crate) local_address: Address,

    current_status: Arc<RwLock<ChainStatus>>,

    global_status: Arc<RwLock<(Address, ChainStatus)>>,

    pub(crate) node_manager: NodeManager,

    pub(crate) sync_manager: SyncManager,

    task_sender: mpsc::UnboundedSender<EventTask>,
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
        key_id: u64,
        node_address: Vec<u8>,
        task_sender: mpsc::UnboundedSender<EventTask>,
    ) -> Self {
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

        let auth = Arc::new(RwLock::new(Authentication::new(
            kms_port,
            storage_port,
            sys_config,
        )));
        let pool = Arc::new(RwLock::new(Pool::new(DEFAULT_PACKAGE_LIMIT)));
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
        )));

        Controller {
            network_port,
            storage_port,
            auth,
            pool,
            chain,
            consensus_port,
            kms_port,
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
            node_manager: NodeManager::default(),
            sync_manager: SyncManager::default(),
            task_sender,
        }
    }

    pub async fn init(&self, init_block_number: u64, sys_config: SystemConfig) {
        {
            let chain = self.chain.write().await;
            chain.init(init_block_number).await;
            chain.init_auth(init_block_number).await;
        }
        let status = self
            .init_status(init_block_number, sys_config)
            .await
            .unwrap();
        self.set_status(status.clone()).await;
        // todo
        self.broadcast_chain_status_init(
            self.network_port,
            ChainStatusInit {
                chain_status: Some(status),
                signature: vec![],
                public_key: vec![],
            },
        )
        .await
        .await
        .unwrap();
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

        {
            let chain = self.chain.read().await;
            if chain.check_dup_tx(&tx_hash) {
                return Err(format!(
                    "Dup transaction in chain, hash: {}",
                    hex::encode(&tx_hash)
                ));
            }
        }

        if {
            let mut pool = self.pool.write().await;
            pool.enqueue(tx_hash.clone(), raw_tx.clone())
        } {
            if broadcast {
                self.multicast_send_tx(self.network_port, raw_tx).await;
            }
            Ok(tx_hash)
        } else {
            Err(format!(
                "Dup transaction in pool, hash: {}",
                hex::encode(tx_hash)
            ))
        }
    }

    pub async fn batch_transactions(&self, raw_txs: RawTransactions) -> Result<(), Error> {
        {
            let rd = self.chain.read().await;
            // todo not clone
            rd.check_transactions(raw_txs.clone()).await?;
        }

        let mut pool = self.pool.write().await;
        for raw_tx in raw_txs.body {
            pool.enqueue(get_tx_hash(&raw_tx)?, raw_tx);
        }
        Ok(())
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
        let ret = db_get_tx(&tx_hash).await;
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
        let mut chain = self.chain.write().await;
        chain
            .add_proposal(&self.get_global_status().await.1)
            .await?;
        chain.get_proposal().await
    }

    pub async fn chain_check_proposal(&self, height: u64, data: &[u8]) -> Result<bool, Error> {
        let proposal_enum = ProposalEnum::decode(data)
            .map_err(|_| Error::DecodeError(format!("decode ProposalEnum failed")))?;

        let ret = {
            let chain = self.chain.read().await;
            chain.check_proposal(height, proposal_enum.clone()).await
        };

        match ret {
            Ok(true) => match proposal_enum.proposal {
                Some(Proposal::BftProposal(bft_proposal)) => {
                    let block = bft_proposal.proposal.ok_or(Error::NoneProposal)?;

                    let block_hash = get_block_hash(block.header.as_ref())?;

                    // todo re-enter check
                    if {
                        log::info!(
                            "add remote proposal(0x{}) through check_proposal",
                            hex::encode(&block_hash)
                        );
                        let mut chain = self.chain.write().await;
                        chain
                            .add_remote_proposal(&block_hash, block.clone())
                            .await?
                            && !chain.is_candidate(&block_hash)
                    } {
                        let _ = self
                            .batch_transactions(block.body.ok_or(Error::NoneBlockBody)?)
                            .await;
                    }
                }
                None => return Err(Error::NoneProposal),
            },
            Err(Error::ProposalTooHigh(p, c)) => {
                warn!("Proposal(h: {}) is higher than current(h: {})", p, c);
                self.multicast_chain_status(self.network_port, self.get_status().await)
                    .await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate().await;
                }
                self.try_sync_block().await;
                self.task_sender.send(EventTask::SyncBlock).unwrap();
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
    ) -> Result<ConsensusConfiguration, Error> {
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

        let mut chain = self.chain.write().await;
        match chain.commit_block(height, proposal, proof).await {
            Ok((config, mut status)) => {
                status.address = Some(self.local_address.clone());
                self.set_status(status.clone()).await;
                self.multicast_chain_status(self.network_port, status).await;
                let (_, global_status) = self.get_global_status().await;
                match chain.next_step(&global_status).await {
                    ChainStep::SyncStep => {
                        self.try_sync_block().await;
                    }
                    ChainStep::OnlineStep => {}
                }
                Ok(config)
            }
            Err(Error::ProposalTooHigh(p, c)) => {
                warn!("Proposal(h: {}) is higher than current(h: {})", p, c);
                self.multicast_chain_status(self.network_port, self.get_status().await)
                    .await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate().await;
                }
                self.try_sync_block().await;
                self.task_sender.send(EventTask::SyncBlock).unwrap();
                return Err(Error::ProposalTooHigh(p, c));
            }
            Err(e) => Err(e),
        }
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<SimpleResponse, Error> {
        log::debug!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusInitType => {
                let chain_status_init =
                    ChainStatusInit::decode(msg.msg.as_slice()).map_err(|_| {
                        Error::DecodeError(format!(
                            "decode {} msg failed",
                            ControllerMsgType::ChainStatusInitType
                        ))
                    })?;

                let own_status = self.get_status().await;
                match chain_status_init.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            Error::VersionOrIdCheckError | Error::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    self.network_port,
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
                                    .unwrap()
                                    .address
                                    .unwrap();
                                self.delete_global_status(&node).await;
                                self.node_manager.set_ban_node(&node).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let status = chain_status_init.chain_status.unwrap();
                let node = status.address.clone().unwrap();
                self.node_manager.set_origin(&node, msg.origin).await;
                if self
                    .node_manager
                    .set_node(&node, status.clone())
                    .await?
                    .is_none()
                {
                    // todo sig
                    self.unicast_chain_status_init(
                        self.network_port,
                        msg.origin,
                        ChainStatusInit {
                            chain_status: Some(own_status),
                            signature: vec![],
                            public_key: vec![],
                        },
                    )
                    .await;
                } else {
                    self.unicast_chain_status(self.network_port, msg.origin, own_status)
                        .await;
                }
                self.try_update_global_status(&node, status).await?;
            }
            ControllerMsgType::ChainStatusInitRequestType => {
                self.unicast_chain_status_init(
                    self.network_port,
                    msg.origin,
                    ChainStatusInit {
                        chain_status: Some(self.get_status().await),
                        signature: vec![],
                        public_key: vec![],
                    },
                )
                .await;
            }
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::ChainStatusType
                    ))
                })?;

                let own_status = self.get_status().await;
                match chain_status.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            Error::VersionOrIdCheckError | Error::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    self.network_port,
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
                    Err(Error::AddressOriginCheckError) | Ok(false) => {
                        self.unicast_chain_status_init_req(
                            self.network_port,
                            msg.origin,
                            own_status,
                        )
                        .await;
                    }
                    Err(e) => return Err(e),
                }
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
                            h160_address_check(Some(&node))?;
                            self.delete_global_status(&node).await;
                            self.node_manager.set_ban_node(&node).await?;
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

                use crate::protocol::sync_manager::sync_block_respond::Respond;
                tokio::spawn(async move {
                    let mut block_vec = Vec::new();

                    for h in sync_block_request.start_height..=sync_block_request.end_height {
                        if let Some((compact_block, proof)) = get_compact_block(h).await {
                            let full_block = get_full_block(compact_block, proof).await.unwrap();
                            block_vec.push(full_block);
                        } else {
                            let sync_block_respond = SyncBlockRespond {
                                respond: Some(Respond::MissBlock(controller.local_address.clone())),
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
                        respond: Some(Respond::Ok(sync_block)),
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

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_block_respond::Respond;

                tokio::spawn(async move {
                    match sync_block_respond.respond {
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
                                    controller_clone
                                        .task_sender
                                        .send(EventTask::SyncBlock)
                                        .unwrap();
                                }
                                Err(Error::ProvideAddressError) | Err(Error::NoProvideAddress) => {
                                    warn!(
                                        "sync_block_respond error, origin: {}, message: given address error",
                                        msg.origin,
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "sync_block_respond error, origin: {}, message: {}",
                                        msg.origin,
                                        e.to_string()
                                    );

                                    controller_clone
                                        .node_manager
                                        .set_misbehavior_node(
                                            &sync_blocks.address.as_ref().unwrap(),
                                        )
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
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SyncTxType
                    ))
                })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                tokio::spawn(async move {
                    if let Some(raw_tx) = {
                        let rd = controller_clone.chain.read().await;
                        rd.chain_get_tx(&sync_tx.tx_hash).await
                    } {
                        controller_clone
                            .unicast_sync_tx_respond(
                                controller_clone.network_port,
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
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SyncTxRespondType
                    ))
                })?;

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                match sync_tx_respond.respond {
                    Some(Respond::MissTx(node)) => {
                        self.node_manager.set_misbehavior_node(&node).await?;
                        self.delete_global_status(&node).await;
                    }
                    Some(Respond::Ok(raw_tx)) => {
                        self.rpc_send_raw_transaction(raw_tx, false)
                            .await
                            .map_err(Error::ExpectError)?;
                    }
                    None => {}
                }
            }

            ControllerMsgType::SendTxType => {
                let send_tx = RawTransaction::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SendTxType
                    ))
                })?;

                self.batch_transactions(RawTransactions {
                    body: vec![send_tx],
                })
                .await?;
            }

            ControllerMsgType::SendProposalType => {
                let full_block = Block::decode(msg.msg.as_slice()).map_err(|_| {
                    Error::DecodeError(format!(
                        "decode {} msg failed",
                        ControllerMsgType::SendProposalType
                    ))
                })?;

                let controller_clone = self.clone();
                tokio::spawn(async move {
                    let block_hash = get_block_hash(full_block.header.as_ref()).unwrap();

                    if let Some(body) = full_block.body.clone() {
                        let _ = controller_clone.batch_transactions(body).await;
                    }
                    {
                        let mut wr = controller_clone.chain.write().await;
                        if !wr
                            .add_remote_proposal(&block_hash, full_block)
                            .await
                            .unwrap()
                        {
                            warn!("add remote proposal: 0x{} failed", hex::encode(&block_hash))
                        }
                    }
                });
            }

            ControllerMsgType::Noop => match self.node_manager.get_address(msg.origin).await {
                Some(address) => {
                    self.delete_global_status(&address).await;
                    self.node_manager.set_ban_node(&address).await?;
                }
                None => {}
            },
        }

        Ok(SimpleResponse { is_success: true })
    }

    impl_broadcast!(
        broadcast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );

    // impl_multicast!(multicast_send_proposal, Block, "send_proposal");
    impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    impl_multicast!(multicast_send_tx, RawTransaction, "send_tx");
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
        if {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            &gs.0 == node
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
        node: &Address,
        status: ChainStatus,
    ) -> Result<bool, Error> {
        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        if status.height > old_status.1.height && status.height >= own_status.height {
            self.update_global_status(node.to_owned(), status).await;
            self.try_sync_block().await;
            if self
                .sync_manager
                .contains_block(own_status.height + 1)
                .await
            {
                self.task_sender.send(EventTask::SyncBlock).unwrap();
            }

            return Ok(true);
        }

        Ok(false)
    }

    async fn init_status(&self, height: u64, config: SystemConfig) -> Result<ChainStatus, Error> {
        let full_block = {
            if height == 0 {
                let rd = self.chain.read().await;
                full_to_compact(rd.get_genesis_block())
            } else {
                get_compact_block(height)
                    .await
                    .ok_or(Error::NoBlock(height))?
                    .0
            }
        };
        let mut header_bytes = Vec::new();
        full_block
            .header
            .unwrap()
            .encode(&mut header_bytes)
            .map_err(|_| Error::EncodeError(format!("encode compact block error")))?;
        let block_hash = hash_data(&header_bytes);

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash { hash: block_hash }),
            address: Some(self.local_address.clone()),
        })
    }

    pub async fn get_status(&self) -> ChainStatus {
        let rd = self.current_status.read().await;
        rd.clone()
    }

    pub async fn set_status(&self, status: ChainStatus) {
        let mut wr = self.current_status.write().await;
        *wr = status;
    }

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, Error> {
        h160_address_check(sync_blocks.address.as_ref())?;

        Ok(self
            .sync_manager
            .insert_blocks(sync_blocks.address.unwrap(), sync_blocks.sync_blocks)
            .await)
    }

    pub async fn try_sync_block(&self) {
        let current_height = self.get_status().await.height;

        let (global_address, global_status) = self.get_global_status().await;

        if global_address.address.is_empty() {
            return;
        }

        let origin = self.node_manager.get_origin(&global_address).await.unwrap();

        match {
            let chain = self.chain.read().await;
            chain.next_step(&global_status).await
        } {
            ChainStep::SyncStep => {
                if let Some(sync_req) = self
                    .sync_manager
                    .get_sync_block_req(current_height, &global_status)
                    .await
                {
                    self.unicast_sync_block(self.network_port, origin, sync_req)
                        .await;
                }
            }
            _ => {}
        }
    }
}
