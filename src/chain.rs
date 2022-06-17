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

use crate::constant::RESULT;
use crate::utxo_set::{LOCK_ID_BLOCK_LIMIT, LOCK_ID_QUOTA_LIMIT};
use crate::{
    auth::Authentication, node_manager::ChainStatus, pool::Pool, util::*, utxo_set::SystemConfig,
    GenesisBlock,
};
use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx, Block, BlockHeader, RawTransaction, RawTransactions},
    common::{proposal_enum::Proposal, BftProposal, ConsensusConfiguration, Hash, ProposalEnum},
    storage::Regions,
};
use cloud_util::{
    common::get_tx_hash,
    crypto::{get_block_hash, hash_data},
    storage::{load_data, store_data},
    unix_now,
    wal::{LogType, Wal},
};
use prost::Message;
use status_code::StatusCode;
use std::path::Path;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time};

#[derive(PartialEq)]
pub enum ChainStep {
    SyncStep,
    OnlineStep,
    BusyState,
}

#[allow(dead_code)]
pub struct Chain {
    block_number: u64,

    block_hash: Vec<u8>,
    // key of hashmap is block_hash
    candidates: HashMap<Vec<u8>, Block>,

    own_proposal: Option<(u64, Vec<u8>, Block)>,

    pool: Arc<RwLock<Pool>>,

    auth: Arc<RwLock<Authentication>>,

    genesis: GenesisBlock,

    wal_log: Arc<RwLock<Wal>>,
}

impl Chain {
    pub fn new(
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        pool: Arc<RwLock<Pool>>,
        auth: Arc<RwLock<Authentication>>,
        genesis: GenesisBlock,
        wal_path: &str,
    ) -> Self {
        // wal_path must be relative path
        assert!(
            !Path::new(wal_path).is_absolute(),
            "wal_path must be relative path"
        );
        Chain {
            block_number: current_block_number,
            block_hash: current_block_hash,
            candidates: HashMap::new(),
            own_proposal: None,
            pool,
            auth,
            genesis,
            wal_log: Arc::new(RwLock::new(Wal::create(wal_path).unwrap())),
        }
    }

    pub async fn init(&self, init_block_number: u64, server_retry_interval: u64) {
        if init_block_number == 0 {
            log::info!("finalize genesis block");
        } else {
            log::info!("confirm executor status");
        }
        let mut interval = time::interval(Duration::from_secs(server_retry_interval));
        loop {
            interval.tick().await;
            match self
                .finalize_block(self.genesis.genesis_block(), self.block_hash.clone(), false)
                .await
            {
                Ok(()) | Err(StatusCode::ReenterBlock) => {
                    log::info!("executor is ready!");
                    break;
                }
                Err(StatusCode::ExecuteServerNotReady) => {
                    log::warn!("executor server not ready! Retrying.")
                }
                Err(e) => {
                    panic!("init executor panic: {:?}", e);
                }
            }
        }
    }

    pub async fn init_auth(&self, init_block_number: u64) {
        let mut auth = self.auth.write().await;
        auth.init(init_block_number).await;
    }

    pub fn get_block_number(&self, _is_pending: bool) -> u64 {
        self.block_number
    }

    pub async fn extract_proposal_info(&self, h: u64) -> Result<(Vec<u8>, Vec<u8>), StatusCode> {
        let pre_h = h - 1;
        let pre_height_bytes = pre_h.to_be_bytes().to_vec();

        let state_root = load_data(storage_client(), RESULT, pre_height_bytes.clone()).await?;

        let proof = get_compact_block(pre_h).await?.1;

        Ok((state_root, proof))
    }

    pub async fn get_proposal(&self) -> Result<(u64, Vec<u8>, StatusCode), StatusCode> {
        if let Some((h, _, block)) = self.own_proposal.clone() {
            // check again avoid bad proposal
            {
                let auth = self.auth.read().await;
                auth.check_transactions(block.body.as_ref().ok_or(StatusCode::NoneBlockBody)?)
                    .map_err(|e| {
                        log::warn!("get_proposal: check_transactions failed: {:?}", e);
                        e
                    })?;
            }

            let status = {
                if block
                    .body
                    .as_ref()
                    .ok_or(StatusCode::NoneBlockBody)?
                    .body
                    .is_empty()
                {
                    StatusCode::NoTransaction
                } else {
                    StatusCode::Success
                }
            };

            Ok((h, self.assemble_proposal(block, h).await?, status))
        } else {
            Err(StatusCode::NoCandidate)
        }
    }

    pub async fn assemble_proposal(
        &self,
        mut block: Block,
        height: u64,
    ) -> Result<Vec<u8>, StatusCode> {
        block.proof = Vec::new();
        let (pre_state_root, pre_proof) = self.extract_proposal_info(height).await?;

        let proposal = ProposalEnum {
            proposal: Some(Proposal::BftProposal(BftProposal {
                proposal: Some(block),
                pre_state_root,
                pre_proof,
            })),
        };

        let mut proposal_bytes = Vec::with_capacity(proposal.encoded_len());
        proposal.encode(&mut proposal_bytes).map_err(|_| {
            log::warn!("encode proposal error");
            StatusCode::EncodeError
        })?;

        Ok(proposal_bytes)
    }

    pub async fn add_remote_proposal(
        &mut self,
        block_hash: &[u8],
        block: Block,
    ) -> Result<bool, StatusCode> {
        let header = block.header.clone().unwrap();
        let block_height = header.height;
        if block_height <= self.block_number {
            log::warn!("add_remote_proposal: ProposalTooLow, self block number: {}, remote block number: {}",
            self.block_number, block_height);
            return Err(StatusCode::ProposalTooLow);
        }

        if block_height > self.block_number + 1 {
            log::warn!("add_remote_proposal: ProposalTooHigh, self block number: {}, remote block number: {}",
                  self.block_number, block_height);
            return Err(StatusCode::ProposalTooHigh);
        }

        if !self.candidates.contains_key(block_hash) {
            self.candidates.insert(block_hash.to_vec(), block);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn is_own(&self, block_hash: &[u8]) -> bool {
        if let Some((_, hash, _)) = &self.own_proposal {
            hash == block_hash
        } else {
            false
        }
    }

    pub async fn add_proposal(
        &mut self,
        global_status: &ChainStatus,
        proposer: Vec<u8>,
    ) -> Result<(), StatusCode> {
        if self.next_step(global_status).await == ChainStep::SyncStep {
            Err(StatusCode::NodeInSyncMode)
        } else {
            let tx_list = {
                let mut pool = self.pool.write().await;
                log::info!("add_proposal: tx poll len {}", pool.len());
                pool.package(self.block_number + 1)
            };

            let mut data = Vec::new();
            for raw_tx in tx_list.iter() {
                data.extend_from_slice(get_tx_hash(raw_tx)?);
            }
            let transactions_root = hash_data(crypto_client(), &data).await?;

            let prevhash = self.block_hash.clone();
            let height = self.block_number + 1;

            let header = BlockHeader {
                prevhash: prevhash.clone(),
                timestamp: unix_now(),
                height,
                transactions_root,
                proposer,
            };

            let full_block = Block {
                version: 0,
                header: Some(header.clone()),
                body: Some(RawTransactions { body: tx_list }),
                proof: vec![],
            };

            let mut block_header_bytes = Vec::new();
            header
                .encode(&mut block_header_bytes)
                .expect("encode block header failed");

            let block_hash = hash_data(crypto_client(), &block_header_bytes).await?;

            self.own_proposal = Some((height, block_hash.clone(), full_block.clone()));
            self.candidates.insert(block_hash.clone(), full_block);

            log::info!(
                "proposal {} block_hash 0x{} prevhash 0x{}",
                height,
                hex::encode(&block_hash),
                hex::encode(&prevhash),
            );

            Ok(())
        }
    }

    pub async fn check_proposal(&self, h: u64, proposal: ProposalEnum) -> Result<(), StatusCode> {
        if h <= self.block_number {
            log::warn!(
                "check_proposal: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number,
                h
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if h > self.block_number + 1 {
            log::warn!(
                "check_proposal: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number,
                h
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        match proposal.proposal {
            Some(Proposal::BftProposal(bft_proposal)) => {
                let pre_h = h - 1;
                let key = pre_h.to_be_bytes().to_vec();

                let state_root = load_data(storage_client(), RESULT, key).await?;

                let proof = get_compact_block(pre_h).await?.1;

                if bft_proposal.pre_state_root == state_root && bft_proposal.pre_proof == proof {
                    Ok(())
                } else {
                    log::warn!("check_proposal failed!\nproposal_state_root {}\nstate_root {}\nproposal_proof {}\nproof {}",
                          hex::encode(&bft_proposal.pre_state_root),
                          hex::encode(&state_root),
                          hex::encode(&bft_proposal.pre_proof),
                          hex::encode(&proof),
                    );
                    Err(StatusCode::ProposalCheckError)
                }
            }
            None => Err(StatusCode::NoneProposal),
        }
    }

    /// ### ReenterBlock needs to be handled and can only be ignored if wal_redo is true
    /// ```
    /// match executed_block_status {
    ///     StatusCode::Success => {}
    ///     StatusCode::ReenterBlock => {
    ///        if !wal_redo {
    ///            return Err(StatusCode::ReenterBlock);
    ///        }
    ///    }
    ///    status_code => panic!("finalize_block: exec_block panic: {:?}", status_code),
    /// }
    /// ```
    async fn finalize_block(
        &self,
        block: Block,
        block_hash: Vec<u8>,
        wal_redo: bool,
    ) -> Result<(), StatusCode> {
        let block_height = block
            .header
            .as_ref()
            .ok_or(StatusCode::NoneBlockHeader)?
            .height;

        let block_height_bytes = block_height.to_be_bytes().to_vec();

        let block_bytes = {
            let mut buf = Vec::with_capacity(block.encoded_len());
            block.encode(&mut buf).map_err(|_| {
                log::warn!("encode Block failed");
                StatusCode::EncodeError
            })?;
            buf
        };

        if block_height > 0 && !wal_redo {
            self.wal_save_message(block_height, LogType::FinalizeBlock, &block_bytes)
                .await?;
        }

        // region 1: tx_hash - tx
        if let Some(raw_txs) = block.body.clone() {
            for raw_tx in raw_txs.body {
                if let Some(Tx::UtxoTx(utxo_tx)) = raw_tx.tx.clone() {
                    let res = {
                        let mut auth = self.auth.write().await;
                        auth.update_system_config(&utxo_tx)
                    };
                    if res {
                        // if sys_config changed, store utxo tx hash into global region
                        let lock_id = utxo_tx.transaction.as_ref().unwrap().lock_id;
                        store_data(
                            storage_client(),
                            i32::from(Regions::Global) as u32,
                            lock_id.to_be_bytes().to_vec(),
                            utxo_tx.transaction_hash,
                        )
                        .await
                        .is_success()?;
                        match lock_id {
                            LOCK_ID_BLOCK_LIMIT | LOCK_ID_QUOTA_LIMIT => {
                                let sys_config = self.get_system_config().await;
                                let mut pool = self.pool.write().await;
                                pool.set_block_limit(sys_config.block_limit);
                                pool.set_quota_limit(sys_config.quota_limit);
                            }
                            _ => {}
                        }
                    }
                };
            }
        }

        let tx_hash_list = get_tx_hash_list(block.body.as_ref().ok_or(StatusCode::NoneBlockBody)?)?;

        // exec block
        let (executed_block_status, executed_block_hash) = exec_block(block).await;

        log::info!(
            "exec_block({}): status: {}, executed_block_hash: 0x{}",
            block_height,
            executed_block_status,
            hex::encode(&executed_block_hash)
        );

        match executed_block_status {
            StatusCode::Success => {}
            status_code => {
                if status_code != StatusCode::ReenterBlock || !wal_redo {
                    return Err(status_code);
                }
            }
        }

        // update auth and pool
        {
            let mut auth = self.auth.write().await;
            let mut pool = self.pool.write().await;
            auth.insert_tx_hash(block_height, tx_hash_list.clone());
            pool.update(&tx_hash_list);
        }

        store_data(
            storage_client(),
            i32::from(Regions::FullBlock) as u32,
            block_height_bytes.clone(),
            block_bytes,
        )
        .await
        .is_success()?;

        store_data(
            storage_client(),
            i32::from(Regions::Result) as u32,
            block_height_bytes.clone(),
            executed_block_hash,
        )
        .await
        .is_success()?;

        // region 0: 0 - current height; 1 - current hash
        store_data(
            storage_client(),
            i32::from(Regions::Global) as u32,
            1u64.to_be_bytes().to_vec(),
            block_hash.clone(),
        )
        .await
        .is_success()?;

        store_data(
            storage_client(),
            i32::from(Regions::Global) as u32,
            0u64.to_be_bytes().to_vec(),
            block_height_bytes,
        )
        .await
        .is_success()?;

        self.wal_log
            .write()
            .await
            .clear_file()
            .map_err(|e| {
                panic!("exec_block({}) wal clear_file error: {}", block_height, e);
            })
            .unwrap();

        log::info!(
            "finalize_block: {}, block_hash: 0x{}",
            block_height,
            hex::encode(&block_hash)
        );
        Ok(())
    }

    pub async fn commit_block(
        &mut self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCode> {
        if height <= self.block_number {
            log::warn!(
                "commit_block: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number,
                height
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if height > self.block_number + 1 {
            log::warn!(
                "commit_block: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number,
                height
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        let bft_proposal = match ProposalEnum::decode(proposal)
            .map_err(|_| {
                log::warn!("decode ProposalEnum failed");
                StatusCode::DecodeError
            })?
            .proposal
        {
            Some(Proposal::BftProposal(bft_proposal)) => Ok(bft_proposal),
            None => {
                log::warn!("commit_block: proposal({}) is none", height);
                Err(StatusCode::NoneProposal)
            }
        }?;

        if let Some(mut full_block) = bft_proposal.proposal {
            full_block.proof = proof.to_vec();

            let block_hash = get_block_hash(crypto_client(), full_block.header.as_ref()).await?;

            let prev_hash = full_block.header.clone().unwrap().prevhash;

            if prev_hash != self.block_hash {
                log::warn!(
                    "commit_block: proposal(0x{})'s prev-hash is not equal with chain's block_hash",
                    hex::encode(&block_hash)
                );
                return Err(StatusCode::ProposalCheckError);
            }

            log::info!("height: {} hash 0x{}", height, hex::encode(&block_hash));
            self.finalize_block(full_block, block_hash.clone(), false)
                .await?;

            self.block_number = height;
            self.block_hash = block_hash;

            // candidate_block need update
            self.clear_candidate();

            let config = self.get_system_config().await;

            return Ok((
                ConsensusConfiguration {
                    height,
                    block_interval: config.block_interval,
                    validators: config.validators,
                },
                ChainStatus {
                    version: config.version,
                    chain_id: config.chain_id,
                    height,
                    hash: Some(Hash {
                        hash: self.block_hash.clone(),
                    }),
                    address: None,
                },
            ));
        }

        Err(StatusCode::NoForkTree)
    }

    pub async fn process_block(
        &mut self,
        block: Block,
        wal_redo: bool,
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCode> {
        let block_hash = get_block_hash(crypto_client(), block.header.as_ref()).await?;
        let header = block.header.clone().unwrap();
        let height = header.height;

        if height <= self.block_number {
            log::warn!(
                "process_block: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number,
                height
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if height > self.block_number + 1 {
            log::warn!(
                "process_block: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number,
                height
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        if header.prevhash != self.block_hash {
            log::warn!(
                "prev_hash of block({}) is not equal with self block hash",
                height
            );
            return Err(StatusCode::BlockCheckError);
        }

        let proposal_bytes = self.assemble_proposal(block.clone(), height).await?;

        let status = check_block(height, proposal_bytes, block.proof.clone()).await;
        if status != StatusCode::Success {
            return Err(status);
        }

        {
            let auth = self.auth.read().await;
            auth.check_transactions(block.body.as_ref().ok_or(StatusCode::NoneBlockBody)?)?
        }

        match crypto_client()
            .check_transactions(block.body.clone().ok_or(StatusCode::NoneBlockBody)?)
            .await
        {
            Ok(response) => StatusCode::from(response.into_inner()).is_success()?,
            Err(e) => {
                log::warn!(
                    "check_transactions check block(0x{})'s txs failed: {}",
                    hex::encode(&block_hash),
                    e.to_string()
                );
                return Err(StatusCode::CryptoServerNotReady);
            }
        }

        self.finalize_block(
            block,
            get_block_hash(crypto_client(), Some(&header)).await?,
            wal_redo,
        )
        .await?;

        self.block_number = height;
        self.block_hash = block_hash;

        let config = self.get_system_config().await;

        Ok((
            ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            },
            ChainStatus {
                version: config.version,
                chain_id: config.chain_id,
                height,
                hash: Some(Hash {
                    hash: self.block_hash.clone(),
                }),
                address: None,
            },
        ))
    }

    pub async fn get_system_config(&self) -> SystemConfig {
        let rd = self.auth.read().await;
        rd.get_system_config()
    }

    pub async fn next_step(&self, global_status: &ChainStatus) -> ChainStep {
        if global_status.height > self.block_number && self.candidates.is_empty() {
            log::debug!("in sync mod");
            ChainStep::SyncStep
        } else {
            log::debug!("in online mod");
            ChainStep::OnlineStep
        }
    }

    pub async fn chain_get_tx(&self, tx_hash: &[u8]) -> Result<RawTransaction, StatusCode> {
        if let Some(raw_tx) = {
            let rd = self.pool.read().await;
            rd.pool_get_tx(tx_hash)
        } {
            Ok(raw_tx)
        } else {
            db_get_tx(tx_hash).await
        }
    }

    pub fn clear_candidate(&mut self) {
        self.candidates.clear();
        self.own_proposal = None;
    }

    async fn wal_save_message(
        &self,
        height: u64,
        ltype: LogType,
        msg: &[u8],
    ) -> Result<u64, StatusCode> {
        self.wal_log
            .write()
            .await
            .save(height, ltype, msg)
            .map_err(|e| {
                panic!("wal_save_message: failed: {}", e);
            })
    }

    pub async fn load_wal_log(&mut self) -> Option<(ConsensusConfiguration, ChainStatus)> {
        let vec_buf = self.wal_log.read().await.load();
        if vec_buf.is_empty() {
            return None;
        }
        for (mtype, block_bytes) in vec_buf {
            let log_type: LogType = mtype.into();
            log::info!("load_wal_log chain type {:?}", log_type);
            match log_type {
                LogType::FinalizeBlock => match Block::decode(block_bytes.as_slice()) {
                    Ok(block) => match get_block_hash(crypto_client(), block.header.as_ref()).await
                    {
                        Ok(block_hash) => {
                            let header = block.header.clone().unwrap();
                            let height = header.height;
                            if height == self.block_number && block_hash == self.block_hash {
                                log::info!("wal exists, but doesn't need to be redone");
                            } else {
                                match self.process_block(block, true).await {
                                    Ok(config) => return Some(config),
                                    Err(e) => {
                                        log::warn!("wal process_block error {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("load_wal_log: get_block_hash({:?}) error {}", log_type, e);
                        }
                    },
                    Err(e) => {
                        log::warn!("load_wal_log: decode({:?}) error {}", log_type, e);
                    }
                },
                tp => {
                    panic!("only LogType::FinalizeBlock for controller, get {:?}", tp);
                }
            }
        }
        self.wal_log
            .write()
            .await
            .clear_file()
            .map_err(|e| {
                panic!("wal clear_file error: {}", e);
            })
            .unwrap();
        None
    }
}
