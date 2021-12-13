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
    node_manager::ChainStatus,
    pool::Pool,
    util::*,
    utxo_set::SystemConfig,
    wal::{LogType, StoreData, Wal},
    GenesisBlock,
};
use bincode::deserialize;
use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx, Block, BlockHeader, RawTransaction, RawTransactions},
    common::{proposal_enum::Proposal, BftProposal, ConsensusConfiguration, Hash, ProposalEnum},
};
use cloud_util::{
    common::get_tx_hash,
    crypto::{get_block_hash, hash_data},
    storage::{load_data, store_data},
    unix_now,
};
use log::{info, warn};
use prost::Message;
use status_code::StatusCode;
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

    /// executor service port
    executor_port: u16,
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
        Chain {
            block_number: current_block_number,
            block_hash: current_block_hash,
            candidates: HashMap::new(),
            own_proposal: None,
            pool,
            auth,
            genesis,
            wal_log: Arc::new(RwLock::new(Wal::create(wal_path).unwrap())),
            executor_port: 50002,
        }
    }

    pub async fn init(&self, init_block_number: u64, server_retry_interval: u64) {
        if init_block_number == 0 {
            info!("finalize genesis block");
        } else {
            info!("confirm executor status");
        }
        let mut interval = time::interval(Duration::from_secs(server_retry_interval));
        loop {
            interval.tick().await;
            match self
                .finalize_block(self.genesis.genesis_block(), self.block_hash.clone())
                .await
            {
                Ok(()) | Err(StatusCode::ReenterBlock) => {
                    info!("executor is ready!");
                    break;
                }
                Err(code) => warn!("chain init failed with: {:?}! Retrying.", &code),
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

        let state_root = load_data(storage_client(), 6, pre_height_bytes.clone()).await?;

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
                        warn!("get_proposal: check_transactions failed: {:?}", e);
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
            warn!("encode proposal error");
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
            warn!("add_remote_proposal: ProposalTooLow, self block number: {}, remote block number: {}",
            self.block_number, block_height);
            return Err(StatusCode::ProposalTooLow);
        }

        if block_height > self.block_number + 1 {
            warn!("add_remote_proposal: ProposalTooHigh, self block number: {}, remote block number: {}",
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
                info!("add_proposal: tx poll len {}", pool.len());
                pool.package(self.block_number + 1)
            };

            let mut data = Vec::new();
            for raw_tx in tx_list.iter() {
                data.extend_from_slice(get_tx_hash(raw_tx)?);
            }
            let transactions_root = hash_data(kms_client(), &data).await?;

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

            let block_hash = hash_data(kms_client(), &block_header_bytes).await?;

            self.own_proposal = Some((height, block_hash.clone(), full_block.clone()));
            self.candidates.insert(block_hash.clone(), full_block);

            info!(
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
            warn!(
                "check_proposal: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number, h
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if h > self.block_number + 1 {
            warn!(
                "check_proposal: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number, h
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        match proposal.proposal {
            Some(Proposal::BftProposal(bft_proposal)) => {
                let pre_h = h - 1;
                let key = pre_h.to_be_bytes().to_vec();

                let state_root = load_data(storage_client(), 6, key).await?;

                let proof = get_compact_block(pre_h).await?.1;

                if bft_proposal.pre_state_root == state_root && bft_proposal.pre_proof == proof {
                    Ok(())
                } else {
                    warn!("check_proposal failed!\nproposal_state_root {}\nstate_root {}\nproposal_proof {}\nproof {}",
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

    async fn finalize_block(&self, block: Block, block_hash: Vec<u8>) -> Result<(), StatusCode> {
        let block_height = block
            .header
            .as_ref()
            .ok_or(StatusCode::NoneBlockHeader)?
            .height;

        let block_height_bytes = block_height.to_be_bytes().to_vec();

        let block_bytes = {
            let mut buf = Vec::with_capacity(block.encoded_len());
            block.encode(&mut buf).map_err(|_| {
                warn!("encode Block failed");
                StatusCode::EncodeError
            })?;
            buf
        };

        // region 1: tx_hash - tx
        let mut store_data_sys_config_utxo: Option<StoreData> = None;
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
                        store_data_sys_config_utxo = Some(StoreData {
                            region: 0,
                            key: lock_id.to_be_bytes().to_vec(),
                            value: utxo_tx.transaction_hash.clone(),
                        });
                    }
                };
            }
        }

        let tx_hash_list = get_tx_hash_list(block.body.as_ref().ok_or(StatusCode::NoneBlockBody)?)?;

        let store_data_block = StoreData {
            region: 11,
            key: block_height_bytes.clone(),
            value: block_bytes,
        };

        // region 0: 0 - current height; 1 - current hash
        let store_data_current_height = StoreData {
            region: 0,
            key: 0u64.to_be_bytes().to_vec(),
            value: block_height_bytes.clone(),
        };

        let store_data_current_hash = StoreData {
            region: 0,
            key: 1u64.to_be_bytes().to_vec(),
            value: block_hash.clone(),
        };

        if block_height > 0 {
            let mut wal_data: Vec<StoreData> = Vec::new();

            if let Some(store_data_sys_config_utxo) = store_data_sys_config_utxo.clone() {
                wal_data.push(store_data_sys_config_utxo);
            }
            wal_data.push(store_data_block.clone());
            wal_data.push(store_data_current_height.clone());
            wal_data.push(store_data_current_hash.clone());

            // region 6 : block_height - executed_block_hash
            let store_data_executed_block_hash = StoreData {
                region: 6,
                key: block_height_bytes.clone(),
                value: vec![0],
            };
            wal_data.push(store_data_executed_block_hash);

            let smsg = bincode::serialize(&wal_data).unwrap();
            if let Err(status_code) = self
                .wal_save_message(block_height, LogType::FinalizeBlock, &smsg)
                .await
            {
                return Err(status_code);
            }
        }

        // exec block
        let (executed_block_status, executed_block_hash) =
            exec_block(block).await.map_err(|e| {
                warn!("exec_block({}) error: {}", block_height, e.to_string());
                e
            })?;
        executed_block_status.is_success().map_err(|e| {
            warn!("exec_block({}) error: {}", block_height, e.to_string());
            e
        })?;
        info!("executed_block_hash: {:?}", &executed_block_hash);

        // update auth and pool
        {
            let mut auth = self.auth.write().await;
            let mut pool = self.pool.write().await;
            auth.insert_tx_hash(block_height, tx_hash_list.clone());
            pool.update(&tx_hash_list);
        }

        if let Some(store_data_sys_config_utxo) = store_data_sys_config_utxo {
            store_data(
                storage_client(),
                store_data_sys_config_utxo.region,
                store_data_sys_config_utxo.key,
                store_data_sys_config_utxo.value,
            )
            .await
            .is_success()?;
        }
        store_data(
            storage_client(),
            store_data_block.region,
            store_data_block.key,
            store_data_block.value,
        )
        .await
        .is_success()?;
        store_data(
            storage_client(),
            6,
            block_height_bytes.clone(),
            executed_block_hash,
        )
        .await
        .is_success()?;
        store_data(
            storage_client(),
            store_data_current_height.region,
            store_data_current_height.key,
            store_data_current_height.value,
        )
        .await
        .is_success()?;
        store_data(
            storage_client(),
            store_data_current_hash.region,
            store_data_current_hash.key,
            store_data_current_hash.value,
        )
        .await
        .is_success()?;

        info!(
            "finalize_block: {}, block_hash: 0x{}",
            block_height,
            hex::encode(&block_hash)
        );

        self.wal_log.write().await.clear_file().unwrap();
        Ok(())
    }

    pub async fn commit_block(
        &mut self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCode> {
        if height <= self.block_number {
            warn!(
                "commit_block: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number, height
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if height > self.block_number + 1 {
            warn!(
                "commit_block: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number, height
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        let bft_proposal = match ProposalEnum::decode(proposal)
            .map_err(|_| {
                warn!("decode ProposalEnum failed");
                StatusCode::DecodeError
            })?
            .proposal
        {
            Some(Proposal::BftProposal(bft_proposal)) => Ok(bft_proposal),
            None => {
                warn!("commit_block: proposal({}) is none", height);
                Err(StatusCode::NoneProposal)
            }
        }?;

        if let Some(mut full_block) = bft_proposal.proposal {
            full_block.proof = proof.to_vec();

            let block_hash = get_block_hash(kms_client(), full_block.header.as_ref()).await?;

            let prev_hash = full_block.header.clone().unwrap().prevhash;

            if prev_hash != self.block_hash {
                warn!(
                    "commit_block: proposal(0x{})'s prev-hash is not equal with chain's block_hash",
                    hex::encode(&block_hash)
                );
                return Err(StatusCode::ProposalCheckError);
            }

            info!("height: {} hash 0x{}", height, hex::encode(&block_hash));
            self.finalize_block(full_block, block_hash.clone()).await?;

            self.block_number += 1;
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
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCode> {
        let block_hash = get_block_hash(kms_client(), block.header.as_ref()).await?;
        let header = block.header.clone().unwrap();
        let height = header.height;

        if height <= self.block_number {
            warn!(
                "process_block: ProposalTooLow, self block number: {}, remote block number: {}",
                self.block_number, height
            );
            return Err(StatusCode::ProposalTooLow);
        }

        if height > self.block_number + 1 {
            warn!(
                "process_block: ProposalTooHigh, self block number: {}, remote block number: {}",
                self.block_number, height
            );
            return Err(StatusCode::ProposalTooHigh);
        }

        if header.prevhash != self.block_hash {
            warn!(
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

        match kms_client()
            .check_transactions(block.body.clone().ok_or(StatusCode::NoneBlockBody)?)
            .await
        {
            Ok(response) => StatusCode::from(response.into_inner()).is_success()?,
            Err(e) => {
                warn!(
                    "check_transactions check block(0x{})'s txs failed: {}",
                    hex::encode(&block_hash),
                    e.to_string()
                );
                return Err(StatusCode::KmsServerNotReady);
            }
        }

        self.finalize_block(block, get_block_hash(kms_client(), Some(&header)).await?)
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
        match self.wal_log.write().await.save(height, ltype, msg) {
            Ok(hlen) => Ok(hlen),
            Err(e) => {
                log::error!("wal failed: {}", e.to_string());
                Err(StatusCode::NoneStatusCode)
            }
        }
    }

    pub async fn load_wal_log(&mut self) -> Option<(ConsensusConfiguration, ChainStatus)> {
        let vec_buf = self.wal_log.read().await.load();
        if vec_buf.is_empty() {
            return None;
        }
        let height = self.wal_log.read().await.get_cur_height();
        let config = self.get_system_config().await;
        let mut consensus_config = ConsensusConfiguration {
            height,
            block_interval: config.block_interval,
            validators: config.validators.clone(),
        };
        let mut chain_status = ChainStatus {
            version: config.version,
            chain_id: config.chain_id.clone(),
            height,
            hash: Some(Hash {
                hash: self.block_hash.clone(),
            }),
            address: None,
        };
        for (mtype, vec_out) in vec_buf {
            let log_type: LogType = mtype.into();
            info!("load_wal_log chain type {:?}({})", log_type, mtype);
            if let LogType::FinalizeBlock = log_type {
                let wal_data: Vec<StoreData> = match deserialize(&vec_out) {
                    Err(e) => {
                        warn!("load_wal_log: deserialize({:?}) error {}", log_type, e);
                        continue;
                    }
                    Ok(p) => p,
                };

                let mut block_hash = None;
                let mut block_bytes = None;
                let mut storage_data_exec = None;
                for data in wal_data {
                    if data.region == 6 {
                        info!("redo exec_block: {:?}", data.key);
                        storage_data_exec = Some(data.clone());
                        continue;
                    }
                    if data.region == 0 && data.key == 1u64.to_be_bytes().to_vec() {
                        block_hash = Some(data.value.clone());
                    }
                    if data.region == 11 {
                        block_bytes = Some(data.value.clone());
                    }
                    store_data(storage_client(), data.region, data.key.clone(), data.value).await;
                    if let Ok(v) = load_data(storage_client(), data.region, data.key.clone()).await
                    {
                        info!(
                            "store success region: {}, key: {:?}, value: {:?}",
                            &data.region, &data.key, v
                        );
                    }
                }

                if let Some(block_hash) = block_hash {
                    self.block_number = height;
                    self.block_hash = block_hash;
                }

                if let Some(block_bytes) = block_bytes {
                    match Block::decode(block_bytes.as_slice()) {
                        Ok(block) => {
                            if let Some(storage_data_exec) = storage_data_exec {
                                match exec_block(block).await {
                                    Ok((_, executed_block_hash)) => {
                                        store_data(
                                            storage_client(),
                                            storage_data_exec.region,
                                            storage_data_exec.key,
                                            executed_block_hash,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        panic!("wal redo exec_block error: {}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            panic!("wal block_hash decode error: {}", e);
                        }
                    }
                }
                consensus_config = ConsensusConfiguration {
                    height,
                    block_interval: config.block_interval,
                    validators: config.validators.clone(),
                };
                chain_status = ChainStatus {
                    version: config.version,
                    chain_id: config.chain_id.clone(),
                    height,
                    hash: Some(Hash {
                        hash: self.block_hash.clone(),
                    }),
                    address: None,
                };
            }
        }
        // candidate_block need update
        self.clear_candidate();

        self.wal_log.write().await.clear_file().unwrap();
        info!("load_wal_log ends");
        Some((consensus_config, chain_status))
    }
}
