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
use crate::node_manager::ChainStatus;
use crate::pool::Pool;
use crate::util::*;
use crate::utxo_set::SystemConfig;
use crate::GenesisBlock;
use cita_cloud_proto::blockchain::raw_transaction::Tx;
use cita_cloud_proto::blockchain::{Block, BlockHeader, RawTransaction, RawTransactions};
use cita_cloud_proto::common::{
    proposal_enum::Proposal, BftProposal, ConsensusConfiguration, Hash, ProposalEnum,
};
use cloud_util::common::get_tx_hash;
use cloud_util::crypto::{get_block_hash, hash_data};
use cloud_util::storage::{load_data, store_data};
use cloud_util::unix_now;
use log::{info, warn};
use prost::Message;
use status_code::StatusCode;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;

#[derive(PartialEq)]
pub enum ChainStep {
    SyncStep,
    OnlineStep,
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
}

impl Chain {
    pub fn new(
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        pool: Arc<RwLock<Pool>>,
        auth: Arc<RwLock<Authentication>>,
        genesis: GenesisBlock,
    ) -> Self {
        Chain {
            block_number: current_block_number,
            block_hash: current_block_hash,
            candidates: HashMap::new(),
            own_proposal: None,
            pool,
            auth,
            genesis,
        }
    }

    pub async fn init(&self, init_block_number: u64) {
        if init_block_number == 0 {
            info!("finalize genesis block");
            let mut interval = time::interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                match self
                    .finalize_block(self.genesis.genesis_block(), self.block_hash.clone())
                    .await
                {
                    Ok(()) | Err(StatusCode::ReenterBlock) => break,
                    _ => warn!("executor not ready! Retrying"),
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

        let state_root = load_data(storage_client(), 6, pre_height_bytes.clone()).await?;

        let proof = get_compact_block(pre_h).await?.1;

        Ok((state_root, proof))
    }

    pub async fn get_proposal(&self) -> Result<(u64, Vec<u8>, StatusCode), StatusCode> {
        if let Some((h, _, block)) = self.own_proposal.clone() {
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

            return Ok((h, self.assemble_proposal(block, h).await?, status));
        }
        Err(StatusCode::NoCandidate)
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
                            0,
                            lock_id.to_be_bytes().to_vec(),
                            utxo_tx.transaction_hash.clone(),
                        )
                        .await
                        .is_success()?;
                    }
                };
            }
        }

        let block_bytes = {
            let mut buf = Vec::with_capacity(block.encoded_len());
            block.encode(&mut buf).map_err(|_| {
                warn!("encode Block failed");
                StatusCode::EncodeError
            })?;
            buf
        };

        let block_height = block
            .header
            .as_ref()
            .ok_or(StatusCode::NoneBlockHeader)?
            .height;
        let block_height_bytes = block_height.to_be_bytes().to_vec();

        store_data(
            storage_client(),
            11,
            block_height_bytes.clone(),
            block_bytes,
        )
        .await
        .is_success()?;

        let tx_hash_list = get_tx_hash_list(block.body.as_ref().ok_or(StatusCode::NoneBlockBody)?)?;

        // exec block
        let executed_block_hash = exec_block(block).await.map_err(|e| {
            warn!("exec_block({}) error: {}", block_height, e.to_string());
            e
        })?;
        // region 6 : block_height - executed_block_hash
        store_data(
            storage_client(),
            6,
            block_height_bytes.clone(),
            executed_block_hash.clone(),
        )
        .await
        .is_success()?;

        // this must be before update pool
        {
            let mut auth = self.auth.write().await;
            auth.insert_tx_hash(block_height, tx_hash_list.clone());
        }

        // update pool
        {
            let mut pool = self.pool.write().await;
            pool.update(&tx_hash_list);
        }

        // region 0: 0 - current height; 1 - current hash
        store_data(
            storage_client(),
            0,
            0u64.to_be_bytes().to_vec(),
            block_height_bytes,
        )
        .await
        .is_success()?;
        store_data(
            storage_client(),
            0,
            1u64.to_be_bytes().to_vec(),
            block_hash.clone(),
        )
        .await
        .is_success()?;

        info!(
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

            print_main_chain(&[block_hash.clone(); 1], self.block_number);

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
}
