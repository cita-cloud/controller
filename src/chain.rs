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

use crate::system_config::{LOCK_ID_BLOCK_LIMIT, LOCK_ID_QUOTA_LIMIT};
use crate::{
    auth::Authentication, node_manager::ChainStatus, pool::Pool, system_config::SystemConfig,
    util::*, GenesisBlock,
};
use cita_cloud_proto::status_code::StatusCodeEnum;
use cita_cloud_proto::{
    blockchain::{raw_transaction::Tx, Block, BlockHeader, RawTransaction, RawTransactions},
    client::CryptoClientTrait,
    common::{proposal_enum::Proposal, ConsensusConfiguration, Hash, ProposalEnum},
    storage::Regions,
};
use cloud_util::{
    common::get_tx_hash,
    crypto::{get_block_hash, hash_data},
    storage::store_data,
    unix_now,
};
use prost::Message;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time};

#[derive(Eq, PartialEq)]
pub enum ChainStep {
    SyncStep,
    OnlineStep,
    BusyState,
}

#[allow(dead_code)]
pub struct Chain {
    block_number: u64,

    block_hash: Vec<u8>,
    // key is block_hash
    candidates: HashSet<Vec<u8>>,

    own_proposal: Option<(u64, Vec<u8>)>,

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
            candidates: HashSet::new(),
            own_proposal: None,
            pool,
            auth,
            genesis,
        }
    }

    pub async fn init(&self, _init_block_number: u64, server_retry_interval: u64) {
        let mut interval = time::interval(Duration::from_secs(server_retry_interval));
        loop {
            interval.tick().await;
            // if height != 0 block_hash is wrong but doesn't matter
            match self
                .finalize_block(self.genesis.genesis_block(), self.block_hash.clone())
                .await
            {
                Ok(()) | Err(StatusCodeEnum::ReenterBlock) => {
                    info!("executor service ready");
                    break;
                }
                Err(StatusCodeEnum::ExecuteServerNotReady) => {
                    warn!("executor service not ready: retrying...")
                }
                Err(e) => {
                    panic!("check executor service failed: {e:?}");
                }
            }
        }
    }

    pub async fn init_auth(&self, init_block_number: u64) {
        let mut auth = self.auth.write().await;
        auth.init(init_block_number).await;
    }

    pub async fn get_proposal(&self) -> Result<(u64, Vec<u8>), StatusCodeEnum> {
        if let Some(proposal) = self.own_proposal.clone() {
            Ok(proposal)
        } else {
            Err(StatusCodeEnum::NoCandidate)
        }
    }

    pub async fn add_remote_proposal(&mut self, block_hash: &[u8]) {
        self.candidates.insert(block_hash.to_vec());
    }

    pub fn is_candidate(&self, block_hash: &[u8]) -> bool {
        self.candidates.contains(block_hash)
    }

    pub fn is_own(&self, proposal_to_check: &[u8]) -> bool {
        if let Some((_, proposal_data)) = &self.own_proposal {
            proposal_data == proposal_to_check
        } else {
            false
        }
    }

    pub async fn add_proposal(
        &mut self,
        global_status: &ChainStatus,
        proposer: Vec<u8>,
    ) -> Result<(), StatusCodeEnum> {
        if self.next_step(global_status).await == ChainStep::SyncStep {
            Err(StatusCodeEnum::NodeInSyncMode)
        } else if self.own_proposal.is_some() {
            // already have own proposal
            Ok(())
        } else {
            let prevhash = self.block_hash.clone();
            let height = self.block_number + 1;

            let (tx_list, quota) = {
                let mut pool = self.pool.write().await;
                let ret = pool.package(self.block_number + 1);
                let (pool_len, pool_quota) = pool.pool_status();
                info!(
                    "package proposal({}): pool len: {}, pool quota: {}",
                    height, pool_len, pool_quota
                );
                ret
            };
            let tx_count = tx_list.len();

            let mut transantion_data = Vec::new();
            for raw_tx in tx_list.iter() {
                transantion_data.extend_from_slice(get_tx_hash(raw_tx)?);
            }
            let transactions_root = hash_data(crypto_client(), &transantion_data).await?;

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
                state_root: vec![],
            };

            let mut block_header_bytes = Vec::new();
            header
                .encode(&mut block_header_bytes)
                .expect("add proposal failed: encode BlockHeader failed");

            let block_hash = hash_data(crypto_client(), &block_header_bytes).await?;

            self.own_proposal =
                Some((height, assemble_proposal(full_block.clone(), height).await?));
            self.candidates.insert(block_hash.clone());

            info!(
                "add proposal({}): tx count: {}, quota: {}, hash: 0x{}",
                height,
                tx_count,
                quota,
                hex::encode(&block_hash),
            );

            Ok(())
        }
    }

    pub async fn check_proposal(&self, height: u64) -> Result<(), StatusCodeEnum> {
        if height != self.block_number + 1 {
            warn!(
                "check proposal({}) failed: get height: {}, need height: {}",
                height,
                height,
                self.block_number + 1
            );
            if height < self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn finalize_block(
        &self,
        mut block: Block,
        block_hash: Vec<u8>,
    ) -> Result<(), StatusCodeEnum> {
        let block_height = block
            .header
            .as_ref()
            .ok_or(StatusCodeEnum::NoneBlockHeader)?
            .height;

        // execute block, executed_blocks_hash == state_root
        {
            let (executed_blocks_status, executed_blocks_hash) = exec_block(block.clone()).await;

            info!(
                "execute block({}) {}: state_root: 0x{}. hash: 0x{}",
                block_height,
                executed_blocks_status,
                hex::encode(&executed_blocks_hash),
                hex::encode(&block_hash),
            );

            match executed_blocks_status {
                StatusCodeEnum::Success => {}
                StatusCodeEnum::ReenterBlock => {
                    warn!(
                        "ReenterBlock({}): status: {}, state_root: 0x{}",
                        block_height,
                        executed_blocks_status,
                        hex::encode(&executed_blocks_hash)
                    );
                    // The genesis block does not allow reenter
                    if block_height == 0 {
                        return Err(StatusCodeEnum::ReenterBlock);
                    }
                }
                status_code => {
                    return Err(status_code);
                }
            }

            // if state_root is not empty should verify it
            if block.state_root.is_empty() {
                block.state_root = executed_blocks_hash;
            } else if block.state_root != executed_blocks_hash {
                warn!("finalize block failed: check state_root error");
                return Err(StatusCodeEnum::StateRootCheckError);
            }
        }

        // persistence to storage
        {
            let block_with_stateroot_bytes = {
                let mut buf = Vec::with_capacity(block.encoded_len());
                block.encode(&mut buf).map_err(|_| {
                    warn!("finalize block failed: encode Block failed");
                    StatusCodeEnum::EncodeError
                })?;
                buf
            };

            store_data(
                storage_client(),
                i32::from(Regions::AllBlockData) as u32,
                block_height.to_be_bytes().to_vec(),
                block_with_stateroot_bytes,
            )
            .await
            .is_success()?;
            info!(
                "store AllBlockData({}) success: hash: 0x{}",
                block_height,
                hex::encode(&block_hash)
            );
        }

        // update auth pool and systemconfig
        // even empty block, we also need update current height of auth
        {
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

            let tx_hash_list =
                get_tx_hash_list(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)?;
            self.auth
                .write()
                .await
                .insert_tx_hash(block_height, tx_hash_list.clone());
            self.pool.write().await.remove(&tx_hash_list);
            info!(
                "update auth and pool, tx_hash_list len {}",
                tx_hash_list.len()
            );
        }

        // success and print pool status
        {
            let (pool_len, pool_quota) = self.pool.read().await.pool_status();
            info!(
                "finalize block({}) success: pool len: {}, pool quota: {}. hash: 0x{}",
                block_height,
                pool_len,
                pool_quota,
                hex::encode(&block_hash)
            );
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn commit_block(
        &mut self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCodeEnum> {
        if height != self.block_number + 1 {
            warn!(
                "commit block({}) failed: get height: {}, need height: {}",
                height,
                height,
                self.block_number + 1
            );
            if height < self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        let bft_proposal = match ProposalEnum::decode(proposal)
            .map_err(|_| {
                warn!(
                    "commit block({}) failed: decode ProposalEnum failed",
                    height
                );
                StatusCodeEnum::DecodeError
            })?
            .proposal
        {
            Some(Proposal::BftProposal(bft_proposal)) => Ok(bft_proposal),
            None => {
                warn!("commit block({}) failed: proposal is none", height);
                Err(StatusCodeEnum::NoneProposal)
            }
        }?;

        if let Some(mut full_block) = bft_proposal.proposal {
            full_block.proof = proof.to_vec();

            let block_hash = get_block_hash(crypto_client(), full_block.header.as_ref()).await?;

            let prev_hash = full_block.header.clone().unwrap().prevhash;

            if prev_hash != self.block_hash {
                warn!(
                    "commit block({}) failed: get prehash: 0x{}, correct prehash: 0x{}. hash: 0x{}",
                    height,
                    hex::encode(&prev_hash),
                    hex::encode(&self.block_hash),
                    hex::encode(&block_hash),
                );
                return Err(StatusCodeEnum::ProposalCheckError);
            }

            info!(
                "commit block({}): hash: 0x{}",
                height,
                hex::encode(&block_hash)
            );
            self.finalize_block(full_block, block_hash.clone()).await?;

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

        Err(StatusCodeEnum::NoForkTree)
    }

    pub async fn process_block(
        &mut self,
        block: Block,
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCodeEnum> {
        let block_hash = get_block_hash(crypto_client(), block.header.as_ref()).await?;
        let header = block.header.clone().unwrap();
        let height = header.height;

        if height != self.block_number + 1 {
            warn!(
                "process block({}) failed: get height: {}, need height: {}",
                height,
                height,
                self.block_number + 1
            );
            if height < self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > self.block_number + 1 {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        if header.prevhash != self.block_hash {
            warn!(
                "process block({}) failed: get prehash: 0x{}, correct prehash: 0x{}. hash: 0x{}",
                height,
                hex::encode(&header.prevhash),
                hex::encode(&self.block_hash),
                hex::encode(&block_hash)
            );
            return Err(StatusCodeEnum::BlockCheckError);
        }

        let proposal_bytes_for_check = assemble_proposal(block.clone(), height).await?;

        let status = check_block(height, proposal_bytes_for_check, block.proof.clone()).await;
        if status != StatusCodeEnum::Success {
            return Err(status);
        }

        {
            let auth = self.auth.read().await;
            auth.check_transactions(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)?
        }

        match crypto_client()
            .check_transactions(block.body.clone().ok_or(StatusCodeEnum::NoneBlockBody)?)
            .await
        {
            Ok(code) => StatusCodeEnum::from(code).is_success()?,
            Err(e) => {
                warn!(
                    "process block({}) failed: check transactions failed: {}. hash: 0x{}",
                    height,
                    e.to_string(),
                    hex::encode(&block_hash),
                );
                return Err(StatusCodeEnum::CryptoServerNotReady);
            }
        }

        self.finalize_block(block, block_hash.clone()).await?;

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
            debug!("sync mode");
            ChainStep::SyncStep
        } else {
            debug!("online mode");
            ChainStep::OnlineStep
        }
    }

    pub async fn chain_get_tx(&self, tx_hash: &[u8]) -> Result<RawTransaction, StatusCodeEnum> {
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
