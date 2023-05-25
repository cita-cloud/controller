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

use prost::Message;
use std::{cmp::Ordering, collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time};

use cita_cloud_proto::{
    blockchain::{
        raw_transaction::Tx, Block, BlockHeader, CompactBlock, CompactBlockBody, RawTransaction,
        RawTransactions,
    },
    common::{ConsensusConfiguration, Hash, ProposalInner},
    status_code::StatusCodeEnum,
    storage::Regions,
};
use cloud_util::{common::extract_compact, unix_now};

use crate::{
    auth::Authentication,
    crypto::{check_transactions, get_block_hash, hash_data},
    grpc_client::{
        consensus::check_block,
        executor::exec_block,
        storage::{assemble_proposal, db_get_tx, store_data},
    },
    node_manager::ChainStatus,
    pool::Pool,
    system_config::SystemConfig,
    system_config::{LOCK_ID_BLOCK_LIMIT, LOCK_ID_QUOTA_LIMIT},
    util::*,
    GenesisBlock,
};

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
                    panic!("finalize genesis_block failed: {e:?}");
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

            let (tx_hash_list, quota) = {
                let mut pool = self.pool.write().await;
                let ret = pool.package(self.block_number + 1);
                let (pool_len, pool_quota) = pool.pool_status();
                info!(
                    "package proposal({}): pool len: {}, pool quota: {}",
                    height, pool_len, pool_quota
                );
                ret
            };
            let tx_count = tx_hash_list.len();
            let transactions_root = hash_data(&tx_hash_list.concat());

            let header = BlockHeader {
                prevhash: prevhash.clone(),
                timestamp: unix_now(),
                height,
                transactions_root,
                proposer,
            };

            let compact_block = CompactBlock {
                version: 0,
                header: Some(header.clone()),
                body: Some(CompactBlockBody {
                    tx_hashes: tx_hash_list,
                }),
            };

            let mut block_header_bytes = Vec::new();
            header
                .encode(&mut block_header_bytes)
                .expect("add proposal failed: encode BlockHeader failed");

            let block_hash = hash_data(&block_header_bytes);

            self.own_proposal = Some((height, assemble_proposal(&compact_block, height).await?));
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
            let block_bytes = {
                let mut buf = Vec::with_capacity(block.encoded_len());
                block.encode(&mut buf).map_err(|_| {
                    warn!("finalize block failed: encode Block failed");
                    StatusCodeEnum::EncodeError
                })?;
                let mut block_bytes = Vec::new();
                block_bytes.extend_from_slice(&block_hash);
                block_bytes.extend_from_slice(&buf);
                block_bytes
            };

            store_data(
                i32::from(Regions::AllBlockData) as u32,
                block_height.to_be_bytes().to_vec(),
                block_bytes,
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
            match height.cmp(&(self.block_number + 1)) {
                Ordering::Less => return Err(StatusCodeEnum::ProposalTooLow),
                Ordering::Greater => return Err(StatusCodeEnum::ProposalTooHigh),
                Ordering::Equal => (),
            }
        }

        let proposal_inner = ProposalInner::decode(proposal).map_err(|_| {
            warn!(
                "commit block({}) failed: decode ProposalInner failed",
                height
            );
            StatusCodeEnum::DecodeError
        })?;

        if let Some(compact_block) = proposal_inner.proposal {
            let block_hash = get_block_hash(compact_block.header.as_ref())?;

            let prev_hash = compact_block.header.clone().unwrap().prevhash;

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

            let mut tx_list = vec![];
            let tx_hashes = &compact_block
                .body
                .as_ref()
                .ok_or(StatusCodeEnum::NoneBlockBody)?
                .tx_hashes;

            for tx_hash in tx_hashes {
                if let Some(tx) = self.pool.read().await.pool_get_tx(tx_hash) {
                    tx_list.push(tx);
                } else {
                    return Err(StatusCodeEnum::NoneRawTx);
                }
            }

            let full_block = Block {
                version: 0,
                header: compact_block.header.clone(),
                body: Some(RawTransactions { body: tx_list }),
                proof: proof.to_vec(),
                state_root: vec![],
            };

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
        let block_hash = get_block_hash(block.header.as_ref())?;
        let header = block.header.clone().unwrap();
        let height = header.height;

        if height != self.block_number + 1 {
            warn!(
                "process block({}) failed: get height: {}, need height: {}",
                height,
                height,
                self.block_number + 1
            );
            match height.cmp(&(self.block_number + 1)) {
                Ordering::Less => return Err(StatusCodeEnum::ProposalTooLow),
                Ordering::Greater => return Err(StatusCodeEnum::ProposalTooHigh),
                Ordering::Equal => (),
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

        let compact_blk = extract_compact(block.clone());
        let proposal_bytes_for_check = assemble_proposal(&compact_blk, height).await?;

        check_block(height, proposal_bytes_for_check, block.proof.clone())
            .await
            .is_success()?;

        {
            let auth = self.auth.read().await;
            auth.check_transactions(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)?
        }

        check_transactions(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)
            .is_success()?;

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
