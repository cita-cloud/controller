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
use crate::chain::Chain;
use crate::pool::Pool;
use crate::sync::Notifier;
use crate::util::{
    check_tx_exists, get_network_status, get_new_tx, get_proposal, get_tx, load_data, load_tx_info,
    remove_proposal, remove_tx, write_new_tx,
};
use crate::utxo_set::SystemConfig;
use crate::GenesisBlock;
use cita_cloud_proto::blockchain::CompactBlock;
use cita_cloud_proto::controller::RawTransaction;
use cita_cloud_proto::network::NetworkMsg;
use log::{info, warn};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;

#[derive(Clone)]
pub struct Controller {
    network_port: u16,
    storage_port: u16,
    auth: Arc<RwLock<Authentication>>,
    pool: Arc<RwLock<Pool>>,
    chain: Arc<RwLock<Chain>>,
    txs_notifier: Arc<Notifier>,
    proposals_notifier: Arc<Notifier>,
    blocks_notifier: Arc<Notifier>,
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
        txs_notifier: Arc<Notifier>,
        proposals_notifier: Arc<Notifier>,
        blocks_notifier: Arc<Notifier>,
        key_id: u64,
        node_address: Vec<u8>,
    ) -> Self {
        let auth = Arc::new(RwLock::new(Authentication::new(
            kms_port,
            storage_port,
            sys_config,
        )));
        let pool = Arc::new(RwLock::new(Pool::new(500)));
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
            node_address,
        )));
        Controller {
            network_port,
            storage_port,
            auth,
            pool,
            chain,
            txs_notifier,
            proposals_notifier,
            blocks_notifier,
        }
    }

    pub async fn init(&self, init_block_number: u64) {
        {
            let mut chain = self.chain.write().await;
            chain.init(init_block_number).await;
            chain.add_proposal().await
        }
        {
            let mut auth = self.auth.write().await;
            auth.init(init_block_number).await;
        }
        self.proc_sync_notify().await;
    }

    pub async fn proc_sync_notify(&self) {
        // setup list
        let txs_notifier_clone = self.txs_notifier.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(30, 0));
            loop {
                interval.tick().await;
                {
                    txs_notifier_clone.list(0);
                }
            }
        });

        let proposals_notifier_clone = self.proposals_notifier.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(30, 0));
            loop {
                interval.tick().await;
                {
                    proposals_notifier_clone.list(0);
                }
            }
        });

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

        // setup watch
        let txs_notifier_clone = self.txs_notifier.clone();
        tokio::spawn(async move {
            txs_notifier_clone.watch().await;
        });

        let proposals_notifier_clone = self.proposals_notifier.clone();
        tokio::spawn(async move {
            proposals_notifier_clone.watch().await;
        });

        let blocks_notifier_clone = self.blocks_notifier.clone();
        tokio::spawn(async move {
            blocks_notifier_clone.watch().await;
        });

        // proc notify event
        let txs_notifier_clone = self.txs_notifier.clone();
        let controller_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(1, 0));
            loop {
                interval.tick().await;
                {
                    while let Some(event) = txs_notifier_clone.queue.pop() {
                        let file_name = event.filename;
                        let mut is_new_tx = true;
                        let tx_hash_str =
                            if let Some(striped_file_name) = file_name.strip_prefix("new_") {
                                // skip prefix new_
                                striped_file_name
                            } else {
                                is_new_tx = false;
                                &file_name
                            };
                        if let Ok(tx_hash) = hex::decode(tx_hash_str) {
                            let opt_raw_tx = if is_new_tx {
                                get_new_tx(&tx_hash).await
                            } else {
                                get_tx(&tx_hash).await
                            };
                            if let Some(raw_tx) = opt_raw_tx {
                                let ret = controller_clone.rpc_send_raw_transaction(raw_tx).await;
                                match ret {
                                    Ok(hash) => {
                                        if hash == tx_hash {
                                            continue;
                                        } else {
                                            warn!("tx hash mismatch");
                                        }
                                    }
                                    Err(e) => {
                                        if e == "dup"
                                            || e == "Invalid valid_until_block"
                                            || e == "internal err"
                                        {
                                            continue;
                                        } else {
                                            warn!("add sync tx failed: {:?}", e);
                                        }
                                    }
                                }
                            }
                        }
                        // any failed delete the tx file
                        warn!("sync tx invalid");
                        remove_tx(file_name.as_str()).await;
                    }
                }
            }
        });

        let proposals_notifier_clone = self.proposals_notifier.clone();
        let chain_clone = self.chain.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::new(1, 0));
            loop {
                interval.tick().await;
                {
                    while let Some(event) = proposals_notifier_clone.queue.pop() {
                        if let Ok(block_hash) = hex::decode(&event.filename) {
                            if let Some(block) = get_proposal(&block_hash).await {
                                info!("add proposal");
                                let mut chain = chain_clone.write().await;
                                if chain.add_remote_proposal(block).await {
                                    continue;
                                } else {
                                    warn!("add_remote_proposal failed");
                                }
                            } else {
                                warn!("get_proposal failed");
                            }
                        } else {
                            warn!("decode filename failed {}", &event.filename);
                        }
                        // any failed delete the proposal file
                        warn!("sync proposal invalid {}", &event.filename);
                        remove_proposal(event.filename.as_str()).await;
                    }
                }
            }
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
    ) -> Result<Vec<u8>, String> {
        let tx_hash = {
            let auth = self.auth.read().await;
            auth.check_raw_tx(raw_tx.clone()).await?
        };

        let mut pool = self.pool.write().await;
        let is_ok = pool.enqueue(tx_hash.clone());
        if is_ok {
            let is_exists = check_tx_exists(tx_hash.as_slice());
            if !is_exists {
                let mut raw_tx_bytes: Vec<u8> = Vec::new();
                let _ = raw_tx.encode(&mut raw_tx_bytes);
                write_new_tx(tx_hash.as_slice(), raw_tx_bytes.as_slice()).await;
            }
            Ok(tx_hash)
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

    pub async fn chain_get_proposal(&self) -> Result<(u64, Vec<u8>), String> {
        {
            let chain = self.chain.read().await;
            if let Some(proposal) = chain.get_proposal().await {
                return Ok(proposal);
            }
        }
        {
            let mut chain = self.chain.write().await;
            chain.add_proposal().await;
        }
        {
            let chain = self.chain.read().await;
            if let Some(proposal) = chain.get_proposal().await {
                return Ok(proposal);
            }
        }
        Err("get proposal error".to_owned())
    }

    pub async fn chain_check_proposal(&self, height: u64, proposal: &[u8]) -> Result<bool, String> {
        let chain = self.chain.read().await;
        let ret = chain.check_proposal(height, proposal).await;
        Ok(ret)
    }

    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<(), String> {
        let mut chain = self.chain.write().await;
        chain.commit_block(height, proposal, proof).await;
        Ok(())
    }

    pub async fn process_network_msg(&self, _msg: NetworkMsg) -> Result<(), String> {
        Ok(())
    }
}
