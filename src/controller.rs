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
    check_tx_exists, get_proposal, get_tx, load_data, remove_block, remove_proposal, remove_tx,
    write_tx,
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
    notifier: Arc<Notifier>,
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
        notifier: Arc<Notifier>,
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
        )));
        Controller {
            network_port,
            storage_port,
            auth,
            pool,
            chain,
            notifier,
        }
    }

    pub async fn init(&self, init_block_number: u64) {
        {
            let mut chain = self.chain.write().await;
            chain.add_proposal().await
        }
        {
            let mut auth = self.auth.write().await;
            auth.init(init_block_number).await;
        }
        self.notifier.list(3600u64);
        self.proc_sync_notify().await;
    }

    pub async fn proc_sync_notify(&self) {
        let c = self.clone();
        let notifier_clone = c.notifier.clone();
        tokio::spawn(async move {
            notifier_clone.watch().await;
        });
        let notifier_clone = c.notifier.clone();
        tokio::spawn(async move {
            loop {
                time::delay_for(Duration::new(60, 0)).await;
                {
                    notifier_clone.list(120);
                }
            }
        });
        let notifier_clone = c.notifier.clone();
        tokio::spawn(async move {
            loop {
                time::delay_for(Duration::new(1, 0)).await;
                {
                    let events = notifier_clone.fetch_events();
                    for event in events {
                        match event.folder.as_str() {
                            "txs" => {
                                if let Ok(tx_hash) = hex::decode(&event.filename) {
                                    if let Some(raw_tx) = get_tx(&tx_hash).await {
                                        if let Ok(hash) = c.rpc_send_raw_transaction(raw_tx).await {
                                            if hash == tx_hash {
                                                continue;
                                            }
                                        }
                                    }
                                }
                                // any failed delete the tx file
                                warn!("sync tx invalid");
                                remove_tx(event.filename.as_str()).await;
                            }
                            "proposals" => {
                                if let Ok(block_hash) = hex::decode(&event.filename) {
                                    if let Some(block) = get_proposal(&block_hash).await {
                                        if let Some(block_body) = block.clone().body {
                                            let tx_hash_list = block_body.tx_hashes;
                                            let is_valid = {
                                                for hash in tx_hash_list.iter() {
                                                    if !check_tx_exists(hash) {
                                                        return false;
                                                    }
                                                }
                                                true
                                            };
                                            if is_valid {
                                                info!("add proposal");
                                                let mut chain = c.chain.write().await;
                                                if chain.add_block(block).await {
                                                    continue;
                                                } else {
                                                    warn!("add_block failed");
                                                }
                                            } else {
                                                warn!("find tx failed");
                                            }
                                        } else {
                                            warn!("get block_body failed");
                                        }
                                    } else {
                                        warn!("get_block failed");
                                    }
                                } else {
                                    warn!("decode filename failed {}", &event.filename);
                                }
                                // any failed delete the proposal file
                                warn!("sync proposal invalid");
                                remove_proposal(event.filename.as_str()).await;
                            }
                            "blocks" => {
                                if u64::from_str_radix(event.filename.as_str(), 10).is_ok() {
                                    {
                                        let mut chain = c.chain.write().await;
                                        chain.proc_sync_block().await;
                                        continue;
                                    }
                                }
                                // any failed delete the block file
                                warn!("sync block invalid {}", event.filename.as_str());
                                remove_block(event.filename.as_str()).await;
                            }
                            _ => panic!("unexpected folder"),
                        }
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

        let is_exists = check_tx_exists(tx_hash.as_slice());
        if !is_exists {
            let mut raw_tx_bytes: Vec<u8> = Vec::new();
            let _ = raw_tx.encode(&mut raw_tx_bytes);
            write_tx(tx_hash.as_slice(), raw_tx_bytes.as_slice()).await;
        }
        let mut pool = self.pool.write().await;
        let is_ok = pool.enqueue(tx_hash.clone());
        if is_ok {
            Ok(tx_hash)
        } else {
            Err("dup".to_owned())
        }
    }

    pub async fn rpc_get_block_by_hash(&self, _hash: Vec<u8>) -> Result<CompactBlock, String> {
        Err("unimplemented".to_owned())
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
            let ret = load_data(self.storage_port, 1, tx_hash).await;
            if let Ok(raw_tx_bytes) = ret {
                let ret = RawTransaction::decode(raw_tx_bytes.as_slice());
                if ret.is_err() {
                    Err("decode failed".to_owned())
                } else {
                    let raw_tx = ret.unwrap();
                    Ok(raw_tx)
                }
            } else {
                Err("can't get transaction".to_owned())
            }
        }
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, String> {
        let auth = self.auth.read().await;
        let sys_config = auth.get_system_config();
        Ok(sys_config)
    }

    pub async fn chain_get_proposal(&self) -> Result<Vec<u8>, String> {
        {
            let chain = self.chain.read().await;
            if let Some(proposal) = chain.get_candidate_block_hash() {
                return Ok(proposal);
            }
        }

        // there are no proposal, try to add it
        let mut chain = self.chain.write().await;
        chain.add_proposal().await;
        Err("get proposal error".to_owned())
    }

    pub async fn chain_check_proposal(&self, proposal: &[u8]) -> Result<bool, String> {
        let chain = self.chain.read().await;
        let ret = chain.check_proposal(proposal).await;
        Ok(ret)
    }

    pub async fn chain_commit_block(&self, proposal: &[u8], proof: &[u8]) -> Result<(), String> {
        let mut chain = self.chain.write().await;
        chain.commit_block(proposal, proof).await;
        Ok(())
    }

    pub async fn process_network_msg(&self, _msg: NetworkMsg) -> Result<(), String> {
        Ok(())
    }
}
