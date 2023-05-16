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

use tonic::{Request, Response, Status};

use cita_cloud_proto::controller::CrossChainProof;
use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    common::{Empty, Hash, Hashes, NodeNetInfo, NodeStatus, Proof, StateRoot},
    controller::{
        rpc_service_server::RpcService, BlockNumber, Flag, SystemConfig, TransactionIndex,
    },
    status_code::StatusCodeEnum,
};

use crate::{
    controller::Controller,
    grpc_client::storage::get_hash_in_range,
    system_config::{LOCK_ID_BUTTON, LOCK_ID_VERSION},
};

// grpc server of RPC
pub struct RPCServer {
    controller: Controller,
}

impl RPCServer {
    pub(crate) fn new(controller: Controller) -> Self {
        RPCServer { controller }
    }
}

#[tonic::async_trait]
impl RpcService for RPCServer {
    #[instrument(skip_all)]
    async fn get_block_number(
        &self,
        request: Request<Flag>,
    ) -> Result<Response<BlockNumber>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_number request: {:?}", request);

        let flag = request.into_inner().flag;
        self.controller
            .rpc_get_block_number(flag)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e)),
                |block_number| {
                    let reply = Response::new(BlockNumber { block_number });
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn send_raw_transaction(
        &self,
        request: Request<RawTransaction>,
    ) -> Result<Response<Hash>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("send_raw_transaction request: {:?}", request);

        let raw_tx = request.into_inner();

        self.controller
            .rpc_send_raw_transaction(raw_tx, self.controller.config.enable_forward)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |tx_hash| {
                    let reply = Response::new(Hash { hash: tx_hash });
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn send_raw_transactions(
        &self,
        request: Request<RawTransactions>,
    ) -> Result<Response<Hashes>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("send_raw_transactions request: {:?}", request);

        let raw_txs = request.into_inner();

        self.controller
            .batch_transactions(raw_txs, self.controller.config.enable_forward)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |hashes| {
                    let reply = Response::new(hashes);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_block_by_hash(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<CompactBlock>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_by_hash request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_block_by_hash(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_height_by_hash(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<BlockNumber>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_height_by_hash request: {:?}", request);

        let hash = request.into_inner().hash;

        self.controller
            .rpc_get_height_by_hash(hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |height| {
                    let reply = Response::new(height);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_block_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<CompactBlock>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_by_number request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_by_number(block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_state_root_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<StateRoot>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_state_root_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        self.controller
            .rpc_get_state_root_by_number(height)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |state_root| {
                    let reply = Response::new(state_root);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_proof_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<Proof>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_proof_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        self.controller
            .rpc_get_proof_by_number(height)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |proof| {
                    let reply = Response::new(proof);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_block_detail_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<tonic::Response<Block>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_detail_by_number request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_detail_by_number(block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block| {
                    let reply = Response::new(block);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_transaction(
        &self,
        request: Request<Hash>,
    ) -> Result<tonic::Response<RawTransaction>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_by_number request: {:?}", request);

        let hash = request.into_inner();

        self.controller
            .rpc_get_transaction(hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |raw_tx| {
                    let reply = Response::new(raw_tx);
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_system_config(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<SystemConfig>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_system_config request: {:?}", request);

        self.controller.rpc_get_system_config().await.map_or_else(
            |e| Err(Status::invalid_argument(e.to_string())),
            |sys_config| {
                let reply = Response::new(sys_config.generate_proto_sys_config());
                Ok(reply)
            },
        )
    }

    #[instrument(skip_all)]
    async fn get_system_config_by_number(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<Response<SystemConfig>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_system_config_by_number request: {:?}", request);

        let height: u64 = request.into_inner().block_number;

        let mut initial_sys_config = self.controller.initial_sys_config.clone();
        let utxo_tx_hashes = self
            .controller
            .rpc_get_system_config()
            .await
            .unwrap()
            .utxo_tx_hashes;

        for lock_id in LOCK_ID_VERSION..LOCK_ID_BUTTON {
            let hash = utxo_tx_hashes.get(&lock_id).unwrap().to_owned();
            if hash != vec![0u8; 33] {
                let hash_in_range = get_hash_in_range(hash, height)
                    .await
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                if hash_in_range != vec![0u8; 33] {
                    //modify sys_config by utxo_tx
                    let res = initial_sys_config
                        .modify_sys_config_by_utxotx_hash(hash_in_range)
                        .await;
                    if res != StatusCodeEnum::Success {
                        return Err(Status::invalid_argument(res.to_string()));
                    }
                }
            }
        }

        let reply = Response::new(initial_sys_config.generate_proto_sys_config());

        Ok(reply)
    }

    #[instrument(skip_all)]
    async fn get_block_hash(
        &self,
        request: Request<BlockNumber>,
    ) -> Result<Response<Hash>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_block_hash request: {:?}", request);

        let block_number = request.into_inner().block_number;

        self.controller
            .rpc_get_block_hash(block_number)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block_hash| {
                    let reply = Response::new(Hash { hash: block_hash });
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_transaction_block_number(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<BlockNumber>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_transaction_block_number request: {:?}", request);

        let tx_hash = request.into_inner().hash;

        self.controller
            .rpc_get_tx_block_number(tx_hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |block_number| {
                    let reply = Response::new(BlockNumber { block_number });
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn get_transaction_index(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<TransactionIndex>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_transaction_index request: {:?}", request);

        let tx_hash = request.into_inner();

        self.controller
            .rpc_get_tx_index(tx_hash.hash)
            .await
            .map_or_else(
                |e| Err(Status::invalid_argument(e.to_string())),
                |tx_index| {
                    let reply = Response::new(TransactionIndex { tx_index });
                    Ok(reply)
                },
            )
    }

    #[instrument(skip_all)]
    async fn add_node(
        &self,
        request: Request<NodeNetInfo>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("add_node request: {:?}", request);

        let info = request.into_inner();

        let reply = Response::new(self.controller.rpc_add_node(info).await);

        Ok(reply)
    }

    #[instrument(skip_all)]
    async fn get_node_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<NodeStatus>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_node_status request: {:?}", request);

        Ok(Response::new(
            self.controller
                .rpc_get_node_status(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }

    #[instrument(skip_all)]
    async fn get_cross_chain_proof(
        &self,
        request: Request<Hash>,
    ) -> Result<Response<CrossChainProof>, Status> {
        Ok(Response::new(
            self.controller
                .rpc_get_cross_chain_proof(request.into_inner())
                .await
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        ))
    }
}
