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

use cita_cloud_proto::{
    common::{
        ConsensusConfiguration, ConsensusConfigurationResponse, Empty, Proposal, ProposalResponse,
        ProposalWithProof,
    },
    controller::consensus2_controller_service_server::Consensus2ControllerService,
    status_code::StatusCodeEnum,
};

use crate::controller::Controller;

//grpc server for Consensus2ControllerService
pub struct Consensus2ControllerServer {
    controller: Controller,
}

impl Consensus2ControllerServer {
    pub(crate) fn new(controller: Controller) -> Self {
        Consensus2ControllerServer { controller }
    }
}

#[tonic::async_trait]
impl Consensus2ControllerService for Consensus2ControllerServer {
    #[instrument(skip_all)]
    async fn get_proposal(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ProposalResponse>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("get_proposal request: {:?}", request);

        match self.controller.chain_get_proposal().await {
            Ok((height, data)) => {
                let proposal = Proposal { height, data };
                Ok(Response::new(ProposalResponse {
                    status: Some(StatusCodeEnum::Success.into()),
                    proposal: Some(proposal),
                }))
            }
            Err(e) => {
                warn!("rpc get proposal failed: {}", e.to_string());
                Ok(Response::new(ProposalResponse {
                    status: Some(e.into()),
                    proposal: None,
                }))
            }
        }
    }

    #[instrument(skip_all)]
    async fn check_proposal(
        &self,
        request: Request<Proposal>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("check_proposal request: {:?}", request);

        let proposal = request.into_inner();

        let height = proposal.height;
        let data = proposal.data;

        match self.controller.chain_check_proposal(height, &data).await {
            Err(e) => {
                warn!("rpc check proposal({}) failed: {}", height, e.to_string());
                Ok(Response::new(e.into()))
            }
            Ok(_) => Ok(Response::new(StatusCodeEnum::Success.into())),
        }
    }

    #[instrument(skip_all)]
    async fn commit_block(
        &self,
        request: Request<ProposalWithProof>,
    ) -> Result<Response<ConsensusConfigurationResponse>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("commit_block request: {:?}", request);

        let proposal_with_proof = request.into_inner();
        let proposal = proposal_with_proof
            .proposal
            .ok_or_else(|| Status::internal("missing proposal in ProposalWithProof request"))?;
        let height = proposal.height;
        let data = proposal.data;
        let proof = proposal_with_proof.proof;

        let config = {
            let rd = self.controller.auth.read().await;
            rd.get_system_config()
        };

        let result = if height != u64::MAX {
            self.controller
                .chain_commit_block(height, &data, &proof)
                .await
        } else {
            Ok(ConsensusConfiguration {
                height: self.controller.get_status().await.height,
                block_interval: config.block_interval,
                validators: config.validators.clone(),
            })
        };

        let response = match result {
            Ok(config) => Response::new(ConsensusConfigurationResponse {
                status: Some(StatusCodeEnum::Success.into()),
                config: Some(config),
            }),
            Err(e) => {
                warn!("rpc commit block({}) failed: {}", height, e);
                let con_cfg = ConsensusConfiguration {
                    height,
                    block_interval: config.block_interval,
                    validators: config.validators,
                };
                Response::new(ConsensusConfigurationResponse {
                    status: Some(e.into()),
                    config: Some(con_cfg),
                })
            }
        };

        Ok(response)
    }
}
