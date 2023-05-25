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
    network::{network_msg_handler_service_server::NetworkMsgHandlerService, NetworkMsg},
    status_code::StatusCodeEnum,
};

use crate::controller::Controller;

// grpc server of network msg handler
pub struct NetworkMsgHandlerServer {
    controller: Controller,
}

impl NetworkMsgHandlerServer {
    pub(crate) fn new(controller: Controller) -> Self {
        NetworkMsgHandlerServer { controller }
    }
}

#[tonic::async_trait]
impl NetworkMsgHandlerService for NetworkMsgHandlerServer {
    #[instrument(skip_all)]
    async fn process_network_msg(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<cita_cloud_proto::common::StatusCode>, Status> {
        cloud_util::tracer::set_parent(&request);
        debug!("process_network_msg request: {:?}", request);

        let msg = request.into_inner();
        if msg.module != "controller" {
            return Ok(Response::new(StatusCodeEnum::ModuleNotController.into()));
        }

        let msg_type = msg.r#type.clone();
        let msg_origin = msg.origin;
        match self.controller.process_network_msg(msg).await {
            Ok(_) => Ok(Response::new(StatusCodeEnum::Success.into())),
            Err(e) if e != StatusCodeEnum::HistoryDupTx || rand::random::<u16>() < 8 => {
                warn!(
                    "rpc process network msg failed: {}. from: {:x}, type: {}",
                    e.to_string(),
                    msg_origin,
                    msg_type
                );
                Ok(Response::new(e.into()))
            }
            Err(e) => Ok(Response::new(e.into())),
        }
    }
}
