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

use crate::controller::Controller;
use cita_cloud_proto::health_check::{
    health_check_response::ServingStatus, health_server::Health, HealthCheckRequest,
    HealthCheckResponse,
};
use tonic::{Request, Response, Status};

// grpc server of Health Check
pub struct HealthCheckServer {
    _controller: Controller,
}

impl HealthCheckServer {
    pub fn new(_controller: Controller) -> Self {
        HealthCheckServer { _controller }
    }
}

#[tonic::async_trait]
impl Health for HealthCheckServer {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let reply = Response::new(HealthCheckResponse {
            status: ServingStatus::Serving.into(),
        });
        Ok(reply)
    }
}
