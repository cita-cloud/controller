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
use cloud_util::unix_now;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Request, Response, Status};
use tracing::info;

// grpc server of Health Check
pub struct HealthCheckServer {
    controller: Controller,
    timestamp: AtomicU64,
    height: AtomicU64,
    timeout: u64,
}

impl HealthCheckServer {
    pub fn new(controller: Controller, timeout: u64) -> Self {
        HealthCheckServer {
            controller,
            timestamp: AtomicU64::new(unix_now()),
            height: AtomicU64::new(0),
            timeout,
        }
    }
}

#[tonic::async_trait]
impl Health for HealthCheckServer {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        info!("healthcheck entry!");
        let height = self.controller.rpc_get_block_number(true).await.unwrap();
        let timestamp = unix_now();
        let old_height = self.height.load(Ordering::Relaxed);
        let old_timestamp = self.timestamp.load(Ordering::Relaxed);

        let status = if height > old_height {
            self.height.store(height, Ordering::Relaxed);
            self.timestamp.store(timestamp, Ordering::Relaxed);
            info!(
                "healthcheck: block increase {} {} {}",
                old_height, height, timestamp
            );
            ServingStatus::Serving.into()
        } else {
            // block number not increase for a long time
            info!(
                "healthcheck: block not increase {} {} {}",
                height, old_timestamp, timestamp
            );
            if timestamp - old_timestamp > self.timeout * 1000 {
                ServingStatus::NotServing.into()
            } else {
                ServingStatus::Serving.into()
            }
        };

        let reply = Response::new(HealthCheckResponse { status });
        Ok(reply)
    }
}
