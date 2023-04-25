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

use cita_cloud_proto::{
    blockchain::Block, client::ExecutorClientTrait, status_code::StatusCodeEnum,
};

use crate::grpc_client::executor_client;

pub async fn exec_block(block: Block) -> (StatusCodeEnum, Vec<u8>) {
    match executor_client().exec(block).await {
        Ok(hash_respond) => (
            StatusCodeEnum::from(
                hash_respond
                    .status
                    .unwrap_or_else(|| StatusCodeEnum::NoneStatusCode.into()),
            ),
            hash_respond
                .hash
                .unwrap_or(cita_cloud_proto::common::Hash { hash: vec![] })
                .hash,
        ),
        Err(e) => {
            warn!("execute block failed: {}", e.to_string());
            (StatusCodeEnum::ExecuteServerNotReady, vec![])
        }
    }
}
