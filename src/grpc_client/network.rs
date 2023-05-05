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
    client::NetworkClientTrait,
    common::{Empty, TotalNodeNetInfo},
    network::NetworkStatusResponse,
    status_code::StatusCodeEnum,
};

use crate::grpc_client::network_client;

#[macro_export]
macro_rules! impl_multicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> Vec<tokio::task::JoinHandle<()>> {
            use cita_cloud_proto::client::NetworkClientTrait;

            let nodes = self.node_manager.grab_node().await;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            let mut handle_vec = Vec::new();

            for node in nodes {
                debug!("multicast {} to {}: len: {}", $name, node, buf.len());

                let msg = cita_cloud_proto::network::NetworkMsg {
                    module: "controller".to_string(),
                    r#type: $name.into(),
                    origin: node.0,
                    msg: buf.clone(),
                };

                let handle = tokio::spawn(async move {
                    match $crate::grpc_client::network_client().send_msg(msg).await {
                        Ok(_) => {
                            debug!("multicast {} success", $name)
                        }
                        Err(status) => {
                            warn!("multicast {} to {} failed: {:?}", $name, node, status)
                        }
                    }
                });

                handle_vec.push(handle);
            }
            handle_vec
        }
    };
}

// todo change return to handle panic & unwrap
#[macro_export]
macro_rules! impl_unicast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, origin: u64, item: $type) -> tokio::task::JoinHandle<()> {
            use cita_cloud_proto::client::NetworkClientTrait;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            debug!(
                "unicast {} to origin({}): len: {}",
                $name,
                origin,
                buf.len()
            );

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin,
                msg: buf,
            };

            tokio::spawn(async move {
                match $crate::grpc_client::network_client().send_msg(msg).await {
                    Ok(_) => {}
                    Err(status) => {
                        warn!(
                            "unicast {} to origin({}) failed: {:?}",
                            $name, origin, status
                        )
                    }
                }
            })
        }
    };
}

#[macro_export]
macro_rules! impl_broadcast {
    ($func_name:ident, $type:ident, $name:expr) => {
        pub async fn $func_name(&self, item: $type) -> tokio::task::JoinHandle<()> {
            use cita_cloud_proto::client::NetworkClientTrait;

            let mut buf = Vec::new();

            item.encode(&mut buf)
                .expect(&($name.to_string() + " encode failed"));

            debug!("broadcast {}: len: {}", $name, buf.len());

            let msg = cita_cloud_proto::network::NetworkMsg {
                module: "controller".to_string(),
                r#type: $name.into(),
                origin: 0,
                msg: buf,
            };

            tokio::spawn(async move {
                match $crate::grpc_client::network_client().broadcast(msg).await {
                    Ok(_) => {}
                    Err(status) => {
                        warn!("broadcast {} failed: {:?}", $name, status)
                    }
                }
            })
        }
    };
}

pub async fn get_network_status() -> Result<NetworkStatusResponse, StatusCodeEnum> {
    let network_status_response = network_client()
        .get_network_status(Empty {})
        .await
        .map_err(|e| {
            warn!("get network status failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady
        })?;
    Ok(network_status_response)
}

pub async fn get_peers_info() -> Result<TotalNodeNetInfo, StatusCodeEnum> {
    let peers_info = network_client()
        .get_peers_net_info(Empty {})
        .await
        .map_err(|e| {
            warn!("get peers status failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady
        })?;
    Ok(peers_info)
}
