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

use std::{fs, path::Path};

use cloud_util::{common::read_toml, tracer::LogConfig};
use serde_derive::Deserialize;
use tokio::sync::OnceCell;

pub static CONFIG: OnceCell<ControllerConfig> = OnceCell::const_new();

#[allow(dead_code)]
pub fn controller_config() -> &'static ControllerConfig {
    CONFIG.get().unwrap()
}

/// ControllerConfig define majority of controller conduction: micro-server port, reconnect,
/// discovery other node, sync, log config, crypto related
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ControllerConfig {
    /// domain
    pub domain: String,
    /// controller service port
    pub controller_port: u16,
    /// network service port
    pub network_port: u16,
    /// consensus service port
    pub consensus_port: u16,
    /// storage service port
    pub storage_port: u16,
    /// crypto service port
    pub crypto_port: u16,
    /// executor service port
    pub executor_port: u16,
    /// self node address file path
    pub node_address: String,
    /// self validator address file path
    pub validator_address: String,
    /// audit blocks epoch length
    pub block_limit: u64,
    /// address length from crypto
    pub address_len: u32,
    /// hash length from crypto
    pub hash_len: u32,
    /// signature length from crypto
    pub signature_len: u32,
    /// other micro-serv reconnect interval
    pub server_retry_interval: u64,
    /// discovery other nodes interval
    pub origin_node_reconnect_interval: u64,
    /// inner healthy check through block height growth normally
    pub inner_block_growth_check_interval: u64,
    /// switch of tx forward
    pub enable_forward: bool,
    /// sync block height interval
    pub sync_interval: u64,
    /// sync block request times
    pub sync_req: u64,
    /// the height epoch to force send sync
    pub force_sync_epoch: u64,
    /// WAL log path
    pub wal_path: String,
    /// multi-send chain status interval, counted by block number
    /// recommend: n < origin_node_reconnect_interval / block_interval
    pub send_chain_status_interval_sync: u64,
    /// health check timeout
    /// how many seconds after block number stop increase, will report unhealthy
    pub health_check_timeout: u64,
    /// interval(seconds) of send HTTP2 Ping frames.
    pub http2_keepalive_interval: u64,
    /// if the ping is not acknowledged within the timeout(seconds), the connection will be closed.
    pub http2_keepalive_timeout: u64,
    /// the duration(seconds) specified will be the time to remain idle before sending TCP keepalive probes.
    pub tcp_keepalive: u64,
    /// enable metrics or not
    pub enable_metrics: bool,
    /// metrics exporter port
    pub metrics_port: u16,
    /// metrics histogram buckets
    pub metrics_buckets: Vec<f64>,
    /// the tx batch's upper limit of retransmission
    pub count_per_batch: usize,
    /// forward tx batch check interval
    pub buffer_duration: u64,
    /// danger mode
    pub is_danger: bool,
    /// log config
    pub log_config: LogConfig,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            domain: Default::default(),
            controller_port: 50004,
            network_port: 50000,
            consensus_port: 50001,
            storage_port: 50003,
            crypto_port: 50005,
            executor_port: 50002,
            node_address: "".to_string(),
            validator_address: "".to_string(),
            block_limit: 0,
            address_len: 20,
            hash_len: 32,
            signature_len: 128,
            server_retry_interval: 3,
            inner_block_growth_check_interval: 60,
            origin_node_reconnect_interval: 86400,
            enable_forward: true,
            sync_interval: 10,
            sync_req: 5,
            force_sync_epoch: 100,
            wal_path: "./data/wal_chain".to_string(),
            send_chain_status_interval_sync: 1000,
            health_check_timeout: 300,
            http2_keepalive_interval: 300,
            http2_keepalive_timeout: 10,
            tcp_keepalive: 600,
            enable_metrics: true,
            metrics_port: 60004,
            metrics_buckets: vec![
                0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0,
            ],
            count_per_batch: 1000,
            buffer_duration: 300,
            is_danger: false,
            log_config: Default::default(),
        }
    }
}

impl ControllerConfig {
    pub fn new(config_str: &str) -> Self {
        let mut config: ControllerConfig = read_toml(config_str, "controller");
        config.node_address = fs::read_to_string(config.node_address).unwrap();
        config.validator_address = fs::read_to_string(config.validator_address).unwrap();
        // wal_path must be relative path
        assert!(
            !Path::new(&config.wal_path).is_absolute(),
            "wal_path must be relative path"
        );
        config
    }

    pub fn set_global(self) {
        CONFIG.set(self).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::ControllerConfig;

    #[test]
    fn basic_test() {
        let config = ControllerConfig::new("example/config.toml");

        assert_eq!(config.network_port, 50000);
        assert_eq!(config.consensus_port, 50001);
        assert_eq!(config.storage_port, 50003);
        assert_eq!(config.crypto_port, 50005);
        assert_eq!(config.executor_port, 50002);
        assert_eq!(config.controller_port, 50004);
        assert_eq!(
            config.node_address,
            "c356876e7f4831476f99ea0593b0cd7a6053e4d3".to_string()
        );
        assert_eq!(
            config.validator_address,
            "a0781253f4e4ee3a0cee7a3db9d9772dc75d4fb76be3e9b465284d82b8061c61b18ff6256955f48c441db29d1b997a69".to_string()
        );
    }
}
