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

use cloud_util::common::read_toml;
use serde_derive::Deserialize;
use tokio::sync::OnceCell;

pub static CONFIG: OnceCell<ControllerConfig> = OnceCell::const_new();

pub fn controller_config() -> &'static ControllerConfig {
    CONFIG.get().unwrap()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ControllerConfig {
    pub controller_port: u16,

    pub network_port: u16,

    pub consensus_port: u16,

    pub storage_port: u16,

    pub kms_port: u16,

    pub executor_port: u16,

    pub node_address: String,

    pub block_limit: u64,

    pub package_limit: u64,

    pub address_len: u32,

    pub hash_len: u32,

    pub signature_len: u32,

    pub key_id: u64,

    pub server_retry_interval: u64,

    pub origin_node_reconnect_interval: u64,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            controller_port: 50004,
            network_port: 50000,
            consensus_port: 50001,
            storage_port: 50003,
            kms_port: 50005,
            executor_port: 50002,
            node_address: "".to_string(),
            block_limit: 0,
            package_limit: 30000,
            address_len: 20,
            hash_len: 32,
            signature_len: 128,
            key_id: 0,
            server_retry_interval: 3,
            origin_node_reconnect_interval: 3600,
        }
    }
}

impl ControllerConfig {
    pub fn new(config_str: &str) -> Self {
        read_toml(config_str, "controller")
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
        assert_eq!(config.kms_port, 50005);
        assert_eq!(config.executor_port, 50002);
        assert_eq!(config.controller_port, 50004);
        assert_eq!(
            config.node_address,
            "0x37d1c7449bfe76fe9c445e626da06265e9377601".to_string()
        );
    }
}
