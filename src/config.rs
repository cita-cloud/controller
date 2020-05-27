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

use serde_derive::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct ControllerConfig {
    pub network_port: u16,
    pub consensus_port: u16,
    pub storage_port: u16,
    pub kms_port: u16,
    pub executor_port: u16,
    pub block_delay_number: u32,
}

impl ControllerConfig {
    pub fn new(config_str: &str) -> Self {
        toml::from_str::<ControllerConfig>(config_str).expect("Error while parsing config")
    }
}

#[cfg(test)]
mod tests {
    use super::NetConfig;

    #[test]
    fn basic_test() {
        let toml_str = r#"
        network_port = 50000
        consensus_port = 50001
        storage_port = 50003
        kms_port = 50005
        executor_port = 50002
        block_delay_number = 6
        "#;

        let config = ControllerConfig::new(toml_str);

        assert_eq!(config.network_port, 50000);
        assert_eq!(config.consensus_port, 50001);
        assert_eq!(config.storage_port, 50003);
        assert_eq!(config.kms_port, 50005);
        assert_eq!(config.executor_port, 50002);
        assert_eq!(config.block_delay_number, 6);
    }
}
