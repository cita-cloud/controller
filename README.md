# controller poc
## build
```
cargo build --release
```
## build docker image
```
docker build -t citacloud/controller_poc .
```

## configure files

1. `controller-config.toml`配置其他微服务的端口以及`block_delay_number`。示例如下：

   ```toml
   network_port = 50000
   consensus_port = 50001
   storage_port = 50003
   kms_port = 50005
   executor_port = 50002
   block_delay_number = 6
   ```

2. `genesis.toml`配置创世块相关的信息。示例如下：

   ```toml
   timestamp = 1596109880468
   prevhash = "0x0000000000000000000000000000000000000000000000000000000000000000"
   ```

3. `init_sys_config.toml`配置初始的系统配置信息。示例如下：

   ```toml
   version = 0
   chain_id = "0x68747470733a2f2f63646e2e636974616875622e636f6d2f69636f6e5f636974"
   admin = "0xffffffffffffffffffffffffffffffffff020004"
   block_interval = 6
   validators = ["0xffffffffffffffffffffffffffffffffff020005", "0xffffffffffffffffffffffffffffffffff020006"]
   ```

   

