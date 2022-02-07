# controller
## build
```
cargo build --release
```
## build docker image
```
docker build -t citacloud/controller .
```

## related config in config.toml

1. `config.toml`-`[controller]`配置其他微服务的端口以及`key_id`和`node_address`。示例如下：

   ```
   [controller]
   consensus_port = 50001
   controller_port = 50004
   executor_port = 50002
   key_id = 1
   kms_port = 50005
   network_port = 50000
   node_address = '262a554f4a34884aa1c2ec04786349ee9c62fb98'
   package_limit = 30000
   storage_port = 50003
   ```

2. `config.toml`-`[genesis_block]`配置创世块相关的信息。示例如下：

   ```
   [genesis_block]
   prevhash = '0x0000000000000000000000000000000000000000000000000000000000000000'
   timestamp = 1642573944688
   ```

3. `config.toml`-`[system_config]`配置初始的系统配置信息。示例如下：

   ```
   [system_config]
   admin = '5b0618082d6ac17fb755463f9e509bf515d93ae2'
   block_interval = 3
   block_limit = 100
   chain_id = '63586a3c0255f337c77a777ff54f0040b8c388da04f23ecee6bfd4953a6512b4'
   validators = [
      '262a554f4a34884aa1c2ec04786349ee9c62fb98',
      'c377f99f31d42f0709587a13381c8a5c134ecb4c',
   ]
   version = 0
   ```

   

