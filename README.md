# controller

## build

```
cargo build --release
```

## build docker image

```
docker build -t citacloud/controller .
```

## install

```
cargo install --path ./
```

## related config in config.toml

1. `config.toml`-`[controller]`配置其他微服务的端口以及`key_id`和`node_address`。示例如下：

   ```toml
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

   ```toml
   [genesis_block]
   prevhash = '0x0000000000000000000000000000000000000000000000000000000000000000'
   timestamp = 1642573944688
   ```

3. `config.toml`-`[system_config]`配置初始的系统配置信息。示例如下：

   ```toml
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

## 使用方法

```shell
$ controller -h
controller 6.3.2
Rivtower Technologies.
This doc string acts as a help message when the user runs '--help' as do all doc strings on fields

USAGE:
    controller <SUBCOMMAND>

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    git     print information from git
    help    Print this message or the help of the given subcommand(s)
    run     run this service
```

### controller git

打印`git`相关的信息

```shell
$ controller git
git version: 8fedf45-modified
homepage: https://github.com/cita-cloud/controller
```

### controller run

运行`controller`服务。

```shell
$ controller run -h
controller-run 
run this service

USAGE:
    controller run [OPTIONS]

OPTIONS:
    -c, --config <CONFIG_PATH>    Chain config path [default: config.toml]
    -h, --help                    Print help information
    -l, --log <LOG_FILE>          log config path [default: controller-log4rs.yaml]
```

参数：
1. 微服务配置文件。

    参见示例`example/config.toml`。

    其中：
    * `consensus_port` 是共识微服务的gRPC端口
    * `controller_port` 是控制器微服务的gRPC端口
    * `executor_port` 是执行器微服务的gRPC端口
    * `kms_port` 是kms微服务的gRPC端口
    * `network_port` 是网络微服务的gRPC端口
    * `storage_port` 是存储微服务的gRPC端口

2. 日志配置文件。

    参见示例`controller-log4rs.yaml`。

    其中：

    * `level` 为日志等级。可选项有：`Error`，`Warn`，`Info`，`Debug`，`Trace`，默认为`Info`。
    * `appenders` 为输出选项，类型为一个数组。可选项有：标准输出(`stdout`)和滚动的日志文件（`journey-service`），默认为同时输出到两个地方。

```shell
$ controller run -c example/config.toml -l controller-log4rs.yaml
2022-03-09T11:53:21.015943+08:00 INFO controller - grpc port of this service: 50004
2022-03-09T11:53:24.023215+08:00 INFO controller - register network msg handler success!
2022-03-09T11:53:24.025524+08:00 INFO controller - kms(sm) is ready!
2022-03-09T11:53:24.025583+08:00 INFO controller - load sys_config
2022-03-09T11:53:24.028314+08:00 INFO controller - get current block number success!
2022-03-09T11:53:24.028346+08:00 INFO controller - this is a new chain!
2022-03-09T11:53:24.028927+08:00 INFO controller - current block number: 0, current block hash: ...
2022-03-09T11:53:24.029761+08:00 INFO controller - lock_id: 1000 empty, store from config to local
2022-03-09T11:53:24.030727+08:00 INFO controller - lock_id: 1001 empty, store from config to local
2022-03-09T11:53:24.031320+08:00 INFO controller - lock_id: 1002 empty, store from config to local
2022-03-09T11:53:24.031872+08:00 INFO controller - lock_id: 1003 empty, store from config to local
2022-03-09T11:53:24.032435+08:00 INFO controller - lock_id: 1004 empty, store from config to local
2022-03-09T11:53:24.033142+08:00 INFO controller - lock_id: 1005 empty, store from config to local
2022-03-09T11:53:24.033722+08:00 INFO controller - lock_id: 1006 empty, store from config to local
2022-03-09T11:53:24.034156+08:00 INFO controller - load sys_config complete
2022-03-09T11:53:24.034186+08:00 INFO controller::controller - node address: ...
2022-03-09T11:53:24.034560+08:00 INFO controller::chain - finalize genesis block
2022-03-09T11:53:24.038520+08:00 INFO controller::chain - exec_block(0): status: Success, executed_block_hash: [...]
2022-03-09T11:53:24.040881+08:00 INFO controller::chain - finalize_block: 0, block_hash: ...
2022-03-09T11:53:24.040900+08:00 INFO controller::chain - executor is ready!
2022-03-09T11:53:24.041611+08:00 INFO controller::controller - init_block_number: 0
2022-03-09T11:53:24.043737+08:00 INFO controller::controller - reconfigure consensus success!
2022-03-09T11:53:24.043782+08:00 INFO controller - start grpc server!
...
```

## 设计

`Controller`微服务在整个区块链中处于核心的位置，主导所有主要的流程，并给上层用户提供RPC接口。

接口除了前述的针对`Consensus`微服务的接口，就是针对上层用户的RPC接口。其中最重要的是`SendRawTransaction`发送交易接口，剩下的都是一些信息查询接口。

单独就这个微服务来说，可以认为是一个提案管理系统。用户通过发送交易接口，提交原始交易数据，`Controller`管理这些原始交易数据。通过计算原始交易数据的哈希，组装`CompactBlock`，以及再次哈希，形成`Consensus`需要的提案，管理这些提案。这里所说的管理，包括持久化，同步，以及验证其合法性。

`Blockchain.proto`文件中定义了一套交易和块的数据结构，但是前面所述的从原始交易数据如何产生最终`Consensus`需要的提案，并且这个过程还是要可验证的，这些都由具体实现决定。未来我们会提供一个框架，方便用户自定义整个流程，甚至是自定义交易和块等核心数据结构。

参见[项目wiki](https://github.com/cita-cloud/controller/wiki)
