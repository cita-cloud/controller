# controller

`CITA-Cloud`中[controller微服务](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/controller.proto)的实现。

## 编译docker镜像

```
docker build -t citacloud/controller .
```

## 使用方法

```shell
$ controller -h
controller 6.6.5
Rivtower Technologies <contact@rivtower.com>

Usage: controller <COMMAND>

Commands:
  run   run this service
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
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
```

参数：

1. 微服务配置文件。

   参见示例`example/config.toml`。

   其中`[controller]`段为微服务本身的配置：
    * `domain` 节点的域名
    * `consensus_port` 共识微服务的gRPC端口
    * `controller_port` 控制器微服务的gRPC端口
    * `executor_port` 执行器微服务的gRPC端口
    * `crypto_port` crypto微服务的gRPC端口
    * `network_port` 网络微服务的gRPC端口
    * `storage_port` 存储微服务的gRPC端口
    * `validator_address_len` 是共识使用的地址的长度
    * `metrics_port` 是metrics信息的导出端口
    * `enable_metrics` 是metrics功能的开关
    * `node_address` 节点地址文件路径

   其中`[controller.log_config]`段为微服务日志的配置：
    * `max_level` 日志等级
    * `filter` 日志过滤配置
    * `service_name` 服务名称，用作日志文件名与日志采集的服务名称
    * `rolling_file_path` 日志文件路径
    * `agent_endpoint` jaeger 采集端地址

   其中`[genesis_block]`段配置创世块相关的信息：
    * `prevhash` 默认全0
    * `timestamp` 创世块的时间戳

   其中`[system_config]`配置初始的系统配置信息：
    * `admin` 管理员账户地址
    * `block_interval` 出块间隔
    * `chain_id` 链的唯一标识
    * `validators` 验证者账户地址列表
    * `version` 协议版本号
    * `block_limit` 交易查重历史区块上限
    * `quota_limit` 每个区块打包的交易`quota`上限

```shell
$ controller run -c example/config.toml
2023-02-22T13:57:22.59095273+08:00  INFO controller: controller grpc port: 50004
2023-02-22T13:57:22.591016492+08:00  INFO controller: health check timeout: 300
2023-02-22T13:57:22.693847752+08:00  INFO controller: network service ready
2023-02-22T13:57:22.695732764+08:00  INFO controller: crypto_sm service ready
2023-02-22T13:57:22.695771017+08:00  INFO controller: load system config
2023-02-22T13:57:23.124056936+08:00  INFO controller: storage service ready, get current height success
2023-02-22T13:57:23.124097583+08:00  INFO controller: this is a new chain
2023-02-22T13:57:23.124812206+08:00  INFO controller: init height: 0, init block hash: 0x123
2023-02-22T13:57:23.126118419+08:00  INFO controller: update system config by file: lock_id: 1000
2023-02-22T13:57:23.126878598+08:00  INFO controller: update system config by file: lock_id: 1001
2023-02-22T13:57:23.127386789+08:00  INFO controller: update system config by file: lock_id: 1002
2023-02-22T13:57:23.127991302+08:00  INFO controller: update system config by file: lock_id: 1003
2023-02-22T13:57:23.128675016+08:00  INFO controller: update system config by file: lock_id: 1004
2023-02-22T13:57:23.129325438+08:00  INFO controller: update system config by file: lock_id: 1005
2023-02-22T13:57:23.130118259+08:00  INFO controller: update system config by file: lock_id: 1006
2023-02-22T13:57:23.130783639+08:00  INFO controller: update system config by file: lock_id: 1007
2023-02-22T13:57:23.131130741+08:00  INFO controller::controller: node address: 0ef9ac94edc6fa717fd0595296acab6af8264951
2023-02-22T13:57:23.131149938+08:00  INFO controller::controller: validator address: 9401ed0969b83bf31a9ca1a302e6f2fb49c6bc69
2023-02-22T13:57:23.248322425+08:00  INFO finalize_block: controller::chain: execute block(0) Success: state_root: 0x123. hash: 0x123
2023-02-22T13:57:23.249649918+08:00  INFO finalize_block: controller::chain: store AllBlockData(0) success: hash: 0x123
2023-02-22T13:57:23.249851923+08:00  INFO finalize_block: controller::chain: finalize block(0) success: pool len: 0, pool quota: 0. hash: 0x123
2023-02-22T13:57:23.24987652+08:00  INFO controller::chain: executor service ready
2023-02-22T13:57:23.250538914+08:00  INFO controller: start controller grpc server
...
```

## 设计

### 概述

`Controller`微服务在整个区块链中处于核心的位置，主导所有主要的流程，并给上层用户提供RPC接口。

接口除了针对`Consensus`微服务的接口，就是针对上层用户的RPC接口。其中最重要的是`SendRawTransaction`发送交易接口，剩下的都是一些信息查询接口。

单独就这个微服务来说，可以认为是一个提案管理系统。用户通过发送交易接口，提交原始交易数据，`Controller`管理这些原始交易数据。通过计算原始交易数据的哈希，组装`CompactBlock`，以及再次哈希，形成`Consensus`需要的提案，管理这些提案。这里所说的管理，包括持久化，同步，以及验证其合法性。

### 主要模块介绍

#### genesis

创世块本质是一个配置文件，里面配置了高度为0的创世块的内容。创世块里只有timestamp和prevhash两项内容，其它配置有另外单独的配置文件。

初始化的时候会读取配置文件，然后生成相应的块结构。

#### system_config

本实现的区块链系统同时支持account和utxo两种类型的交易。但是utxo类型的交易只用于系统配置管理。

初始化时需要一个单独的初始系统配置，给系统配置项赋予初值。

在一个块确认之后，如果发现有utxo类型的交易，会调用本模块的update函数更新系统配置。

#### pool

交易池负责维护尚未确认的交易。本实现就简单的使用HashSet来保存这些交易的hash，另外有一个HashMap给每笔交易赋予一个序号。

交易池直到块最终确认（后面累计block_delay_number个后续块），才会将交易从交易池中删除。

为了尽量避免打包重复的交易，打包交易的时候是随机选择交易的。具体做法是随机选择一个开始的序号，然后往后顺序扫描，直到没有交易，或者交易数量达到打包上限。

#### auth

auth主要负责检查交易的合法性，包括交易中每个字段的合法性，以及与历史交易是否重复。auth会保存最近100个历史块中的交易hash。

对于utxo类型的交易的交易，还要校验prevhash是否正确。

#### chain

chain主要是维护了一个管理未最终确认块的fork_tree。因为链的未确认部分长度是确定的（block_delay_number），因此fork_tree是一个HashMap，以index（块高-已经确认的最高块高）为key。其总长度为block_delay_number * 2 + 2，主要是考虑节点落后的情况下，可以尽量多的同步未来的候选块，加快同步的速度。

本实现比较简单，并没有分叉链之间切换的逻辑。而是在一个块得到确认之后，按照prevhash临时找出对应的分叉链。如果该分叉链比之前的主链要长，则进行替换，并更新一些相关的状态。

如果新的主链长度大于block_delay_number，则多出的部分最老的块得到最终确认，会将相关信息写入storage，并更新当前块高/块hash等信息。

新确认的块有两个来源：一个是共识微服务通过commit_block提交的块，另外一个是同步过来的块。只不过共识提交的确认块在未最终确认的区域，而同步过来的块是已经最终确认的。同步块的处理基本与上述相同，额外的会清理掉当前的主链，因为最新的当前块变化之后，原来的主链肯定跟它连不上了。

候选块除了刚启动初始化时会主动增加，后续是每次有新的块最终确认才会增加新的候选块。同步块的处理没有这个操作，因为处于同步状态意味着本节点落后了，那么主链和候选块很可能是没有意义的。

### 出块实现过程

```mermaid
sequenceDiagram
participant Consensue(leader)
participant Consensue
participant Controller
participant NetWork
Consensue ->> Controller: get_proposal()
Controller -->> Consensue: prepared_proposal
Consensue(leader) ->> Consensue(leader): pending_proposal()
Consensue ->> NetWork: sendMsg(ready)
Consensue ->> Controller: check_proposal(prepared_proposal)
Controller -->> Consensue: sussess
Consensue ->> Controller: commit_block
Controller ->> Controller: finalize_block
Controller ->> storage: store_data()
Controller -->> Consensue: reconfigure(status)
Consensue ->> Consensue: store()
```
