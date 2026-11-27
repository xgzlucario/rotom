# Rotom

![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![tests](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

中文 | [English](README.md)

## 介绍

这里是 rotom，一个使用 Go 编写的高性能，低延迟的 tiny Redis Server。基于 IO 多路复用还原了 Redis 中的 AeLoop 核心事件循环机制。

## 特性

- 还原了 Redis 中的 AeLoop 单线程事件循环，Linux 上基于 epoll，macOS/BSD 上基于 kqueue
- 非常好的优化，达到原生 Redis 的性能
- 兼容 Redis RESP 协议，可以使用任何 redis 客户端连接
- 实现了 String, Hash, Set, List, ZSet 数据结构
- 支持 RDB 和 AOF 持久化，AOF 采用 Redis 7 的 multi-part 结构，支持后台重写
- 支持 20 多种常用命令

## AELoop 事件循环

AeLoop(Async Event Loop) 是 Redis 的核心异步事件驱动机制，主要有以下部分：

![img](asserts/aeloop.png)

1. FileEvent：使用 IO 多路复用处理网络 socket 上的读写事件。事件分为 `READABLE` 和 `WRITABLE`
2. TimeEvent：处理需要延迟执行或定时执行的任务，如每隔 `100ms` 进行过期淘汰
3. 当事件就绪时，通过该事件绑定的回调函数进行处理

在 rotom 内部实现中，还原了 Redis 中的 AeLoop 事件循环机制，具体来说：

1. 当一个新的 tcp 连接到达时，`acceptHandler` 以非阻塞方式接受连接，并注册读事件
2. 读事件就绪时，`readQueryFromClient` 将数据读入客户端的 `queryBuf`
3. `processInputBuffer` 解析并执行 `queryBuf` 中所有完整的命令，回复追加到客户端的回复缓冲区，写命令追加到 AOF 缓冲区
4. 事件循环进入等待前，`beforeSleep` 先写 AOF 缓冲区，再直接把回复写入 socket。只有 socket 缓冲区写不下时才注册写事件，由 `sendReplyToClient` 继续发送

## 数据结构

rotom 在数据结构上做了许多优化：

- dict：移植自 Redis 的 dict，支持渐进式 rehash 和反向二进制 `SCAN` 游标。entry 本身就是桶链表节点，过期时间内联存储
- hash：`zipmap`，所有值紧凑存放在一个字节数组中，另有 field -> offset 索引
- set：较小时使用 `zipset`（基于 listpack），变大后转为哈希集合
- list：`quicklist`，由 listpack 组成的双向链表
- zset：较小时使用 `zipzset`（有序 listpack），变大后转为 map + 带 span 的跳表（rank 和区间查询 O(log n)）

`listpack` 是 Redis 用来替代 `ziplist` 的紧凑列表：每个 entry 的长度在两端都有记录，可以双向遍历，也不存在级联更新问题。

## 代码结构

```
main.go                 参数、日志、启动
internal/
  ae/        事件循环（linux 上用 epoll，darwin/bsd 上用 kqueue）
  net/       非阻塞 socket 系统调用
  resp/      RESP 请求解析与回复写入（零分配）
  server/    客户端、请求处理、AOF/RDB 接入
  command/   命令表与命令实现
  db/        键空间：dict、过期、值类型、RDB
  aof/       multi-part AOF 与重写
  codec/     RDB 二进制编码
  listpack/  hash/  list/  set/  zset/     数据结构
  config/    toml 配置
  fileutil/  原子写文件
```

## 持久化

开启 `appendonly = true` 后，AOF 是一个目录，结构与 Redis 7 的 multi-part AOF 相同：

```
appendonlydir/
  appendonly.aof.manifest      下面这些文件及其加载顺序
  appendonly.aof.3.base.rdb    上次重写生成的快照
  appendonly.aof.7.incr.aof    快照之后写入的命令
```

执行 `BGREWRITEAOF`，或者 AOF 增长超过 `auto-aof-rewrite-percentage` / `auto-aof-rewrite-min-size` 时，会触发重写，不需要 fork：先同步编码数据集，同时把后续写命令切到新的 incr 文件；快照在后台写盘，确认落盘后再切换 manifest。任何时刻崩溃，manifest 指向的文件组合都是完整的数据集。

编码快照这一步会阻塞事件循环，大约每 10 万个 key 30ms，而 Redis 的 fork 几乎不阻塞。最后一个 incr 文件末尾如果有崩溃时写了一半的命令，加载时会自动截掉。旧版本的单文件 `appendonly.aof` 启动时会被自动移进目录。

## 性能

![img](asserts/bench.jpg)

测试将在同一台机器上，关闭 `appendonly`，并使用 `redis-benchmark` 工具测试不同命令的 qps。

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/rotom
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

## 计划

- LRU 缓存及内存淘汰支持
- 兼容更多常用命令

## 使用

**本机运行**

首先克隆项目到本地：

```bash
git clone https://github.com/xgzlucario/rotom
```

确保本地 golang 环境 `>= 1.22`，在项目目录下执行 `go run .` 启动服务，默认监听 `6379` 端口：

```
$ go run .
2024-07-18 23:37:13 DBG 
 ________      _____                  
 ___  __ \_______  /_____________ ___   Rotom 64 bit (amd64/linux)
 __  /_/ /  __ \  __/  __ \_  __ '__ \  Port: 6379, Pid: 15817
 _  _, _// /_/ / /_ / /_/ /  / / / / /  Build: 
 /_/ |_| \____/\__/ \____//_/ /_/ /_/

2024-07-18 23:37:13 INF read config file config=config.json
2024-07-18 23:37:13 INF rotom server is ready to accept.
```

**容器运行**

或者你也可以使用容器运行，首先运行 `make build-docker` 打包：

```
REPOSITORY       TAG           IMAGE ID       CREATED         SIZE
rotom            latest        0cd91943967a   5 seconds ago   20.9MB
```

然后启动容器：

```bash
docker run --rm -p 6379:6379 --name rotom rotom:latest
```