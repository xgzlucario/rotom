# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.22-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

## 介绍

这里是 rotom，一个使用 Go 编写的 tiny Redis Server。基于 IO 多路复用还原了 Redis 中的 AeLoop 核心事件循环机制。

### 实现特性

1. 基于 epoll 网络模型，还原了 Redis 中的 AeLoop 单线程事件循环
2. 兼容 Redis RESP 协议，你可以使用任何 redis 客户端连接 rotom
3. 实现了 dict, quicklist(listpack), hash(map, zipmap), set(mapset, zipset), zset 数据结构
4. AOF 支持
5. 支持 18 种常用命令

### AELoop 事件循环

AeLoop(Async Event Loop) 是 Redis 的核心异步事件驱动机制，主要有以下部分：

1. FileEvent：使用 IO 多路复用处理网络 socket 上的读写事件。事件类型分为 `READABLE` 和 `WRIABLE`
2. TimeEvent：处理需要延迟执行或定时执行的任务，如每隔 `100ms` 进行过期淘汰
3. 当事件就绪时，通过该事件绑定的回调函数进行处理

在 rotom 内部实现中，还原了 Redis 中的 AeLoop 事件循环机制，具体来说：

1. 当一个新的 tcp 连接到达时，通过 `AcceptHandler` 获取连接的 socket fd，并添加至事件循环，注册读事件
2. 读事件就绪时，通过 `ReadQueryFromClient` 将缓冲数据读出至 `queryBuf`
3. 通过 `ProcessQueryBuf` 从 `queryBuf` 中解析并执行对应命令
4. 保存命令执行结果，并注册 socket fd 的写事件
5. 写事件就绪时，通过 `SendReplyToClient` 将所有结果写回客户端，一个写事件可能一次性写回多个读事件的结果
6. 资源释放，并不断循环上述过程，直到服务关闭

### 数据结构

rotom 在数据结构上做了许多优化，当 hash 和 set 较小时，使用空间紧凑的 `zipmap` 和 `zipset` 以优化内存效率。它们都基于 `listpack`, 这是 Redis 5.0+ 提出的新型压缩列表，支持正序及逆序遍历。

### 计划

- LRU 缓存及内存淘汰支持
- dict 渐进式哈希支持
- RDB 及 AOF Rewrite 支持
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
2024-07-18 23:37:13 INF current version buildTime=240718_233649+0800
2024-07-18 23:37:13 INF read cmd arguments config=/etc/rotom/config.json debug=false
2024-07-18 23:37:13 INF running on port=6379
2024-07-18 23:37:13 INF rotom server is ready to accept.
```

**容器运行**

或者你也可以使用容器运行，首先运行 `make build-docker` 打包：

```
REPOSITORY       TAG           IMAGE ID       CREATED         SIZE
rotom            latest        22f42ce9ae0e   8 seconds ago   20.5MB
```

然后启动容器：

```bash
docker run --rm -p 6379:6379 --name rotom rotom:latest
```

## Benchmark

测试将在同一台机器上运行 rotom，关闭 `appendonly`，并使用官方 `redis-benchmark` 工具测试不同命令的耗时。

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/rotom/dict
cpu: AMD Ryzen 7 5800H with Radeon Graphics
```

测试命令

```bash
redis-benchmark --csv
```

```
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"PING_INLINE","259067.36","0.102","0.024","0.103","0.119","0.199","1.855"
"PING_MBULK","262467.19","0.101","0.032","0.095","0.127","0.199","2.559"
"SET","266666.66","0.100","0.024","0.095","0.143","0.239","1.495"
"GET","261780.11","0.102","0.024","0.095","0.143","0.271","1.343"
"INCR","280898.88","0.095","0.032","0.095","0.119","0.255","0.999"
"LPUSH","286532.94","0.095","0.024","0.095","0.127","0.279","1.359"
"RPUSH","309597.50","0.089","0.032","0.087","0.119","0.223","1.839"
"LPOP","273224.03","0.097","0.032","0.095","0.119","0.191","1.855"
"RPOP","278551.53","0.094","0.032","0.095","0.111","0.183","1.303"
"SADD","281690.16","0.094","0.032","0.095","0.111","0.207","4.911"
"HSET","289017.34","0.092","0.024","0.087","0.111","0.207","3.447"
"SPOP","280112.06","0.095","0.024","0.095","0.119","0.215","1.559"
"ZADD","289855.06","0.091","0.024","0.087","0.111","0.207","0.983"
"ZPOPMIN","273224.03","0.097","0.032","0.095","0.111","0.175","0.975"
"LPUSH (needed to benchmark LRANGE)","292397.66","0.092","0.032","0.087","0.111","0.191","0.775"
"LRANGE_100 (first 100 elements)","42863.27","0.581","0.080","0.591","0.735","0.839","2.999"
"LRANGE_300 (first 300 elements)","20973.15","1.189","0.088","1.191","1.583","1.831","4.479"
"LRANGE_500 (first 500 elements)","13970.38","1.774","0.088","1.767","2.279","2.695","7.231"
"LRANGE_600 (first 600 elements)","11764.71","2.112","0.088","2.095","2.831","3.407","10.127"
"MSET (10 keys)","268096.53","0.103","0.024","0.095","0.167","0.279","1.079"
"XADD","261096.61","0.101","0.032","0.095","0.127","0.231","1.087"
```

```bash
redis-benchmark --csv -P 10
```

```
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"PING_INLINE","1851851.75","0.142","0.064","0.143","0.175","0.215","1.511"
"PING_MBULK","1470588.12","0.177","0.080","0.175","0.239","0.463","1.543"
"SET","1724138.00","0.154","0.048","0.143","0.207","0.399","1.391"
"GET","1538461.62","0.169","0.056","0.167","0.231","0.343","0.831"
"INCR","2380952.50","0.118","0.048","0.111","0.223","0.367","0.479"
"LPUSH","2127659.75","0.182","0.088","0.159","0.327","0.495","0.855"
"RPUSH","1587301.50","0.290","0.080","0.279","0.463","0.615","0.951"
"LPOP","1886792.50","0.141","0.048","0.135","0.175","0.279","1.903"
"RPOP","1851851.75","0.140","0.064","0.135","0.175","0.255","0.775"
"SADD","2272727.25","0.122","0.048","0.111","0.183","0.367","1.951"
"HSET","2000000.00","0.228","0.080","0.207","0.335","0.583","3.015"
"SPOP","2173913.00","0.123","0.048","0.111","0.167","0.271","1.175"
"ZADD","2173913.00","0.201","0.080","0.175","0.359","0.703","5.639"
"ZPOPMIN","1515151.50","0.170","0.064","0.167","0.239","0.399","0.575"
"LPUSH (needed to benchmark LRANGE)","2083333.38","0.204","0.080","0.191","0.351","0.543","2.599"
"LRANGE_100 (first 100 elements)","76452.60","3.291","0.096","3.239","4.735","6.407","8.839"
"LRANGE_300 (first 300 elements)","26136.96","8.855","0.136","8.911","15.191","17.007","21.103"
"LRANGE_500 (first 500 elements)","20251.11","9.431","0.224","9.175","15.863","20.319","22.911"
"LRANGE_600 (first 600 elements)","14214.64","11.956","0.248","12.223","19.663","22.895","29.983"
"MSET (10 keys)","1449275.38","0.288","0.064","0.215","0.567","0.895","4.439"
"XADD","1694915.25","0.149","0.048","0.151","0.191","0.271","0.575"
```