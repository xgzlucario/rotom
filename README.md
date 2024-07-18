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

rotom 在数据结构上做了许多优化，当 hash 和 set 较小时，使用空间紧凑的 `zipmap` 和 `zipset` 以优化内存效率，并在适时使用 `lz4` 压缩算法压缩较冷数据，以进一步节省内存。

其中 `zipmap` 和 `zipset` 以及 `quicklist` 都基于 `listpack`, 这是 Redis 7.0+ 提出的新型压缩列表，支持正序及逆序遍历。

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

ROTOM

```
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"PING_INLINE","84674.01","0.316","0.136","0.303","0.399","0.567","2.967"
"PING_MBULK","85397.09","0.314","0.120","0.303","0.391","0.583","8.111"
"SET","83472.46","0.326","0.112","0.311","0.407","0.567","6.471"
"GET","87412.59","0.308","0.112","0.295","0.375","0.575","4.479"
"INCR","87032.20","0.313","0.120","0.303","0.383","0.503","5.519"
"LPUSH","35323.21","1.374","0.264","1.351","1.911","2.359","12.783"
"RPUSH","86805.56","0.317","0.104","0.303","0.391","0.575","8.383"
"LPOP","33990.48","1.429","0.280","1.431","1.951","2.423","7.479"
"RPOP","85984.52","0.314","0.160","0.303","0.383","0.551","5.367"
"SADD","86956.52","0.316","0.112","0.303","0.391","0.615","6.143"
"HSET","87183.96","0.319","0.088","0.303","0.391","0.615","7.031"
"SPOP","86281.27","0.313","0.096","0.303","0.383","0.527","9.247"
"ZADD","88495.58","0.314","0.112","0.303","0.383","0.551","6.207"
"ZPOPMIN","86132.64","0.313","0.112","0.303","0.383","0.543","7.135"
"LPUSH (needed to benchmark LRANGE)","34710.17","1.400","0.296","1.351","1.983","2.495","4.583"
"LRANGE_100 (first 100 elements)","18667.16","1.345","0.592","1.343","1.703","1.951","4.975"
"LRANGE_300 (first 300 elements)","9813.54","2.538","0.384","2.535","3.151","3.767","8.311"
"LRANGE_500 (first 500 elements)","6947.34","3.570","0.520","3.527","4.519","5.479","13.783"
"LRANGE_600 (first 600 elements)","5622.08","4.415","0.592","4.335","5.871","7.535","13.663"
"MSET (10 keys)","56947.61","0.531","0.232","0.463","0.959","1.567","7.535"
"XADD","75585.79","0.364","0.096","0.327","0.559","0.943","10.167"
```

REDIS

```
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"PING_INLINE","76394.20","0.341","0.088","0.335","0.439","0.663","2.391"
"PING_MBULK","74349.44","0.349","0.104","0.343","0.455","0.623","3.087"
"SET","77639.75","0.335","0.080","0.327","0.423","0.551","3.079"
"GET","73475.39","0.353","0.080","0.343","0.471","0.631","3.551"
"INCR","75757.57","0.342","0.120","0.335","0.439","0.551","2.511"
"LPUSH","76804.91","0.337","0.096","0.327","0.431","0.567","3.135"
"RPUSH","76863.95","0.338","0.080","0.327","0.431","0.543","2.455"
"LPOP","76628.36","0.339","0.112","0.327","0.431","0.591","2.687"
"RPOP","75642.96","0.344","0.088","0.335","0.439","0.591","3.607"
"SADD","65231.57","0.399","0.096","0.375","0.591","0.831","3.495"
"HSET","71123.76","0.367","0.112","0.351","0.495","0.679","2.959"
"SPOP","74074.07","0.349","0.152","0.335","0.455","0.623","3.591"
"ZADD","74962.52","0.348","0.112","0.335","0.455","0.591","6.383"
"ZPOPMIN","71994.23","0.360","0.096","0.351","0.487","0.687","2.575"
"LPUSH (needed to benchmark LRANGE)","72833.21","0.359","0.104","0.343","0.503","0.711","2.095"
"LRANGE_100 (first 100 elements)","39494.47","0.647","0.192","0.631","0.847","1.079","3.375"
"LRANGE_300 (first 300 elements)","16920.47","1.482","0.296","1.463","1.927","2.263","4.319"
"LRANGE_500 (first 500 elements)","11713.72","2.130","0.408","2.071","2.831","3.439","9.655"
"LRANGE_600 (first 600 elements)","10833.06","2.298","0.432","2.271","2.847","3.247","6.015"
"MSET (10 keys)","84459.46","0.319","0.096","0.311","0.407","0.583","2.663"
"XADD","81433.22","0.323","0.104","0.311","0.415","0.519","2.503"
```