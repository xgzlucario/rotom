# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.22-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

## 介绍

你好，这里是 rotom，一个使用 Go 编写的 tiny Redis Server。 在 `v2` 版本中，项目废弃了之前版本的内嵌形式，后续将以 `net server` 的形式维护下去，一方面是为了在实践中学习 Linux 网络编程，另一方面也是为了兼容社区中大量成熟的 redis 相关工具来辅助开发。

实现特性：

1. 基于 single epoll server 的网络 IO 框架
2. 兼容 Redis RESP 通信协议，你可以使用任何 redis 客户端连接 rotom
3. DB hashmap 基于 [GigaCache](https://github.com/xgzlucario/GigaCache)
4. AOF 支持
5. 目前仅支持部分命令如 `ping`, `set`, `get`, `hset`, `hget`

目前的精力主要放在最有意思的框架设计上，短期内不会兼容更多的 RESP 命令。

## 使用

首先克隆项目到本地：

```bash
git clone https://github.com/xgzlucario/rotom
```

确保本地 golang 环境 `>= 1.22`，在项目目录下执行 `go run .` 启动服务，默认监听 `6969` 端口：

```
$ go run .
2024/06/04 17:53:09 read config file: {
    "port": 6969,
    "appendonly": false,
    "appendfilename": "appendonly.aof"
}
2024/06/04 17:53:09 rotom server is ready to accept.
```

## 性能测试

测试将在同一台机器上运行 rotom，关闭 `appendonly`，并使用官方 `redis-benchmark` 工具测试不同命令的耗时。

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/rotom
cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
```

SET

```bash
$ redis-benchmark -t set -p 6969
====== SET ======
  100000 requests completed in 0.99 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.00% <= 0.1 milliseconds
0.02% <= 0.2 milliseconds
96.59% <= 0.3 milliseconds
97.43% <= 0.4 milliseconds
98.82% <= 0.5 milliseconds
99.80% <= 0.6 milliseconds
99.87% <= 0.7 milliseconds
99.95% <= 0.8 milliseconds
99.97% <= 0.9 milliseconds
99.98% <= 1.0 milliseconds
100.00% <= 1.1 milliseconds
101214.58 requests per second
```

GET

```bash
$ redis-benchmark -t get -p 6969
====== GET ======
  100000 requests completed in 0.99 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.00% <= 0.1 milliseconds
0.02% <= 0.2 milliseconds
97.46% <= 0.3 milliseconds
98.53% <= 0.4 milliseconds
99.61% <= 0.5 milliseconds
99.79% <= 0.6 milliseconds
99.88% <= 0.7 milliseconds
99.95% <= 0.9 milliseconds
99.98% <= 1.3 milliseconds
100.00% <= 1.3 milliseconds
101522.84 requests per second
```

HSET

```bash
$ redis-benchmark -t hset -p 6969
====== HSET ======
  100000 requests completed in 1.00 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  multi-thread: no

0.00% <= 0.2 milliseconds
97.90% <= 0.3 milliseconds
98.54% <= 0.4 milliseconds
99.20% <= 0.5 milliseconds
99.63% <= 0.6 milliseconds
99.72% <= 0.7 milliseconds
99.88% <= 0.8 milliseconds
99.93% <= 0.9 milliseconds
99.95% <= 1.0 milliseconds
99.97% <= 1.4 milliseconds
100.00% <= 1.4 milliseconds
100300.91 requests per second
```