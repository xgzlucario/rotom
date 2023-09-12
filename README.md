# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.21.0-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) 

## 介绍

​		这里是 Rotom，一个 Go 编写的类似于 Redis 的支持多线程的高性能 Key-Value 内存数据库，内置数据类型 String，Map，Set，List，ZSet，BitMap 等，支持持久化存储，可以在 Golang 中以包引入的方式使用，也可以作为网络服务器使用（暂不支持所有命令）。

目前支持的功能：

1. 支持 Set, SetTX, HSet, BitSet 等二十多种命令
2. 微秒级别的过期时间（ttl）
3. 底层基于 [GigaCache](https://github.com/xgzlucario/GigaCache)，能够规避GC开销，比 stdmap 性能更强，支持多线程
4. 基于 RDB + AOF 混合的持久化策略
5. 支持**包引入**或**服务器**启动

## 如何使用

在使用之前，请先安装 Rotom 到你的项目中：
```bash
go get github.com/xgzlucario/rotom
```
并安装 gofakeit 库，用于生成一些随机数据：
```bash
go get github.com/brianvoe/gofakeit/v6
```
运行示例程序：
```go
package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
)

func main() {
	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Set
	for i := 0; i < 10000; i++ {
		phone := gofakeit.Phone()
        user := []byte(gofakeit.Username())
		// Set bytes
		db.Set(phone, user)
		// Or set with ttl
		db.SetEx(phone, user, time.Minute)
		// Or set with deadline
		db.SetTx(phone, user, time.Now().Add(time.Minute).UnixNano())
	}
    
    fmt.Println("now db length is", db.Stat().Len)

	// Get
	key := gofakeit.Phone()
	user, ttl, ok := db.Get(key)
	if ok {
        // ...
	}
}
```
## 性能

Rotom 具有超强的多线程性能，比 Redis 快数倍。

### 测试环境

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/GigaCache
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

### Rotom

使用 200 个 clients 插入共 100 万数据，1.11s 完成，qps 达到 89.5 万。

```bash
$ go run client/client.go
1000000 requests cost: 1.116634772s
qps: 895467.92 req/sec
```

### Redis

使用 200 个 clients 插入共 100 万数据，使用 8 个线程，4.26s 完成，qps 23.5 万。

```bash
$ redis-benchmark -t set -r 100000000 -n 1000000 -c 200 --threads 8
====== SET ======
  1000000 requests completed in 4.26 seconds
  200 parallel clients
  3 bytes payload
  keep alive: 1
  host configuration "save": 3600 1 300 100 60 10000
  host configuration "appendonly": no
  multi-thread: yes
  threads: 8
  
Summary:
  throughput summary: 234962.41 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.823     0.040     0.783     1.247     1.623     8.407
```

