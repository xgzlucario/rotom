# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.21.0-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

[English](README.md) | 中文 | [在线文档](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 📃介绍

这里是 Rotom，一个使用 Golang 编写单机高性能 Key-Value 内存数据库，内置多种常用数据类型，支持持久化存储。

目前支持的功能：

1. 内置数据类型 String，Map，Set，List，ZSet，BitMap 等，支持 20 多种命令
2. 支持纳秒级别的过期时间
3. 底层基于 [GigaCache](https://github.com/xgzlucario/GigaCache)，支持并发，规避GC开销
4. 基于 RDB + AOF 混合的持久化策略
5. 使用 zstd 算法压缩日志文件，压缩比达到 10:1

如果你想了解更多关于 Rotom 的技术细节，请查看 [在线文档](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 🚚如何使用

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
	"github.com/xgzlucario/rotom"
)

func main() {
	db, err := rotom.Open(rotom.DefaultConfig)
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
    
	// Get
	key := gofakeit.Phone()
	user, ttl, ok := db.Get(key)
	// ...
}
```
## 🚀性能

Rotom 具有超强的多线程性能，以下是压测数据。

### 测试环境

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/GigaCache
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

### Benchmark

下面是部分命令的测试结果。

```bash
========== Set ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 412.600461ms
50th: 245 ns
90th: 304 ns
99th: 913 ns
db file size: 1.1MB

========== Set 8 parallel ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 194.788605ms
50th: 348 ns
90th: 811 ns
99th: 18142 ns
db file size: 4.2MB

========== SetEx ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes, ttl 1min
cost: 442.300466ms
50th: 261 ns
90th: 324 ns
99th: 1005 ns
db file size: 3.0MB

========== Get ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 354.636607ms
50th: 231 ns
90th: 294 ns
99th: 562 ns

========== Get 8 parallel ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 64.410025ms
50th: 249 ns
90th: 347 ns
99th: 595 ns

========== HSet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 498.104681ms
50th: 225 ns
90th: 279 ns
99th: 452 ns
db file size: 823.2KB

========== HGet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 318.662069ms
50th: 213 ns
90th: 250 ns
99th: 536 ns

========== BitSet ==========
size: 100*10000 enties
desc: offset uint32
cost: 171.415936ms
50th: 99 ns
90th: 102 ns
99th: 119 ns
db file size: 895.1KB
```

