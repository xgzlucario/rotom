# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.22-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

[English](README.md) | 中文 | [在线文档](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 📃介绍

这里是 rotom，一个使用 Go 编写的嵌入式高性能 key-value 内存数据库，内置多种数据类型，支持持久化存储以及数据恢复。

目前实现的特性：

1. 内置数据类型 `string`，`map`，`set`，`list`，`zset`，`bitmap`
2. 每个键值对独立的、秒级的过期时间支持
3. 底层 hashmap 基于 [GigaCache](https://github.com/xgzlucario/GigaCache)，可以管理 GB 级别的数据量，比 `stdmap` 节省约 50% 的内存，性能更强，GC开销更小
4. 内置编解码库，比 protobuf 性能更好
5. 支持持久化日志，以及根据日志恢复数据库

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
	db, err := rotom.Open(rotom.DefaultOptions)
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

Rotom 具有超强的性能，下面是一些功能的测试结果。

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
cost: 1.120466957s
qps: 892467.60
50th: 990 ns
90th: 1107 ns
99th: 1724 ns

========== BatchSet ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes, 100 key-values a batch
cost: 377.933308ms
qps: 2645850.27
50th: 20691 ns
90th: 32950 ns
99th: 95645 ns

========== Get ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 255.023737ms
qps: 3920845.99
50th: 212 ns
90th: 267 ns
99th: 532 ns

========== Get 8 parallel ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 138.089874ms
qps: 7240728.26
50th: 173 ns
90th: 261 ns
99th: 501 ns

========== RPush ==========
size: 100*10000 enties
desc: value 10 bytes
cost: 1.033844185s
qps: 967248.36
50th: 936 ns
90th: 1011 ns
99th: 1609 ns

========== HSet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 1.182939928s
qps: 845337.51
50th: 987 ns
90th: 1117 ns
99th: 1850 ns

========== BatchHSet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes, 100 key-values a batch
cost: 365.036647ms
qps: 2739329.61
50th: 16900 ns
90th: 41312 ns
99th: 89803 ns

========== HGet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 292.57105ms
qps: 3417634.25
50th: 220 ns
90th: 306 ns
99th: 575 ns

========== BitSet ==========
size: 100*10000 enties
desc: offset uint32
cost: 916.172391ms
qps: 1091477.02
50th: 833 ns
90th: 880 ns
99th: 1219 ns

========== ZSet ==========
size: 100*10000 enties
desc: field 10 bytes, incr int64
cost: 1.209290877s
qps: 826918.57
50th: 1038 ns
90th: 1192 ns
99th: 2257 ns
```

