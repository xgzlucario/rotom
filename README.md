# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.21.0-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

English | [中文](README_ZN.md) | [doc](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 📃Introduction

This is Rotom, a stand-alone high-performance Key-Value memory database written in Go. It has built-in multiple commonly used data types, supports persistent storage.

Currently supported features:

1. Built-in data types like String, Map, Set, List, ZSet, BitMap, etc., supporting more than 20 commands.
2. Nanosecond level expiration time supported.
3. Based on [GigaCache](https://github.com/xgzlucario/GigaCache), supports concurrency and avoids GC overhead.
4. RDB + AOF hybrid persistence strategy.
5. Use zstd algorithm to compress log files with a compression ratio of 10:1.

If you want to know more technical details about Rotom, please check out the [doc](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc).

## 🚚Usage

Before using, please install Rotom into your project first:
```bash
go get github.com/xgzlucario/rotom
```
And install the gofakeit library for generating some random data:
```bash
go get github.com/brianvoe/gofakeit/v6
```
Run the sample program:
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
## 🚀Performance

Rotom has super multi-threading performance. The following is the bench test data.

### Test Environment

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/GigaCache
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

### Benchmark

```shell
========== Set ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 337.164876ms
50th: 242 ns
90th: 307 ns
99th: 905 ns
db file size: 838.5KB

========== Set 8 parallel ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 192.458463ms
50th: 358 ns
90th: 1373 ns
99th: 29922 ns
db file size: 4.1MB

========== SetEx ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes, ttl 1min
cost: 349.705129ms
50th: 246 ns
90th: 310 ns
99th: 935 ns
db file size: 1.9MB

========== Get ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 295.173803ms
50th: 237 ns
90th: 321 ns
99th: 593 ns

========== Get 8 parallel ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 43.818633ms
50th: 272 ns
90th: 423 ns
99th: 659 ns

========== LRPush ==========
size: 100*10000 enties
desc: value 10 bytes
cost: 226.683466ms
50th: 191 ns
90th: 210 ns
99th: 395 ns
db file size: 277.9KB

========== HSet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 386.041763ms
50th: 215 ns
90th: 282 ns
99th: 493 ns
db file size: 835.2KB

========== HGet ==========
size: 100*10000 enties
desc: field 10 bytes, value 10 bytes
cost: 277.690778ms
50th: 220 ns
90th: 304 ns
99th: 572 ns

========== BitSet ==========
size: 100*10000 enties
desc: offset uint32
cost: 117.428111ms
50th: 97 ns
90th: 101 ns
99th: 121 ns
db file size: 907.3KB
```

