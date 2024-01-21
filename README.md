# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.21.0-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![Test and coverage](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml)

English | [中文](README_ZN.md) | [doc](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 📃Introduction

Welcome to Rotom, an embedded high-performance Key-Value in-memory database written in Go, has many built-in data types, Support for persistence and data recovery.

Currently features:

1. Built-in data types String, Map, Set, List, ZSet, and BitMap.
2. Second level ttl supported for each key-value pair.
3. Based on [GigaCache](https://github.com/xgzlucario/GigaCache), which is managing GB-level data, saving about 40% of memory compared to `stdmap`, with better performance and reduced GC overhead.
4. Internal encoding/decoding lib that more effective than `protobuf`.
5. Persistent log support, and can recover database from logs.

If you want to know more technical details, check out [doc](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc).

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

```bash
========== Set ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes
cost: 1.120466957s
qps: 892467.60
50th: 990 ns
90th: 1107 ns
99th: 1724 ns

========== SetEx ==========
size: 100*10000 enties
desc: key 10 bytes, value 10 bytes, ttl 1min
cost: 1.125406833s
qps: 888551.77
50th: 986 ns
90th: 1092 ns
99th: 2110 ns

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

========== LRPush ==========
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
desc: field 10 bytes, incr float64
cost: 1.321257444s
qps: 756843.75
50th: 1121 ns
90th: 1248 ns
99th: 2025 ns
```

