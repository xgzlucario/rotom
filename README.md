# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/badge/go-1.21.0-orange.svg) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) 

English | [中文](README_ZN.md) | [doc](https://www.yuque.com/1ucario/devdoc/ntyyeekkxu8apngd?singleDoc)

## 📃Introduction

​	This is Rotom, a high-performance Key-Value memory database written in Go. Its multithreaded performance is several times better than Redis. It has built-in multiple commonly used data types, supports persistent storage, and can be used in Golang as an imported package or as a server (the client part is under development and does not support all commands yet).

Features:

1. Built-in data types like String, Map, Set, List, ZSet, BitMap, etc., supporting more than 20 commands.
2. Microsecond-level expiration time (ttl).
3. Based on [GigaCache](https://github.com/xgzlucario/GigaCache), it can avoid GC overhead and have stronger multithreaded performance.
4. RDB + AOF hybrid persistence strategy.
5. Supports being **imported** or **server** startup.

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
    
	// Get
	key := gofakeit.Phone()
	user, ttl, ok := db.Get(key)
	// ...
}
```
Or start as a **server** and listen to port 7676:

```go
package main

import (
	"net/http"
	"github.com/xgzlucario/rotom/store"
)

func main() {
	go http.ListenAndServe("localhost:6060", nil)

	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}

	if err := db.Listen("0.0.0.0:7676"); err != nil {
		panic(err)
	}
}
```

## 🚀Performance

Rotom has very fast performance, which is several times faster than Redis.

### Test Environment

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/GigaCache
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

### Rotom

200 clients inserting a total of 1 million data, completed in 663ms, reaching a qps of 1.5 million, P99 latency is 1.2ms.

```bash
$ go run client/*.go
1000000 requests cost: 663.97797ms
[qps] 1506028.48 req/sec
[latency] avg: 119.645µs | min: 4.052µs | p50: 49.464µs | p95: 425.006µs | p99: 1.195428ms | max: 17.713702ms
```

### Redis

200 clients inserting a total of 1 million data, using 8 threads, completed in 4.26s, reaching a qps of 235,000, P99 latency is 1.6ms.

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

