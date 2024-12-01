# Rotom

[![Go Report Card](https://goreportcard.com/badge/github.com/xgzlucario/rotom)](https://goreportcard.com/report/github.com/xgzlucario/rotom) [![Go Reference](https://pkg.go.dev/badge/github.com/xgzlucario/rotom.svg)](https://pkg.go.dev/github.com/xgzlucario/rotom) ![](https://img.shields.io/github/languages/code-size/xgzlucario/rotom.svg) [![codecov](https://codecov.io/gh/xgzlucario/rotom/graph/badge.svg?token=2V0HJ4KO3E)](https://codecov.io/gh/xgzlucario/rotom) [![tests](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml/badge.svg)](https://github.com/xgzlucario/rotom/actions/workflows/rotom.yml) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

English | [中文](README_CN.md)

## Introduction

This is rotom, a high performance, low latency tiny Redis Server written in Go. It replicates the core event loop mechanism AeLoop in Redis based on I/O multiplexing.

## Features

1. Implements the AeLoop single-threaded event loop from Redis using the epoll network model.
2. Compatible with the Redis RESP protocol, allowing any Redis client to connect to rotom.
3. Implements data structures such as dict, list, map, zipmap, set, zipset, and zset.
4. Supports RDB & AOF.
5. Supports 20+ commonly used commands.

## AELoop

AeLoop (Async Event Loop) is the core asynchronous event-driven mechanism in Redis, which mainly includes:

![img](asserts/aeloop.png)

1. FileEvent: Uses I/O multiplexing to handle read and write events on network sockets, categorized into `READABLE` and `WRITABLE`.
2. TimeEvent: Handles tasks that need to be executed after a delay or periodically, such as expiring items every `100ms`.
3. When events are ready, they are processed by callback functions bound to those events.

In rotom, the AeLoop event loop mechanism in Redis is replicated, specifically:

1. When a new TCP connection arrives, the `AcceptHandler` obtains the socket fd and adds it to the event loop, registering a read event.
2. When the read event is ready, `ReadQueryFromClient` reads buffered data into `queryBuf`.
3. `ProcessQueryBuf` parses and executes the corresponding commands from `queryBuf`.
4. The command execution result is saved, and the socket fd's write event is registered.
5. When the write event is ready, `SendReplyToClient` writes all results back to the client. A write event may return multiple read event results at once.
6. Resources are released, and the process continues until the service is shut down.

## Data Structures

Rotom has made several optimizations in data structures:

- dict: Rotom uses `stdmap` as the db hash table, with built-in progressive rehashing.
- hash: Based on `zipmap` with higher memory efficiency.
- set: Uses `zipset` when the set is small and `mapset` when it is large.
- list: Uses a `quicklist` based on `listpack` for a doubly linked list.
- zset: Uses `zipzset` when small and `hash` + `skiplist` when it is large.

Notably, `zipmap` and `zipset` are space-efficient data structures based on `listpack`, which is a new compressed list proposed by Redis to replace `ziplist`, supporting both forward and reverse traversal and solving the cascading update issue in `ziplist`.

## Benchmark

![img](asserts/bench.jpg)

The test will run rotom on the same machine with `appendonly` disabled, and use `redis-benchmark` tool to test the latency of different commands.

```
goos: linux
goarch: amd64
pkg: github.com/xgzlucario/rotom
cpu: 13th Gen Intel(R) Core(TM) i5-13600KF
```

```
	redis	rotom	redis_P10	rotom_P10	redis_P50	rotom_P50
SET	268817	268817	2222222	2173913	3448276	5263158
GET	265957	259740	2702702	1818181	4347826	4545454
INCR	271739	261780	2500000	2439024	4347826	7692307
LPUSH	289017	282485	2083333	2272727	2941176	4347826
RPUSH	283286	271739	2272727	2439024	3333333	7692307
SADD	273972	269541	2439024	2631579	4000000	7142857
HSET	282485	277777	2000000	2127659	3030303	3703703
ZADD	273224	272479	1960784	2702702	2941176	6249999
```

## Roadmap

- Support for LRU cache and memory eviction.
- Support for gradual rehashing in dict.
- Support for RDB and AOF Rewrite.
- Compatibility with more commonly used commands.

## Usage

**Running Locally**

First, clone the project to your local machine:

```bash
git clone https://github.com/xgzlucario/rotom
```

Ensure your local Go environment is >= `1.22`. In the project directory, run `go run .` to start the service, which listens on port `6379` by default:

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

**Running in a Container**

Alternatively, you can run it in a container. First, build the Docker image by running `make build-docker`:

```
REPOSITORY       TAG           IMAGE ID       CREATED         SIZE
rotom            latest        e93cf2060e5f   13 seconds ago  40.2MB
```

Then, start the container:

```bash
docker run --rm -p 6379:6379 --name rotom rotom:latest
```