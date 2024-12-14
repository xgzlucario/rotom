package main

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func startup() {
	config := &Config{
		Port:           20082,
		AppendOnly:     true,
		AppendFileName: "test.aof",
		Save:           true,
		SaveFileName:   "dump.rdb",
	}
	_ = os.Remove(config.AppendFileName)
	config4Server(config)
	printBanner(config)
	RegisterAeLoop(&server)
	// custom
	server.aeLoop.AddTimeEvent(AeOnce, 300, func(_ *AeLoop, _ int, _ interface{}) {}, nil)
	server.aeLoop.AeMain()
}

const (
	testTypeRotom     = "rotom"
	testTypeMiniRedis = "miniRedis"
	testTypeRealRedis = "realRedis"
)

func TestCommand(t *testing.T) {
	t.Run(testTypeMiniRedis, func(t *testing.T) {
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{
			Addr: s.Addr(),
		})
		sleepFn := func(dur time.Duration) {
			s.FastForward(dur)
		}
		testCommand(t, testTypeMiniRedis, rdb, sleepFn)
	})
	//t.Run(testTypeRealRedis, func(t *testing.T) {
	//	// NOTES: run redis first!
	//	rdb := redis.NewClient(&redis.Options{
	//		Addr: ":6379",
	//	})
	//	sleepFn := func(dur time.Duration) {
	//		time.Sleep(dur)
	//	}
	//	rdb.FlushDB(context.Background())
	//	testCommand(t, testTypeRealRedis, rdb, sleepFn)
	//	rdb.FlushDB(context.Background())
	//})
	t.Run(testTypeRotom, func(t *testing.T) {
		go startup()
		time.Sleep(time.Second / 2)
		rdb := redis.NewClient(&redis.Options{
			Addr: ":20082",
		})
		sleepFn := func(dur time.Duration) {
			time.Sleep(dur)
		}
		testCommand(t, testTypeRotom, rdb, sleepFn)
	})
}

func testCommand(t *testing.T, testType string, rdb *redis.Client, sleepFn func(time.Duration)) {
	ast := assert.New(t)
	ctx := context.Background()

	t.Run("ping", func(t *testing.T) {
		res, err := rdb.Ping(ctx).Result()
		ast.Equal(res, "PONG")
		ast.Nil(err)
	})

	t.Run("key", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", 0).Result()
		ast.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		ast.Equal(res, "bar")

		res, err := rdb.Get(ctx, "none").Result()
		ast.Equal(res, "")
		ast.Equal(err, redis.Nil)

		_type, _ := rdb.Type(ctx, "foo").Result()
		ast.Equal(_type, "string")

		_type, _ = rdb.Type(ctx, "not-exist").Result()
		ast.Equal(_type, "none")

		n, _ := rdb.Del(ctx, "foo", "none").Result()
		ast.Equal(n, int64(1))
		// setex
		{
			res, _ = rdb.Set(ctx, "foo", "bar", time.Second).Result()
			ast.Equal(res, "OK")

			res, _ = rdb.Get(ctx, "foo").Result()
			ast.Equal(res, "bar")

			sleepFn(time.Second + 500*time.Millisecond)

			_, err := rdb.Get(ctx, "foo").Result()
			ast.Equal(err, redis.Nil)
		}
		// setpx
		{
			res, _ = rdb.Set(ctx, "foo", "bar", time.Millisecond*100).Result()
			ast.Equal(res, "OK")

			res, _ = rdb.Get(ctx, "foo").Result()
			ast.Equal(res, "bar")

			sleepFn(time.Millisecond * 101)

			_, err := rdb.Get(ctx, "foo").Result()
			ast.Equal(err, redis.Nil)
		}
		// setnx
		{
			ok, err := rdb.SetNX(ctx, "keynx", "123", redis.KeepTTL).Result()
			ast.Nil(err)
			ast.True(ok)

			ok, err = rdb.SetNX(ctx, "keynx", "123", redis.KeepTTL).Result()
			ast.Nil(err)
			ast.False(ok)
		}
		// error
		{
			lskey := fmt.Sprintf("ls-%x", time.Now().UnixNano())
			rdb.RPush(ctx, lskey, "1")

			_, err := rdb.Get(ctx, lskey).Result()
			ast.Equal(err.Error(), errWrongType.Error())
		}
	})

	t.Run("incr", func(t *testing.T) {
		// incr num
		res, _ := rdb.Incr(ctx, "testInt").Result()
		ast.Equal(res, int64(1))

		res, _ = rdb.Incr(ctx, "testInt").Result()
		ast.Equal(res, int64(2))

		_type, _ := rdb.Type(ctx, "testInt").Result()
		ast.Equal(_type, "string")

		// get int
		str, _ := rdb.Get(ctx, "testInt").Result()
		ast.Equal(str, "2")

		// incr string
		rdb.Set(ctx, "testStr", "5", 0)
		res, _ = rdb.Incr(ctx, "testStr").Result()
		ast.Equal(res, int64(6))

		rdb.Set(ctx, "notNum", "bar", 0)
		_, err := rdb.Incr(ctx, "notNum").Result()
		ast.Equal(err.Error(), errParseInteger.Error())
	})

	t.Run("hash", func(t *testing.T) {
		var keys, vals []string
		for i := 0; i < 100; i++ {
			keys = append(keys, fmt.Sprintf("key-%08d", i))
			vals = append(vals, fmt.Sprintf("val-%08d", i))
		}

		// hset
		args := make([]string, 0, len(keys)+len(vals))
		for i, k := range keys {
			args = append(args, k)
			args = append(args, vals[i])
		}
		res, err := rdb.HSet(ctx, "map", args).Result()
		ast.Equal(res, int64(100))
		ast.Nil(err)

		// hget
		for i, k := range keys {
			res, err := rdb.HGet(ctx, "map", k).Result()
			ast.Equal(res, vals[i])
			ast.Nil(err)
		}

		_, err = rdb.HGet(ctx, "map", "not-exist").Result()
		ast.Equal(err, redis.Nil)

		// hgetall
		resm, _ := rdb.HGetAll(ctx, "map").Result()
		ast.Equal(len(resm), 100)

		_type, _ := rdb.Type(ctx, "map").Result()
		ast.Equal(_type, "hash")

		// hdel
		res, _ = rdb.HDel(ctx, "map", keys[0:10]...).Result()
		ast.Equal(res, int64(10))

		// error hset
		_, err = rdb.HSet(ctx, "map").Result()
		ast.Contains(err.Error(), errWrongArguments.Error())

		_, err = rdb.HSet(ctx, "map", "k1", "v1", "k2").Result()
		ast.Contains(err.Error(), errWrongArguments.Error())

		// err wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err = rdb.HGet(ctx, "key", "field1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HSet(ctx, "key", "field1", "value1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HDel(ctx, "key", "field1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HGetAll(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("list", func(t *testing.T) {
		// lpush
		n, _ := rdb.LPush(ctx, "list", "3", "2", "1").Result()
		ast.Equal(n, int64(3))

		// rpush
		n, _ = rdb.RPush(ctx, "list", "4", "5", "6").Result()
		ast.Equal(n, int64(6))

		_type, _ := rdb.Type(ctx, "list").Result()
		ast.Equal(_type, "list")

		// list: [1,2,3,4,5,6]
		// lrange
		res, _ := rdb.LRange(ctx, "list", 0, -1).Result()
		ast.Equal(res, []string{"1", "2", "3", "4", "5", "6"})
		res, _ = rdb.LRange(ctx, "list", -100, 100).Result()
		ast.Equal(res, []string{"1", "2", "3", "4", "5", "6"})
		res, _ = rdb.LRange(ctx, "list", 1, 3).Result()
		ast.Equal(res, []string{"2", "3", "4"})
		res, _ = rdb.LRange(ctx, "list", 3, 3).Result()
		ast.Equal(res, []string{"4"})
		res, _ = rdb.LRange(ctx, "list", -5, 2).Result()
		ast.Equal(res, []string{"2", "3"})

		// revrange not support
		res, _ = rdb.LRange(ctx, "list", -1, -3).Result()
		ast.Equal(res, []string{})
		res, _ = rdb.LRange(ctx, "list", -1, 2).Result()
		ast.Equal(res, []string{})
		res, _ = rdb.LRange(ctx, "list", 3, 2).Result()
		ast.Equal(res, []string{})
		res, _ = rdb.LRange(ctx, "list", 99, 100).Result()
		ast.Equal(res, []string{})
		res, _ = rdb.LRange(ctx, "list", -100, -99).Result()
		ast.Equal(res, []string{})

		// lpop
		val, _ := rdb.LPop(ctx, "list").Result()
		ast.Equal(val, "1")

		// rpop
		val, _ = rdb.RPop(ctx, "list").Result()
		ast.Equal(val, "6")

		n, _ = rdb.LPush(ctx, "list", "6").Result()
		ast.Equal(n, int64(5))

		// pop nil
		{
			_, err := rdb.LPop(ctx, "list-empty").Result()
			ast.Equal(err, redis.Nil)

			_, err = rdb.RPop(ctx, "list-empty").Result()
			ast.Equal(err, redis.Nil)
		}

		// error wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err := rdb.LPush(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.RPush(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.LPop(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.RPop(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.LRange(ctx, "key", 0, -1).Result()
		ast.Equal(err.Error(), errWrongType.Error())

		rdb.RPush(ctx, "ls-test", "A")
		_, err = rdb.Incr(ctx, "ls-test").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("set", func(t *testing.T) {
		n, _ := rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		ast.Equal(n, int64(3))

		// smembers
		mems, _ := rdb.SMembers(ctx, "set").Result()
		ast.ElementsMatch(mems, []string{"k1", "k2", "k3"})

		_type, _ := rdb.Type(ctx, "set").Result()
		ast.Equal(_type, "set")

		// spop
		for i := 0; i < 3; i++ {
			val, _ := rdb.SPop(ctx, "set").Result()
			ast.NotEqual(val, "")
		}

		_, err := rdb.SPop(ctx, "set").Result()
		ast.Equal(err, redis.Nil)

		// srem
		_, _ = rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		res, _ := rdb.SRem(ctx, "set", "k1", "k2", "k999").Result()
		ast.Equal(res, int64(2))

		// error wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err = rdb.SAdd(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.SRem(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.SMembers(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.SPop(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("zset", func(t *testing.T) {
		n, _ := rdb.ZAdd(ctx, "rank", redis.Z{Member: "user1"}).Result()
		ast.Equal(n, int64(1))

		n, _ = rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "user1", Score: 100},
			redis.Z{Member: "user2", Score: 300.5},
			redis.Z{Member: "user3", Score: 100}).Result()
		ast.Equal(n, int64(2))

		_type, _ := rdb.Type(ctx, "rank").Result()
		ast.Equal(_type, "zset")

		// zrank
		{
			res, _ := rdb.ZRank(ctx, "rank", "user1").Result()
			ast.Equal(res, int64(0))

			res, _ = rdb.ZRank(ctx, "rank", "user2").Result()
			ast.Equal(res, int64(2))

			res, _ = rdb.ZRank(ctx, "rank", "user3").Result()
			ast.Equal(res, int64(1))

			_, err := rdb.ZRank(ctx, "rank", "user0").Result()
			ast.Equal(err, redis.Nil)
		}

		// zrange
		{
			res, _ := rdb.ZRange(ctx, "rank", 0, -1).Result()
			ast.Equal(res, []string{"user1", "user3", "user2"})

			res, _ = rdb.ZRange(ctx, "rank", 1, 3).Result()
			ast.Equal(res, []string{"user3", "user2"})

			res, err := rdb.ZRange(ctx, "rank", 70, 60).Result()
			ast.Equal(len(res), 0)
			ast.Nil(err)
		}
		// zrangeWithScores
		{
			res, _ := rdb.ZRangeWithScores(ctx, "rank", 0, -1).Result()
			ast.Equal(res, []redis.Z{
				{Member: "user1", Score: 100},
				{Member: "user3", Score: 100},
				{Member: "user2", Score: 300.5},
			})

			res, _ = rdb.ZRangeWithScores(ctx, "rank", 1, 3).Result()
			ast.Equal(res, []redis.Z{
				{Member: "user3", Score: 100},
				{Member: "user2", Score: 300.5},
			})

			res, err := rdb.ZRangeWithScores(ctx, "rank", 70, 60).Result()
			ast.Equal(len(res), 0)
			ast.Nil(err)
		}
		// zpopmin
		{
			res, _ := rdb.ZPopMin(ctx, "rank", 3).Result()
			ast.Equal(res, []redis.Z{
				{Member: "user1", Score: 100},
				{Member: "user3", Score: 100},
				{Member: "user2", Score: 300.5},
			})
		}
		// zrem
		rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "user1", Score: 100},
			redis.Z{Member: "user2", Score: 300.5},
			redis.Z{Member: "user3", Score: 100})

		res, _ := rdb.ZRem(ctx, "rank", "user1", "user2", "user0").Result()
		ast.Equal(res, int64(2))

		// err wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err := rdb.ZAdd(ctx, "key", redis.Z{}).Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZRank(ctx, "key", "member1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZRange(ctx, "key", 0, -1).Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZRem(ctx, "key", "member1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZPopMin(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("flushdb", func(t *testing.T) {
		rdb.Set(ctx, "test-flush", "1", 0)
		res, _ := rdb.FlushDB(ctx).Result()
		ast.Equal(res, "OK")

		_, err := rdb.Get(ctx, "test-flush").Result()
		ast.Equal(err, redis.Nil)
	})

	t.Run("scan", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			rdb.Set(ctx, fmt.Sprintf("key-%d", i), 1, 0)
		}
		keys, cursor, err := rdb.Scan(ctx, 0, "", 5).Result()
		ast.Equal(len(keys), 5)
		ast.Equal(cursor, uint64(5))
		ast.Nil(err)

		keys, cursor, err = rdb.Scan(ctx, 0, "", 10).Result()
		ast.Equal(len(keys), 10)
		ast.Equal(cursor, uint64(0))
		ast.Nil(err)
	})

	t.Run("pipline", func(t *testing.T) {
		pip := rdb.Pipeline()
		pip.RPush(ctx, "pip-ls", "1")
		pip.RPush(ctx, "pip-ls", "2")
		pip.RPush(ctx, "pip-ls", "3")
		_, err := pip.Exec(ctx)
		ast.Nil(err)

		sls, _ := rdb.LRange(ctx, "pip-ls", 0, -1).Result()
		ast.Equal(sls, []string{"1", "2", "3"})
	})

	t.Run("concurrency", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				key := fmt.Sprintf("key%08x", rand.Int())
				value := fmt.Sprintf("val%08x", rand.Int())

				_, err := rdb.Set(ctx, key, value, 0).Result()
				ast.Nil(err)

				res, _ := rdb.Get(ctx, key).Result()
				ast.Equal(res, value)

				wg.Done()
			}()
		}
		wg.Wait()
	})

	if testType == testTypeRotom {
		t.Run("bigKey", func(t *testing.T) {
			body := make([]byte, MaxQueryDataLen)
			_, err := rdb.Set(ctx, "bigKey", body, 0).Result()
			ast.NotNil(err)
		})

		t.Run("trans-zipset", func(t *testing.T) {
			for i := 0; i <= 512; i++ {
				k := fmt.Sprintf("%06x", i)
				rdb.SAdd(ctx, "zipset", k)
			}
		})

		t.Run("save-load", func(t *testing.T) {
			rdb.FlushDB(ctx)
			// set key
			rdb.Set(ctx, "rdb-key1", "123", 0)
			rdb.Set(ctx, "rdb-key2", "234", time.Minute)
			rdb.Set(ctx, "rdb-key3", "345", 1)
			rdb.Incr(ctx, "key-incr")

			rdb.HSet(ctx, "rdb-hash1", "k1", "v1", "k2", "v2")
			rdb.SAdd(ctx, "rdb-set1", "k1", "k2")
			for i := 0; i < 1024; i++ {
				key := fmt.Sprintf("%d", i)
				rdb.HSet(ctx, "rdb-hash2", key, key)
				rdb.SAdd(ctx, "rdb-set2", key)
			}

			rdb.RPush(ctx, "rdb-list1", "k1", "k2", "k3")
			rdb.ZAdd(ctx, "rdb-zset1",
				redis.Z{Score: 200, Member: "k2"},
				redis.Z{Score: 100, Member: "k1"},
				redis.Z{Score: 300, Member: "k3"})

			res, _ := rdb.Save(context.Background()).Result()
			ast.Equal(res, "OK")

			_, err := rdb.Do(ctx, "load").Result()
			ast.Nil(err)

			// valid
			res, _ = rdb.Get(ctx, "rdb-key1").Result()
			ast.Equal(res, "123")
			res, _ = rdb.Get(ctx, "rdb-key2").Result()
			ast.Equal(res, "234")
			_, err = rdb.Get(ctx, "rdb-key3").Result()
			ast.Equal(err, redis.Nil)
			res, _ = rdb.Get(ctx, "key-incr").Result()
			ast.Equal(res, "1")

			resm, _ := rdb.HGetAll(ctx, "rdb-hash1").Result()
			ast.Equal(resm, map[string]string{"k1": "v1", "k2": "v2"})
			ress, _ := rdb.SMembers(ctx, "rdb-set1").Result()
			ast.ElementsMatch(ress, []string{"k1", "k2"})

			resm, _ = rdb.HGetAll(ctx, "rdb-hash2").Result()
			ast.Equal(len(resm), 1024)
			ress, _ = rdb.SMembers(ctx, "rdb-set2").Result()
			ast.Equal(len(ress), 1024)

			ress, _ = rdb.LRange(ctx, "rdb-list1", 0, -1).Result()
			ast.Equal(ress, []string{"k1", "k2", "k3"})

			resz, _ := rdb.ZPopMin(ctx, "rdb-zset1").Result()
			ast.Equal(resz, []redis.Z{{
				Member: "k1", Score: 100,
			}})
		})
	}

	t.Run("close", func(t *testing.T) {
		ast.Nil(rdb.Close())
	})
}

func TestConfig(t *testing.T) {
	ast := assert.New(t)
	cfg, _ := LoadConfig("config.json")
	ast.Equal(cfg.Port, 6379)
	_, err := LoadConfig("not-exist.json")
	ast.NotNil(err)
	_, err = LoadConfig("go.mod")
	ast.NotNil(err)
}

func TestReadableSize(t *testing.T) {
	ast := assert.New(t)
	ast.Equal(readableSize(50), "50B")
	ast.Equal(readableSize(50*KB), "50.0KB")
	ast.Equal(readableSize(50*MB), "50.0MB")
	ast.Equal(readableSize(50*GB), "50.0GB")
}
