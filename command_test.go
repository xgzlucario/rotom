package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func startup() {
	config := &Config{
		Port:           20082,
		AppendOnly:     true,
		AppendFileName: "test.aof",
	}
	_ = os.Remove(config.AppendFileName)
	config4Server(config)
	printBanner(config)
	RegisterAeLoop(&server)
	// custom
	server.aeLoop.AddTimeEvent(AE_ONCE, 300, func(loop *AeLoop, id int, extra interface{}) {}, nil)
	server.aeLoop.AeMain()
}

const (
	testTypeRotom     = "rotom"
	testTypeMiniRedis = "miniredis"
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
		res, _ := rdb.Ping(ctx).Result()
		ast.Equal(res, "PONG")
	})

	t.Run("key", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", 0).Result()
		ast.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		ast.Equal(res, "bar")

		res, err := rdb.Get(ctx, "none").Result()
		ast.Equal(res, "")
		ast.Equal(err, redis.Nil)

		n, _ := rdb.Del(ctx, "foo", "none").Result()
		ast.Equal(n, int64(1))

		// setex
		{
			res, _ = rdb.Set(ctx, "foo", "bar", time.Second).Result()
			ast.Equal(res, "OK")

			res, _ = rdb.Get(ctx, "foo").Result()
			ast.Equal(res, "bar")

			sleepFn(time.Second + 10*time.Millisecond)

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
		// hset
		res, _ := rdb.HSet(ctx, "map", "k1", "v1").Result()
		ast.Equal(res, int64(1))

		res, _ = rdb.HSet(ctx, "map", "k2", "v2", "k3", "v3").Result()
		ast.Equal(res, int64(2))

		res, _ = rdb.HSet(ctx, "map", map[string]any{"k4": "v4", "k5": "v5"}).Result()
		ast.Equal(res, int64(2))

		res, _ = rdb.HSet(ctx, "map", map[string]any{"k4": "v4", "k5": "v5"}).Result()
		ast.Equal(res, int64(0))

		// hget
		{
			res, _ := rdb.HGet(ctx, "map", "k1").Result()
			ast.Equal(res, "v1")

			res, err := rdb.HGet(ctx, "map", "k99").Result()
			ast.Equal(err, redis.Nil)
			ast.Equal(res, "")
		}

		resm, _ := rdb.HGetAll(ctx, "map").Result()
		ast.Equal(resm, map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5"})

		// hdel
		res, _ = rdb.HDel(ctx, "map", "k1", "k2", "k3", "k99").Result()
		ast.Equal(res, int64(3))

		// error hset
		_, err := rdb.HSet(ctx, "map").Result()
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

		// spop
		for i := 0; i < 3; i++ {
			val, _ := rdb.SPop(ctx, "set").Result()
			ast.NotEqual(val, "")
		}

		_, err := rdb.SPop(ctx, "set").Result()
		ast.Equal(err, redis.Nil)

		// srem
		rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		res, _ := rdb.SRem(ctx, "set", "k1", "k2", "k999").Result()
		ast.Equal(res, int64(2))

		// error wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err = rdb.SAdd(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.SRem(ctx, "key", "1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.SPop(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("zset", func(t *testing.T) {
		n, _ := rdb.ZAdd(ctx, "rank", redis.Z{Member: "player1"}).Result()
		ast.Equal(n, int64(1))

		n, _ = rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "player1", Score: 100},
			redis.Z{Member: "player2", Score: 300.5},
			redis.Z{Member: "player3", Score: 100}).Result()
		ast.Equal(n, int64(2))

		// zrank
		{
			res, _ := rdb.ZRank(ctx, "rank", "player1").Result()
			ast.Equal(res, int64(0))

			res, _ = rdb.ZRank(ctx, "rank", "player2").Result()
			ast.Equal(res, int64(2))

			res, _ = rdb.ZRank(ctx, "rank", "player3").Result()
			ast.Equal(res, int64(1))

			_, err := rdb.ZRank(ctx, "rank", "player999").Result()
			ast.Equal(err, redis.Nil)
		}

		// zrange
		{
			res, _ := rdb.ZRange(ctx, "rank", 0, -1).Result()
			ast.Equal(res, []string{"player1", "player3", "player2"})

			res, _ = rdb.ZRange(ctx, "rank", 1, 3).Result()
			ast.Equal(res, []string{"player3", "player2"})

			res, err := rdb.ZRange(ctx, "rank", 70, 60).Result()
			ast.Equal(len(res), 0)
			ast.Nil(err)
		}

		// zrangeWithScores
		{
			res, _ := rdb.ZRangeWithScores(ctx, "rank", 0, -1).Result()
			ast.Equal(res, []redis.Z{
				{Member: "player1", Score: 100},
				{Member: "player3", Score: 100},
				{Member: "player2", Score: 300.5},
			})

			res, _ = rdb.ZRangeWithScores(ctx, "rank", 1, 3).Result()
			ast.Equal(res, []redis.Z{
				{Member: "player3", Score: 100},
				{Member: "player2", Score: 300.5},
			})

			res, err := rdb.ZRangeWithScores(ctx, "rank", 70, 60).Result()
			ast.Equal(len(res), 0)
			ast.Nil(err)
		}

		// zpopmin
		{
			res, _ := rdb.ZPopMin(ctx, "rank", 2).Result()
			ast.Equal(res, []redis.Z{
				{Member: "player1", Score: 100},
				{Member: "player3", Score: 100},
			})

			res, _ = rdb.ZPopMin(ctx, "rank").Result()
			ast.Equal(res, []redis.Z{
				{Member: "player2", Score: 300.5},
			})
		}

		// zrem
		rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "player1", Score: 100},
			redis.Z{Member: "player2", Score: 300.5},
			redis.Z{Member: "player3", Score: 100})

		res, _ := rdb.ZRem(ctx, "rank", "player1", "player2", "player999").Result()
		ast.Equal(res, int64(2))

		// err wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err := rdb.ZAdd(ctx, "key", redis.Z{}).Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZRank(ctx, "key", "member1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZRem(ctx, "key", "member1").Result()
		ast.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.ZPopMin(ctx, "key").Result()
		ast.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("eval", func(t *testing.T) {
		res, _ := rdb.Eval(ctx, "return {'key1','key2','key3'}", nil).Result()
		ast.Equal(res, []any{"key1", "key2", "key3"})

		res, _ = rdb.Eval(ctx, "return {1,2,3}", nil).Result()
		ast.Equal(res, []any{int64(1), int64(2), int64(3)})

		// set
		_, err := rdb.Eval(ctx, "redis.call('set','xgz','qwe')", nil).Result()
		ast.Equal(err, redis.Nil)

		res, _ = rdb.Eval(ctx, "return redis.call('set','xgz','qwe')", nil).Result()
		ast.Equal(res, "OK")

		// get
		res, _ = rdb.Eval(ctx, "return redis.call('get','xgz')", nil).Result()
		ast.Equal(res, "qwe")

		// get nil
		_, err = rdb.Eval(ctx, "return redis.call('get','not-ex-evalkey')", nil).Result()
		ast.Equal(err, redis.Nil)

		// error call
		_, err = rdb.Eval(ctx, "return redis.call('myfunc','key')", nil).Result()
		ast.NotNil(err)
	})

	t.Run("flushdb", func(t *testing.T) {
		rdb.Set(ctx, "test-flush", "1", 0)
		res, _ := rdb.FlushDB(ctx).Result()
		ast.Equal(res, "OK")

		_, err := rdb.Get(ctx, "test-flush").Result()
		ast.Equal(err, redis.Nil)
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
		t.Run("save", func(t *testing.T) {
			res, err := rdb.Save(context.Background()).Result()
			ast.Nil(err)
			ast.Equal(res, "OK")
		})

		t.Run("bigKey", func(t *testing.T) {
			body := make([]byte, MaxQueryDataLen)
			_, err := rdb.Set(ctx, "bigKey", body, 0).Result()
			ast.NotNil(err)
		})

		t.Run("trans-zipmap", func(t *testing.T) {
			for i := 0; i <= 256; i++ {
				k := fmt.Sprintf("%06x", i)
				rdb.HSet(ctx, "zipmap", k, k)
			}
		})

		t.Run("trans-zipset", func(t *testing.T) {
			for i := 0; i <= 512; i++ {
				k := fmt.Sprintf("%06x", i)
				rdb.SAdd(ctx, "zipset", k)
			}
		})
	}

	t.Run("closed", func(t *testing.T) {
		err := rdb.Close()
		ast.Nil(err)
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
