package main

import (
	"context"
	"fmt"
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
		AppendFileName: "appendonly-test.aof",
	}
	os.Remove(config.AppendFileName)
	config4Server(config)
	printBanner(config)
	server.aeLoop.AddRead(server.fd, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AE_NORMAL, 1000, SysMonitor, nil)
	server.aeLoop.AeMain()
}

var ctx = context.Background()

func TestCommand(t *testing.T) {
	assert := assert.New(t)

	go startup()
	time.Sleep(time.Second / 2)

	// wait for client starup
	rdb := redis.NewClient(&redis.Options{
		Addr: ":20082",
	})

	t.Run("ping", func(t *testing.T) {
		res, _ := rdb.Ping(ctx).Result()
		assert.Equal(res, "PONG")
	})

	t.Run("key", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", 0).Result()
		assert.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		assert.Equal(res, "bar")

		res, err := rdb.Get(ctx, "none").Result()
		assert.Equal(err, redis.Nil)
		assert.Equal(res, "")

		n, _ := rdb.Del(ctx, "foo", "none").Result()
		assert.Equal(n, int64(1))
	})

	t.Run("setex", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", time.Second).Result()
		assert.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		assert.Equal(res, "bar")

		time.Sleep(time.Second + time.Millisecond)

		_, err := rdb.Get(ctx, "foo").Result()
		assert.Equal(err, redis.Nil)
	})

	t.Run("setpx", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", time.Millisecond*100).Result()
		assert.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		assert.Equal(res, "bar")

		time.Sleep(time.Millisecond * 101)

		_, err := rdb.Get(ctx, "foo").Result()
		assert.Equal(err, redis.Nil)
	})

	t.Run("pipline", func(t *testing.T) {
		pip := rdb.Pipeline()
		pip.RPush(ctx, "ls-pip", "A", "B", "C")
		pip.LPop(ctx, "ls-pip")

		_, err := pip.Exec(ctx)
		assert.Nil(err)

		res, _ := rdb.LRange(ctx, "ls-pip", 0, -1).Result()
		assert.Equal(res, []string{"B", "C"})
	})

	t.Run("incr", func(t *testing.T) {
		// incr num
		res, _ := rdb.Incr(ctx, "testInt").Result()
		assert.Equal(res, int64(1))

		res, _ = rdb.Incr(ctx, "testInt").Result()
		assert.Equal(res, int64(2))

		// get int
		str, _ := rdb.Get(ctx, "testInt").Result()
		assert.Equal(str, "2")

		// incr string
		rdb.Set(ctx, "testStr", "5", 0)
		res, _ = rdb.Incr(ctx, "testStr").Result()
		assert.Equal(res, int64(6))

		rdb.Set(ctx, "notNum", "bar", 0)
		_, err := rdb.Incr(ctx, "notNum").Result()
		assert.Equal(err.Error(), errParseInteger.Error())
	})

	t.Run("hash", func(t *testing.T) {
		// hset
		res, _ := rdb.HSet(ctx, "map", "k1", "v1").Result()
		assert.Equal(res, int64(1))

		res, _ = rdb.HSet(ctx, "map", "k2", "v2", "k3", "v3").Result()
		assert.Equal(res, int64(2))

		res, _ = rdb.HSet(ctx, "map", map[string]any{"k4": "v4", "k5": "v5"}).Result()
		assert.Equal(res, int64(2))

		res, _ = rdb.HSet(ctx, "map", map[string]any{"k4": "v4", "k5": "v5"}).Result()
		assert.Equal(res, int64(0))

		// hget
		{
			res, _ := rdb.HGet(ctx, "map", "k1").Result()
			assert.Equal(res, "v1")

			res, err := rdb.HGet(ctx, "map", "k99").Result()
			assert.Equal(err, redis.Nil)
			assert.Equal(res, "")
		}

		resm, _ := rdb.HGetAll(ctx, "map").Result()
		assert.Equal(resm, map[string]string{"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5"})

		// hdel
		res, _ = rdb.HDel(ctx, "map", "k1", "k2", "k3", "k99").Result()
		assert.Equal(res, int64(3))

		// error hset
		_, err := rdb.HSet(ctx, "map").Result()
		assert.Equal(err.Error(), errInvalidArguments.Error())

		_, err = rdb.HSet(ctx, "map", "k1", "v1", "k2").Result()
		assert.Equal(err.Error(), errInvalidArguments.Error())

		// err wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err = rdb.HGet(ctx, "key", "field1").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HSet(ctx, "key", "field1", "value1").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HDel(ctx, "key", "field1").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.HGetAll(ctx, "key").Result()
		assert.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("list", func(t *testing.T) {
		// lpush
		n, _ := rdb.LPush(ctx, "list", "a", "b", "c").Result()
		assert.Equal(n, int64(3))

		// rpush
		n, _ = rdb.RPush(ctx, "list", "d", "e", "f").Result()
		assert.Equal(n, int64(6))

		// lrange
		res, _ := rdb.LRange(ctx, "list", 0, -1).Result()
		assert.Equal(res, []string{"c", "b", "a", "d", "e", "f"})

		res, _ = rdb.LRange(ctx, "list", 1, 3).Result()
		assert.Equal(res, []string{"b", "a"})

		res, err := rdb.LRange(ctx, "list", 3, 2).Result()
		assert.Equal(len(res), 0)
		assert.Nil(err)

		// lpop
		val, _ := rdb.LPop(ctx, "list").Result()
		assert.Equal(val, "c")

		// rpop
		val, _ = rdb.RPop(ctx, "list").Result()
		assert.Equal(val, "f")

		// pop nil
		{
			_, err := rdb.LPop(ctx, "list-empty").Result()
			assert.Equal(err, redis.Nil)

			_, err = rdb.RPop(ctx, "list-empty").Result()
			assert.Equal(err, redis.Nil)
		}

		// error wrong type
		rdb.Set(ctx, "key", "value", 0)

		_, err = rdb.LPush(ctx, "key", "1").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.RPush(ctx, "key", "1").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.LPop(ctx, "key").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.RPop(ctx, "key").Result()
		assert.Equal(err.Error(), errWrongType.Error())

		_, err = rdb.LRange(ctx, "key", 0, -1).Result()
		assert.Equal(err.Error(), errWrongType.Error())

		rdb.RPush(ctx, "ls-test", "A")
		_, err = rdb.Incr(ctx, "ls-test").Result()
		assert.Equal(err.Error(), errWrongType.Error())
	})

	t.Run("set", func(t *testing.T) {
		n, _ := rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		assert.Equal(n, int64(3))

		// spop
		for i := 0; i < 3; i++ {
			val, _ := rdb.SPop(ctx, "set").Result()
			assert.NotEqual(val, "")
		}

		_, err := rdb.SPop(ctx, "set").Result()
		assert.Equal(err, redis.Nil)

		// srem
		rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		res, _ := rdb.SRem(ctx, "set", "k1", "k2", "k999").Result()
		assert.Equal(res, int64(2))
	})

	t.Run("zset", func(t *testing.T) {
		n, _ := rdb.ZAdd(ctx, "rank", redis.Z{Member: "player1"}).Result()
		assert.Equal(n, int64(1))

		n, _ = rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "player1", Score: 100},
			redis.Z{Member: "player2", Score: 300.5},
			redis.Z{Member: "player3", Score: 100}).Result()
		assert.Equal(n, int64(2))

		// zrank
		{
			res, _ := rdb.ZRank(ctx, "rank", "player1").Result()
			assert.Equal(res, int64(0))

			res, _ = rdb.ZRank(ctx, "rank", "player2").Result()
			assert.Equal(res, int64(2))

			res, _ = rdb.ZRank(ctx, "rank", "player3").Result()
			assert.Equal(res, int64(1))

			_, err := rdb.ZRank(ctx, "rank", "player999").Result()
			assert.Equal(err, redis.Nil)
		}

		// zrange
		{
			res, _ := rdb.ZRange(ctx, "rank", 0, -1).Result()
			assert.Equal(res, []string{"player1", "player3", "player2"})

			res, _ = rdb.ZRange(ctx, "rank", 1, 3).Result()
			assert.Equal(res, []string{"player3", "player2"})

			res, err := rdb.ZRange(ctx, "rank", 70, 60).Result()
			assert.Equal(len(res), 0)
			assert.Nil(err)
		}

		// zrangeWithScores
		{
			res, _ := rdb.ZRangeWithScores(ctx, "rank", 0, -1).Result()
			assert.Equal(res, []redis.Z{
				{Member: "player1", Score: 100},
				{Member: "player3", Score: 100},
				{Member: "player2", Score: 300.5},
			})

			res, _ = rdb.ZRangeWithScores(ctx, "rank", 1, 3).Result()
			assert.Equal(res, []redis.Z{
				{Member: "player3", Score: 100},
				{Member: "player2", Score: 300.5},
			})

			res, err := rdb.ZRangeWithScores(ctx, "rank", 70, 60).Result()
			assert.Equal(len(res), 0)
			assert.Nil(err)
		}

		// zpopmin
		{
			res, _ := rdb.ZPopMin(ctx, "rank", 2).Result()
			assert.Equal(res, []redis.Z{
				{Member: "player1", Score: 100},
				{Member: "player3", Score: 100},
			})

			res, _ = rdb.ZPopMin(ctx, "rank").Result()
			assert.Equal(res, []redis.Z{
				{Member: "player2", Score: 300.5},
			})
		}

		// zrem
		rdb.ZAdd(ctx, "rank",
			redis.Z{Member: "player1", Score: 100},
			redis.Z{Member: "player2", Score: 300.5},
			redis.Z{Member: "player3", Score: 100})

		res, _ := rdb.ZRem(ctx, "rank", "player1", "player2", "player999").Result()
		assert.Equal(res, int64(2))
	})

	t.Run("flushdb", func(t *testing.T) {
		rdb.Set(ctx, "test-flush", "1", 0)
		res, _ := rdb.FlushDB(ctx).Result()
		assert.Equal(res, "OK")

		_, err := rdb.Get(ctx, "test-flush").Result()
		assert.Equal(err, redis.Nil)
	})

	t.Run("concurrency", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				key := fmt.Sprintf("key%08x", rand.Int())
				value := fmt.Sprintf("val%08x", rand.Int())

				_, err := rdb.Set(ctx, key, value, 0).Result()
				assert.Nil(err)

				res, err := rdb.Get(ctx, key).Result()
				assert.Equal(res, value)
				assert.Nil(err)

				wg.Done()
			}()
		}
		wg.Wait()
	})

	t.Run("bigKey", func(t *testing.T) {
		body := make([]byte, MAX_QUERY_DATA_LEN)
		_, err := rdb.Set(ctx, "bigKey", body, 0).Result()
		assert.NotNil(err)
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

	t.Run("client-closed", func(t *testing.T) {
		rdb.Close()
	})
}

func TestConfig(t *testing.T) {
	assert := assert.New(t)

	cfg, _ := LoadConfig("config.json")
	assert.Equal(cfg.Port, 6379)

	_, err := LoadConfig("not-exist.json")
	assert.NotNil(err)

	_, err = LoadConfig("go.mod")
	assert.NotNil(err)
}

func TestReadableSize(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(readableSize(50), "50B")
	assert.Equal(readableSize(50*KB), "50.0KB")
	assert.Equal(readableSize(50*MB), "50.0MB")
	assert.Equal(readableSize(50*GB), "50.0GB")
}
