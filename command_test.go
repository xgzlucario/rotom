package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func startup() {
	config := &Config{
		Port: 20082,
	}
	if err := InitDB(config); err != nil {
		log.Panic().Msgf("init db error: %v", err)
	}
	if err := initServer(config); err != nil {
		log.Panic().Msgf("init server error: %v", err)
	}
	server.aeLoop.AddRead(server.fd, AcceptHandler, nil)
	server.aeLoop.AeMain()
}

var ctx = context.Background()

func TestCommand(t *testing.T) {
	assert := assert.New(t)

	go startup()
	time.Sleep(time.Second / 3)

	// wait for client starup
	rdb := redis.NewClient(&redis.Options{
		Addr: ":20082",
	})
	time.Sleep(time.Second / 3)

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
	})

	t.Run("incr", func(t *testing.T) {
		res, _ := rdb.Incr(ctx, "testIncr").Result()
		assert.Equal(res, int64(1))

		res, _ = rdb.Incr(ctx, "testIncr").Result()
		assert.Equal(res, int64(2))

		rdb.Set(ctx, "notNum", "bar", 0)
		_, err := rdb.Incr(ctx, "notNum").Result()
		assert.Equal(err.Error(), ErrParseInteger.Error())
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
		{
			res, _ := rdb.HDel(ctx, "map", "k1", "k2", "k3", "k99").Result()
			assert.Equal(res, int64(3))
		}

		// error
		_, err := rdb.HSet(ctx, "map").Result()
		assert.Equal(err.Error(), ErrWrongNumberArgs("hset").Error())

		_, err = rdb.HSet(ctx, "map", "k1", "v1", "k2").Result()
		assert.Equal(err.Error(), ErrWrongNumberArgs("hset").Error())
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

		// lpop
		val, _ := rdb.LPop(ctx, "list").Result()
		assert.Equal(val, "c")

		// rpop
		val, _ = rdb.RPop(ctx, "list").Result()
		assert.Equal(val, "f")
	})

	t.Run("set", func(t *testing.T) {
		n, _ := rdb.SAdd(ctx, "set", "k1", "k2", "k3").Result()
		assert.Equal(n, int64(3))

		for i := 0; i < 3; i++ {
			val, _ := rdb.SPop(ctx, "set").Result()
			assert.NotEqual(val, "")
		}

		_, err := rdb.SPop(ctx, "set").Result()
		assert.Equal(err, redis.Nil)
	})

	t.Run("zset", func(t *testing.T) {
		n, _ := rdb.ZAdd(ctx, "zs1", redis.Z{Member: "a"}).Result()
		assert.Equal(n, int64(1))

		n, _ = rdb.ZAdd(ctx, "zs1",
			redis.Z{Member: "a"},
			redis.Z{Member: "b"},
			redis.Z{Member: "c"}).Result()
		assert.Equal(n, int64(2))
	})

	t.Run("mset", func(t *testing.T) {
		res, _ := rdb.MSet(ctx, "mk1", "mv1", "mk2", "mv2").Result()
		assert.Equal(res, "OK")
	})

	t.Run("concurrency", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 10000; i++ {
			wg.Add(1)
			go func() {
				key := fmt.Sprintf("key-%08x", rand.Int())
				value := fmt.Sprintf("val-%08x", rand.Int())

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

	t.Run("client-closed", func(t *testing.T) {
		rdb.Close()
	})
}
