package main

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func startup() {
	config := &Config{Port: 20082}
	if err := InitDB(config); err != nil {
		log.Panic("init db error:", err)
	}
	if err := initServer(config); err != nil {
		log.Panicf("init server error: %v\n", err)
	}
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	server.aeLoop.AeMain()
}

var ctx = context.Background()

func TestCommand(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	go startup()
	time.Sleep(time.Second / 5)

	// wait for client starup
	rdb := redis.NewClient(&redis.Options{
		Addr: ":20082",
	})
	time.Sleep(time.Second / 5)

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
	})

	t.Run("client-closed", func(t *testing.T) {
		rdb.Close()
	})
}
