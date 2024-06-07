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

func TestHandler(t *testing.T) {
	t.Parallel()
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

	t.Run("set", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", 0).Result()
		assert.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		assert.Equal(res, "bar")

		res, err := rdb.Get(ctx, "none").Result()
		assert.Equal(err, redis.Nil)
		assert.Equal(res, "")
	})

	t.Run("hset", func(t *testing.T) {
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

		// error
		_, err := rdb.HSet(ctx, "map").Result()
		assert.Equal(err.Error(), ErrWrongNumberArgs("hset").Error())

		_, err = rdb.HSet(ctx, "map", "k1", "v1", "k2").Result()
		assert.Equal(err.Error(), ErrWrongNumberArgs("hset").Error())
	})
}
