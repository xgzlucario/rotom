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
	server.config = config
	server.RunServe()
}

var ctx = context.Background()

func TestHandler(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	go startup()
	time.Sleep(time.Second / 2)

	// wait for client starup
	rdb := redis.NewClient(&redis.Options{
		Addr: ":20082",
	})
	time.Sleep(time.Second / 2)

	t.Run("ping", func(t *testing.T) {
		res, _ := rdb.Ping(ctx).Result()
		assert.Equal(res, "PONG")
	})

	t.Run("set", func(t *testing.T) {
		res, _ := rdb.Set(ctx, "foo", "bar", 0).Result()
		assert.Equal(res, "OK")

		res, _ = rdb.Get(ctx, "foo").Result()
		assert.Equal(res, "bar")
	})
}
