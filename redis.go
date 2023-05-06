package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-redis/redis/v8"
)

func RedisTest() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("连接 Redis 失败: %v", err)
	}

	// 插入五百万条数据
	start := time.Now()
	pipe := client.Pipeline()
	for i := 0; i < 500*10000; i++ {
		pipe.Set(ctx, gofakeit.Phone(), i, 0)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("批量插入数据失败: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Redis 批量插入五百万条数据耗时: %s\n", elapsed)
}
