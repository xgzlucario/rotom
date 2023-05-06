package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

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

	// 插入 1 万条数据
	start := time.Now()
	pipe := client.Pipeline()
	for i := 0; i < 10000; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		pipe.Set(ctx, key, value, 0)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("批量插入数据失败: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("使用管道批量插入 1 万条数据耗时: %s\n", elapsed)
}
