package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-redis/redis/v8"
	"github.com/xgzlucario/rotom/store"
)

func GetRedisClient() (*redis.Client, error) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("connet Redis error: %v\n", err)
	}

	return client, nil
}

func main() {
	db := store.Open(store.DefaultConfig)

	rediscli, err := GetRedisClient()
	if err != nil {
		panic(err)
	}

	go http.ListenAndServe("localhost:6060", nil)

	// Rotom Expired Test
	db.SetEX("test1", "test", time.Second)
	db.SetEX("test2", "test", time.Second*2)
	db.SetEX("test3", "test", time.Second*3)
	db.SetEX("test4", "test", time.Second*4)
	db.SetEX("test5", "test", time.Second*5)

	for i := 0; i < 55; i++ {
		time.Sleep(time.Second / 10)
		fmt.Println(db.Keys())
	}

	// Rotom Set
	a := time.Now()
	for i := 0; i < 100*10000; i++ {
		db.SetEX(gofakeit.Phone(), gofakeit.Uint16(), time.Hour)
	}
	fmt.Println("Rotom Set cost:", time.Since(a))

	// Redis Set
	a = time.Now()
	pipe := rediscli.Pipeline()
	for i := 0; i < 100*10000; i++ {
		pipe.Set(context.Background(), gofakeit.Phone(), gofakeit.Uint16(), time.Hour)
	}
	_, err = pipe.Exec(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("Redis Set cost:", time.Since(a))
}
