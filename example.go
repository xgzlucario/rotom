package main

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/xgzlucario/rotom/store"
)

func example() {
	db, err := store.Open(store.DefaultConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Set
	for i := 0; i < 10000; i++ {
		phone := gofakeit.Phone()
		user := gofakeit.Username()

		// Set bytes
		db.Set(phone, []byte(user))

		// Or set with ttl
		db.SetEx(phone, []byte(user), time.Minute)

		// Or set with deadline
		db.SetTx(phone, []byte(user), time.Now().Add(time.Minute).UnixNano())
	}

	fmt.Println("now db length is", db.Stat().Len)

	// Get
	key := gofakeit.Phone()
	user, ttl, ok := db.Get(key)
	if ok {
		fmt.Println(string(user), ttl)
	} else {
		fmt.Println("key", key, "is not exist or expired")
	}
}
