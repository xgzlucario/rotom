package main

import (
	"fmt"
	"time"

	"github.com/xgzlucario/rotom/store"
)

var db = store.CreateDB(&store.Config{
	DBDirPath:       "db",
	ShardCount:      32,
	FlushDuration:   time.Second,
	RewriteDuration: time.Second * 10,
})

func main() {
	time.Sleep(time.Second)

	fmt.Println(db.Get("xgz").ToInt())

	db.Set("xgz", 123)

	fmt.Println(db.Get("xgz").ToInt())

	db.Flush()

	time.Sleep(time.Second)
}
