package store

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/xgzlucario/rotom/structx"
)

const (
	DB_SHARD_COUNT = 32
)

var (
	// database store path
	StorePath = "db/"
)

type storeShard struct {
	storePath string
	logger    *log.Logger
	*structx.Cache[any]
}

type store struct {
	shards []*storeShard
}

// database
var db *store

func init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	p := structx.NewPool().WithMaxGoroutines(runtime.NumCPU())

	db = &store{shards: make([]*storeShard, DB_SHARD_COUNT)}
	// load
	for i := range db.shards {
		i := i
		p.Go(func() {
			db.shards[i] = &storeShard{
				storePath: fmt.Sprintf("%s%d.log", StorePath, i),
				Cache:     structx.NewCache[any](),
			}
			db.shards[i].logger = NewLogger(db.shards[i].storePath)
			db.shards[i].load()
		})
	}

	p.Wait()
}
