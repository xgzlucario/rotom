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
	logger *log.Logger
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
			storePath := fmt.Sprintf("%s%d.log", StorePath, i)

			db.shards[i] = &storeShard{
				logger: newLogger(storePath),
				Cache:  structx.NewCache[any](),
			}
			db.shards[i].load(storePath)
		})
	}

	p.Wait()
}

func newLogger(path string) *log.Logger {
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return log.New(writer, "", 0)
}
