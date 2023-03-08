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

	db = &store{shards: make([]*storeShard, DB_SHARD_COUNT)}

	p := structx.NewPool().WithMaxGoroutines(runtime.NumCPU())

	for i := range db.shards {
		i := i
		db.shards[i] = &storeShard{
			storePath: fmt.Sprintf("%s%d.log", StorePath, i),
			Cache:     structx.NewCache[any](),
		}

		db.shards[i].logger = NewLogger(db.shards[i].storePath)

		// load
		p.Go(func() {
			db.shards[i].load()
		})
	}
	p.Wait()
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
