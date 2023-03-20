package store

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

var (
	// StorePath Of DB File
	StorePath = "db/"

	// ShardCount
	ShardCount uint64 = 32

	// PersistDuration
	PersistDuration = time.Second

	// RewriteDuration
	RewriteDuration = time.Minute
)

type storeShard struct {
	storePath string
	buffer    []byte
	rw        *os.File

	sync.Mutex
	*structx.Cache[any]
}

type store struct {
	shards []*storeShard
}

// database
var db *store

func Init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	p := structx.NewPool().WithMaxGoroutines(runtime.NumCPU())

	db = &store{shards: make([]*storeShard, ShardCount)}
	// load
	for i := range db.shards {
		i := i
		p.Go(func() {
			// init
			path := fmt.Sprintf("%sdat%d", StorePath, i)
			db.shards[i] = &storeShard{
				storePath: path,
				rw:        newWriter(path),
				Cache:     structx.NewCache[any](),
			}

			shard := db.shards[i]

			// load
			shard.load()

			// write
			go func() {
				for {
					time.Sleep(PersistDuration)
					shard.writeBufferBlock()
				}
			}()

			// rewrite
			go func() {
				for {
					time.Sleep(RewriteDuration)
					shard.rewrite()
				}
			}()
		})
	}
	p.Wait()
}
