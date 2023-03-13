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
	alive     bool
	storePath string

	rw     *os.File
	buffer []byte

	sync.Mutex
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
			shard.alive = true

			// write
			go func() {
				for shard.alive {
					time.Sleep(PersistDuration)
					shard.writeBufferBlock()
				}
			}()

			// rewrite
			go func() {
				for shard.alive {
					time.Sleep(RewriteDuration)
					shard.rewrite()
				}
			}()
		})
	}
	p.Wait()
}
