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

	// Enabled Zstd Compressor
	EnabledCompress = true

	// AOF duration
	AOFDuration = time.Second
)

type storeShard struct {
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
			storePath := fmt.Sprintf("%sdat%d", StorePath, i)

			db.shards[i] = &storeShard{
				rw:    newWriter(storePath),
				Cache: structx.NewCache[any](),
			}

			// load
			db.shards[i].load(storePath)

			// write
			go func() {
				for {
					time.Sleep(AOFDuration)
					db.shards[i].writeBufferBlock()
				}
			}()
		})
	}
	p.Wait()
}
