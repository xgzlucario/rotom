package store

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

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

	db = &store{shards: make([]*storeShard, DB_SHARD_COUNT)}
	// load
	for i := range db.shards {
		i := i
		p.Go(func() {
			// init
			storePath := fmt.Sprintf("%s%d.log", StorePath, i)

			db.shards[i] = &storeShard{
				rw:     newWriter(storePath),
				buffer: make([]byte, 0),
				Cache:  structx.NewCache[any](),
			}

			// load
			db.shards[i].load(storePath)

			// write buffer
			go func() {
				for {
					time.Sleep(time.Second)
					db.shards[i].writeBuffer()
				}
			}()
		})
	}
	p.Wait()
}
