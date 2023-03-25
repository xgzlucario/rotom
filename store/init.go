package store

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourcegraph/conc/pool"
	"github.com/xgzlucario/rotom/structx"
)

var (
	// StorePath for db file
	StorePath = "db/"

	// ShardCount
	ShardCount uint64 = 32

	// PersistDuration
	PersistDuration = time.Second

	// RewriteDuration
	RewriteDuration = time.Minute
)

var (
	globalTime = time.Now().UnixNano()
)

type storeShard struct {
	storePath string
	rwPath    string

	// buffer
	buffer   *bytes.Buffer
	rwBuffer *bytes.Buffer

	// data
	sync.Mutex
	*structx.Cache[any]

	// bloom filter
	filter *structx.Bloom
}

type store struct {
	shards []*storeShard

	pool *pool.Pool

	// bloom filter
	filter *structx.Bloom
}

// database
var db *store

func init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	db = &store{
		shards: make([]*storeShard, ShardCount),
		filter: structx.NewBloom(),
		pool:   structx.NewPool().WithMaxGoroutines(runtime.NumCPU()),
	}

	// init global time
	go func() {
		for {
			time.Sleep(time.Millisecond)
			atomic.SwapInt64(&globalTime, time.Now().UnixNano())
		}
	}()

	// load
	for i := range db.shards {
		// init
		db.shards[i] = &storeShard{
			storePath: fmt.Sprintf("%sdat%d", StorePath, i),
			rwPath:    fmt.Sprintf("%sdat%d.rw", StorePath, i),
			buffer:    bytes.NewBuffer(nil),
			rwBuffer:  bytes.NewBuffer(nil),
			Cache:     structx.NewCache[any](),
			filter:    db.filter,
		}
		sd := db.shards[i]

		// write buffer
		go func() {
			for {
				time.Sleep(time.Second)
				sd.WriteBuffer()
			}
		}()

		// rewrite buffer
		go func() {
			for {
				time.Sleep(time.Second)
				sd.ReWriteBuffer()
			}
		}()

		db.pool.Go(func() { sd.load() })
	}
	db.pool.Wait()

	// reinit
	db.pool = structx.NewPool().WithMaxGoroutines(runtime.NumCPU())

	// rewriter
	go func() {
		for {
			time.Sleep(RewriteDuration)
			db.filter.ClearAll()
			for _, sd := range db.shards {
				db.pool.Go(func() {
					sd := sd
					sd.WriteBuffer()
					sd.load()
				})
			}
		}
	}()
}

func GlobalTime() int64 {
	return atomic.LoadInt64(&globalTime)
}
