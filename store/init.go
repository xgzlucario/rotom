package store

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
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
	storePath   string
	rewritePath string

	// buffer
	buffer   *bytes.Buffer
	rwBuffer *bytes.Buffer

	// data
	sync.Mutex
	*structx.Cache[any]

	// bloom filter
	filter *structx.Bloom

	logger *log.Logger
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

	db = &store{
		shards: make([]*storeShard, ShardCount),
	}
	logger := log.New(os.Stdout, "[store] ", log.LstdFlags)

	// init globalTime
	base.NewBackWorker(time.Millisecond, func(t time.Time) {
		atomic.SwapInt64(&globalTime, t.UnixNano())
	})

	// load
	for i := range db.shards {
		i := i
		p.Go(func() {
			// init
			db.shards[i] = &storeShard{
				storePath:   fmt.Sprintf("%sdat%d", StorePath, i),
				rewritePath: fmt.Sprintf("%sdat%d.rw", StorePath, i),
				buffer:      bytes.NewBuffer(nil),
				rwBuffer:    bytes.NewBuffer(nil),
				logger:      logger,
				Cache:       structx.NewCache[any](),
			}
			shard := db.shards[i]

			// rewrite buffer
			base.NewBackWorker(time.Second, func(t time.Time) {
				shard.ReWriteBuffer()
			})

			// load
			shard.load()

			// write buffer
			base.NewBackWorker(time.Second, func(t time.Time) {
				shard.WriteBuffer()
			})
		})
	}
	p.Wait()

	// rewriter
	base.NewBackWorker(RewriteDuration, func(t time.Time) {
		for _, sd := range db.shards {
			sd.WriteBuffer()
			sd.load()
		}
	})
}

func GlobalTime() int64 {
	return atomic.LoadInt64(&globalTime)
}
