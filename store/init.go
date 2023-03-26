package store

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

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

// Status for runtime
type Status byte

const (
	ERR Status = iota
	START
	READY
	REWRITE
)

var (
	globalTime = time.Now().UnixNano()
)

type storeShard struct {
	status    Status
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
	}
	pool := structx.NewPool().WithMaxGoroutines(runtime.NumCPU())
	rwPool := structx.NewPool().WithMaxGoroutines(runtime.NumCPU())

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
			status:    START,
			storePath: fmt.Sprintf("%sdat%d", StorePath, i),
			rwPath:    fmt.Sprintf("%sdat%d.rw", StorePath, i),
			buffer:    bytes.NewBuffer(nil),
			rwBuffer:  bytes.NewBuffer(nil),
			Cache:     structx.NewCache[any](),
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

		// rewrite
		go func() {
			for {
				time.Sleep(RewriteDuration)
				sd.status = REWRITE
				rwPool.Go(func() {
					sd.WriteBuffer()
					sd.load()
					sd.status = READY
				})
			}
		}()

		pool.Go(func() {
			sd.load()
			sd.status = READY
		})
	}
	pool.Wait()
}

func GlobalTime() int64 {
	return atomic.LoadInt64(&globalTime)
}
