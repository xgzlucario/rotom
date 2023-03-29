package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/structx"
)

var (
	// StorePath for db file
	StorePath = "db/"

	// ShardCount for db
	ShardCount uint64 = 32

	// FlushDuration is the time interval for flushing data to disk
	FlushDuration = time.Second

	// RewriteDuration is the time interval for rewriting data to disk
	RewriteDuration = time.Minute

	// default buffer size
	defaultBufSize = 4096

	// global time
	globalTime = time.Now().UnixNano()

	// database
	db *store
)

// Status for runtime
type Status byte

const (
	ERR Status = iota
	START
	READY
	REWRITE
)

type store struct {
	shards []*storeShard
}

type storeShard struct {
	status    Status
	storePath string
	rwPath    string

	// buffer
	buf []byte

	// data
	sync.Mutex
	*structx.Cache[any]

	// bloom filter
	filter *structx.Bloom
}

func init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	db = &store{
		shards: make([]*storeShard, ShardCount),
	}

	pool := structx.NewDefaultPool()
	rwPool := structx.NewDefaultPool()

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
			buf:       make([]byte, 0, defaultBufSize),
			Cache:     structx.NewCache[any](),
		}
		sd := db.shards[i]
		sd.setStatus(START)

		// write buffer
		go func() {
			for {
				time.Sleep(time.Second)

				if sd.getStatus() == REWRITE {
					sd.ReWriteBuffer()
				} else {
					sd.WriteBuffer()
				}
			}
		}()

		// rewrite
		go func() {
			for {
				time.Sleep(RewriteDuration)
				sd.setStatus(REWRITE)

				rwPool.Go(func() {
					sd.WriteBuffer()
					sd.load()
					sd.setStatus(READY)
				})
			}
		}()

		pool.Go(func() {
			sd.load()
			sd.setStatus(READY)
		})
	}
	pool.Wait()
}

func GlobalTime() int64 {
	return atomic.LoadInt64(&globalTime)
}
