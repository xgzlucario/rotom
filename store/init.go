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
	RewriteDuration = time.Second * 10

	// global time
	globalTime = time.Now().UnixNano()

	// database
	db store
)

const (
	INIT uint32 = iota + 1
	NORMAL
	REWRITE
)

type store []*storeShard

type storeShard struct {
	// runtime status
	status uint32

	// store path
	storePath string
	rwPath    string

	// buffer
	buf []byte

	// rw buffer
	rwbuf []byte
	sync.RWMutex

	// data
	*structx.Cache[any]

	// filter
	filter *structx.Bloom
}

func init() {
	// init store dir
	if err := os.MkdirAll(StorePath, os.ModeDir); err != nil {
		panic(err)
	}

	db = make([]*storeShard, ShardCount)

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
	for i := range db {
		// init
		db[i] = &storeShard{
			status:    INIT,
			storePath: fmt.Sprintf("%sdat%d", StorePath, i),
			rwPath:    fmt.Sprintf("%sdat%d.rw", StorePath, i),
			Cache:     structx.NewCache[any](),
		}
		sd := db[i]

		// flush buffer
		go func() {
			tk := time.NewTicker(time.Second)
			for range tk.C {
				if s := sd.getStatus(); s == NORMAL {
					sd.flushBuffer()

				} else if s == REWRITE || s == INIT {
					sd.flushRwBuffer()
				}
			}
		}()

		// rewrite
		go func() {
			tk := time.NewTicker(RewriteDuration)
			for range tk.C {
				rwPool.Go(func() {
					sd.setStatus(REWRITE)
					sd.load()
					sd.setStatus(NORMAL)
				})
			}
		}()

		pool.Go(func() {
			sd.load()
			sd.setStatus(NORMAL)
		})
	}
	pool.Wait()
}

func GlobalTime() int64 {
	return atomic.LoadInt64(&globalTime)
}
