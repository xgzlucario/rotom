package store

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

const (
	OP_SETTX byte = iota + 'a'
	OP_REMOVE
	OP_PERSIST
	OP_INCR

	// TODO
	OP_HGET
	OP_HSET
	OP_HREMOVE

	// TODO
	OP_GETBIT
	OP_SETBIT
	OP_COUNTBIT

	// TODO
	OP_LPUSH
	OP_LPOP
	OP_RPUSH
	OP_RPOP
	OP_LLEN
)

const (
	C_SPR     = byte(' ')
	C_END     = byte('\n')
	timeCarry = 1000 * 1000 * 1000
)

type (
	Map    = structx.Map[string, string]
	List   = structx.List[string]
	Set    = structx.Set[string]
	ZSet   = structx.ZSet[string, float64, string]
	BitMap = structx.BitMap
)

var (
	globalTime    = time.Now().UnixNano()
	DefaultConfig = &Config{
		Path:        "db",
		ShardCount:  32,
		AOFInterval: time.Second,
		RDBInterval: time.Second * 10,
	}
)

type store struct {
	*Config
	mask   uint64
	shards []*storeShard
}

type Config struct {
	ShardCount uint64
	Path       string

	SyncPolicy base.SyncPolicy
	// AOFInterval
	AOFInterval time.Duration

	// RDBInterval
	RDBInterval time.Duration
}

type storeShard struct {
	// dbPath and rwPath
	path   string
	rwPath string

	// buffer and rwbuffer
	*base.Coder
	rwbuf *base.Coder

	// data based on Cache
	*structx.Cache[any]

	sync.RWMutex
	rwlock sync.Mutex
}

func init() {
	go func() {
		for t := range time.NewTicker(time.Millisecond).C {
			atomic.SwapInt64(&globalTime, t.UnixNano())
		}
	}()
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf *Config) *store {
	db := &store{
		Config: conf,
		mask:   conf.ShardCount - 1,
		shards: make([]*storeShard, conf.ShardCount),
	}

	if err := os.MkdirAll(db.Path, os.ModeDir); err != nil {
		panic(err)
	}

	// load config
	for i := range db.shards {
		db.shards[i] = &storeShard{
			Coder:  base.NewCoder(nil),
			rwbuf:  base.NewCoder(nil),
			path:   path.Join(db.Path, strconv.Itoa(i)+".rdb"),
			rwPath: path.Join(db.Path, strconv.Itoa(i)+".aof"),
			Cache:  structx.NewCache[any](),
		}
	}

	// initial
	pool := structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]
		pool.Go(func() { sd.load() })
	}
	pool.Wait()

	// start worker
	pool = structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]

		// AOF
		go func() {
			for {
				time.Sleep(db.AOFInterval)

				if sd.rwlock.TryLock() {
					sd.WriteTo(sd.path)
					sd.rwlock.Unlock()
				}
			}
		}()
		// RDB
		go func() {
			for {
				time.Sleep(db.RDBInterval)
				pool.Go(func() {
					sd.WriteTo(sd.path)
					sd.rdb()
				})
			}
		}()
	}

	return db
}

// Close
func (db *store) Close() error {
	return db.Flush()
}

// Set
func (s *store) Set(key string, val any) {
	s.SetTX(key, val, math.MaxInt64)
}

// SetEX
func (s *store) SetEX(key string, val any, ttl time.Duration) {
	s.SetTX(key, val, atomic.LoadInt64(&globalTime)+int64(ttl))
}

// SetTX
func (s *store) SetTX(key string, val any, ts int64) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	sd.Enc(OP_SETTX)
	sd.EncodeBytes(C_SPR, base.S2B(&key)...)
	sd.EncodeInt64(C_SPR, ts/timeCarry)
	if err := sd.Encode(val, C_END); err != nil {
		panic(err)
	}

	sd.SetTX(key, val, ts)
}

// Remove
func (s *store) Remove(key string) (val any, ok bool) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	sd.Enc(OP_REMOVE).EncodeBytes(C_SPR, base.S2B(&key)...).Enc(C_END)

	return sd.Remove(key)
}

// Persist removes the expiration from a key
func (s *store) Persist(key string) bool {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	sd.Enc(OP_PERSIST).EncodeBytes(C_SPR, base.S2B(&key)...).Enc(C_END)

	return sd.Persist(key)
}

// Flush writes all the buf data to disk
func (s *store) Flush() error {
	for _, sd := range s.shards {
		if _, err := sd.WriteTo(sd.path); err != nil {
			return err
		}
	}
	return nil
}

// Count
func (s *store) Count() (sum int) {
	for _, s := range s.shards {
		sum += s.Count()
	}

	return sum
}

// Keys
func (s *store) Keys() []string {
	arr := make([]string, 0, s.Count())
	for _, s := range s.shards {
		arr = append(arr, s.Keys()...)
	}

	return arr
}

// load redo operation from file
func (s *storeShard) load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}

	// read line
	lines := bytes.Split(data, []byte{C_END})
	for _, line := range lines {
		s.readLine(line)
	}
}

// rdb dump snapshot to disk
func (s *storeShard) rdb() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	// dump
	a := time.Now()
	s.Scan(func(k string, i int64, v any) bool {
		s.rwbuf.Enc(OP_SETTX)
		s.rwbuf.EncodeBytes(C_SPR, base.S2B(&k)...)
		s.rwbuf.EncodeInt64(C_SPR, i/timeCarry)
		if err := s.rwbuf.Encode(v, C_END); err != nil {
			panic(err)
		}

		return true
	})
	fmt.Println("rdb cost:", time.Since(a))

	// flush
	s.Lock()
	defer s.Unlock()

	s.rwbuf.WriteTo(s.rwPath)
	s.WriteTo(s.rwPath)

	// rename rwFile to storeFile
	if err := os.Rename(s.rwPath, s.path); err != nil {
		panic(err)
	}
}

// readLine
func (s *storeShard) readLine(line []byte) {
	if len(line) == 0 {
		return
	}

	// parse key

	switch line[0] {
	case OP_SETTX:
		sp1 := bytes.IndexByte(line, C_SPR)
		sp2 := bytes.IndexByte(line[sp1+1:], C_SPR)
		sp2 += sp1 + 1

		ts, err := strconv.ParseInt(*base.B2S(line[sp1+1 : sp2]), 36, 64)
		if err != nil {
			panic(err)
		}
		ts *= timeCarry

		// not expired
		if ts > atomic.LoadInt64(&globalTime) {
			s.SetTX(*base.B2S(line[1:sp1]), base.Raw(line[sp2+1:]), ts)
		}

	case OP_REMOVE:
		s.Remove(*base.B2S(line[1:]))

	case OP_PERSIST:
		s.Persist(*base.B2S(line[1:]))
	}
}

// getShard
func (s *store) getShard(key string) *storeShard {
	return s.shards[xxh3.HashString(key)&(s.ShardCount-1)]
}

// Get
func (s *store) Get(key string) Value {
	sd := s.getShard(key)
	val, ok := sd.Cache.Get(key)
	if !ok {
		return Value{}
	}

	if raw, isRaw := val.(base.Raw); isRaw {
		return Value{raw: raw, key: key, sd: sd}

	} else {
		return Value{val: val, key: key, sd: sd}
	}
}
