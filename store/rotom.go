package store

import (
	"bytes"
	"encoding/binary"
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
	OP_SET byte = iota + 1
	OP_SETEX
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
	C_SPR   = byte(0x00)
	C_VALID = byte(0x01)
)

type (
	Map    = structx.Map[string, string]
	List   = structx.List[string]
	Set    = structx.Set[string]
	ZSet   = structx.ZSet[string, float64, string]
	RBTree = structx.RBTree[string, string]
	Trie   = structx.Trie[string]
	BitMap = structx.BitMap
)

var (
	globalTime = time.Now().UnixNano()

	lineSpr = []byte{C_VALID, C_SPR, C_SPR, '\n'}

	DefaultConfig = &Config{
		DBDirPath:       "db",
		ShardCount:      64,
		FlushDuration:   time.Second,
		RewriteDuration: time.Second * 10,
	}
)

type store struct {
	*Config
	mask   uint64
	shards []*storeShard
}

type Config struct {
	ShardCount uint64
	DBDirPath  string

	// FlushDuration is the time interval for flushing data to disk
	FlushDuration time.Duration

	// RewriteDuration is the time interval for rewriting data to disk
	RewriteDuration time.Duration
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
		for t := range time.NewTicker(time.Microsecond).C {
			atomic.SwapInt64(&globalTime, t.UnixNano())
		}
	}()
}

func CreateDB(conf *Config) *store {
	db := &store{
		Config: conf,
		mask:   conf.ShardCount - 1,
		shards: make([]*storeShard, conf.ShardCount),
	}

	if err := os.MkdirAll(db.DBDirPath, os.ModeDir); err != nil {
		panic(err)
	}

	// load config
	for i := range db.shards {
		db.shards[i] = &storeShard{
			Coder:  base.NewCoder(nil),
			rwbuf:  base.NewCoder(nil),
			path:   path.Join(db.DBDirPath, "dat"+strconv.Itoa(i)),
			rwPath: path.Join(db.DBDirPath, "rw"+strconv.Itoa(i)),
			Cache:  structx.NewCache[any](),
		}
	}

	// initial
	pool := structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]
		pool.Go(func() {
			sd.load()
		})
	}
	pool.Wait()

	// start worker
	pool = structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]

		// flush worker
		go func() {
			for {
				time.Sleep(db.FlushDuration)

				if sd.rwlock.TryLock() {
					sd.FlushFile(sd.path)
					sd.rwlock.Unlock()
				}
			}
		}()
		// rewrite worker
		go func() {
			for {
				time.Sleep(db.RewriteDuration)
				pool.Go(func() {
					sd.FlushFile(sd.path)
					sd.reWrite()
				})
			}
		}()
	}

	return db
}

// Set
func (s *store) Set(key string, val any) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {SET}{key}|{value}
	sd.EncBytes(OP_SET).EncBytes(base.S2B(&key)...).EncBytes(C_SPR)
	if err := sd.Encode(val); err != nil {
		panic(err)
	}
	sd.EncBytes(lineSpr...)

	sd.Set(key, val)
}

// SetEX
func (s *store) SetEX(key string, val any, ttl time.Duration) {
	sd := s.getShard(key)

	i64ts := atomic.LoadInt64(&globalTime) + int64(ttl)

	sd.Lock()
	defer sd.Unlock()

	// {SETEX}{key}|{ttl}|{value}
	sd.EncBytes(OP_SETEX).EncBytes(base.S2B(&key)...).EncBytes(C_SPR).EncInt64(i64ts).EncBytes(C_SPR)
	if err := sd.Encode(val); err != nil {
		panic(err)
	}
	sd.EncBytes(lineSpr...)

	sd.SetTX(key, val, i64ts)
}

// Remove
func (s *store) Remove(key string) (val any, ok bool) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {REMOVE}{key}
	sd.EncBytes(OP_REMOVE).EncBytes(base.S2B(&key)...).EncBytes(lineSpr...)

	return sd.Remove(key)
}

// Persist removes the expiration from a key
func (s *store) Persist(key string) bool {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {PERSIST}{key}
	sd.EncBytes(OP_PERSIST).EncBytes(base.S2B(&key)...).EncBytes(lineSpr...)

	return sd.Persist(key)
}

// Flush writes all the buf data to disk
func (s *store) Flush() error {
	for _, sd := range s.shards {
		if _, err := sd.FlushFile(sd.path); err != nil {
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
	lines := bytes.Split(data, lineSpr[1:])
	for _, line := range lines {
		s.readLine(line)
	}
}

// reWrite shrink the database
func (s *storeShard) reWrite() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	// dump
	s.Scan(func(k string, i int64, v any) bool {
		s.rwbuf.EncBytes(OP_SETEX).EncBytes(base.S2B(&k)...).EncBytes(C_SPR).EncInt64(i).EncBytes(C_SPR)
		if err := s.rwbuf.Encode(v); err != nil {
			panic(err)
		}
		s.rwbuf.EncBytes(lineSpr...)

		return true
	})

	// flush
	s.Lock()
	defer s.Unlock()

	s.rwbuf.FlushFile(s.rwPath)
	s.FlushFile(s.rwPath)

	// rename rwFile to storeFile
	if err := os.Rename(s.rwPath, s.path); err != nil {
		panic(err)
	}
}

// readLine
func (s *storeShard) readLine(line []byte) {
	n := len(line)
	if n == 0 || line[n-1] != lineSpr[0] {
		return
	}
	line = line[:n-1]

	switch line[0] {
	// {SET}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, C_SPR)
		if i <= 0 {
			return
		}
		s.Set(*base.B2S(line[1:i]), base.Raw(line[i+1:]))

	// {SETEX}{key}|{ttl}|{value}
	case OP_SETEX:
		sp1 := bytes.IndexByte(line, C_SPR)
		sp2 := bytes.IndexByte(line[sp1+1:], C_SPR)
		sp2 += sp1 + 1

		ts, _ := binary.Varint(line[sp1+1 : sp2])

		// not expired
		if ts > atomic.LoadInt64(&globalTime) {
			s.SetTX(*base.B2S(line[1:sp1]), base.Raw(line[sp2+1:]), ts)
		}

	// {REMOVE}{key}
	case OP_REMOVE:
		s.Remove(*base.B2S(line[1:]))

	// {PERSIST}{key}
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
