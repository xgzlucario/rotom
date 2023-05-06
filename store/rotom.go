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
	C_SPR = byte(0x00)
	C_END = byte(0xff)

	timeCarry = 1000 * 1000 * 1000
)

var (
	globalTime = time.Now().UnixNano()

	lineSpr = []byte{C_SPR, C_SPR, C_END}

	DefaultConfig = &Config{
		DBDirPath:       "db",
		ShardCount:      32,
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

	// filter
	filter *structx.Bloom

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
			sd.reWrite(true)
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
	u32ts := uint32(i64ts / timeCarry)

	sd.Lock()
	defer sd.Unlock()

	// {SETEX}{key}|{ttl}|{value}
	sd.EncBytes(OP_SETEX).EncBytes(base.S2B(&key)...).EncBytes(C_SPR).EncUint32(u32ts).EncBytes(C_SPR)
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

// WithExpired
func (s *store) WithExpired(f func(string, any, int64)) *store {
	for _, s := range s.shards {
		s.WithExpired(f)
	}

	return s
}

// Keys
func (s *store) Keys() []string {
	arr := make([]string, 0, s.Count())
	for _, s := range s.shards {
		arr = append(arr, s.Keys()...)
	}

	return arr
}

// reWrite shrink the database
func (s *storeShard) reWrite(initial ...bool) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}

	// init filter
	s.filter = structx.NewBloom()

	// read line from tail
	lines := bytes.Split(data, []byte{C_SPR, C_END})
	init := len(initial) > 0 && initial[0]

	for i := len(lines) - 1; i >= 0; i-- {
		s.readLine(lines[i], init)
	}

	// flush
	s.Lock()
	defer s.Unlock()

	s.FlushFile(s.rwPath)
	s.rwbuf.FlushFile(s.rwPath)

	// rename rwFile to storeFile
	if err := os.Rename(s.rwPath, s.path); err != nil {
		panic(err)
	}
}

// readLine
func (s *storeShard) readLine(line []byte, init bool) {
	// line validate
	n := len(line)
	if n == 0 || line[n-1] != C_SPR {
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
		if !s.testAdd(line[1:i]) {
			return
		}

		s.rwbuf.EncBytes(line...).EncBytes(lineSpr...)

		if init {
			s.Set(*base.B2S(line[1:i]), base.Raw(line[i+1:]))
		}

	// {SETEX}{key}|{ttl}|{value}
	case OP_SETEX:
		sp1 := bytes.IndexByte(line, C_SPR)
		sp2 := bytes.IndexByte(line[sp1+1:], C_SPR)
		sp2 += sp1 + 1

		if !s.testAdd(line[1:sp1]) {
			return
		}

		u64ts, _ := binary.Uvarint(line[sp1+1 : sp2])
		ts := int64(u64ts) * timeCarry

		// not expired
		if ts > atomic.LoadInt64(&globalTime) {
			s.rwbuf.EncBytes(line...).EncBytes(lineSpr...)

			if init {
				s.SetTX(*base.B2S(line[1:sp1]), base.Raw(line[sp2+1:]), ts)
			}
		}

	// {REMOVE}{key}
	case OP_REMOVE:
		if !s.testAdd(line[1:]) {
			return
		}
		if init {
			s.Remove(*base.B2S(line[1:]))
		}

	// {PERSIST}{key}
	case OP_PERSIST:
		if !s.testAdd(line) {
			return
		}
		s.rwbuf.EncBytes(line...).EncBytes(lineSpr...)

		if init {
			s.Persist(*base.B2S(line[1:]))
		}
	}
}

// testAdd
func (s *storeShard) testAdd(line []byte) bool {
	if s.filter.Test(line) {
		return false
	}
	s.filter.Add(line)
	return true
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
