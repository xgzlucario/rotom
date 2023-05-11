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
	OP_SET byte = iota + 'A'
	OP_SETTX
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
	C_SPR     = byte('\n')
	timeCarry = 1000 * 1000 * 1000
	noTTL     = math.MaxInt64
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
		RDBInterval: time.Second * 15,
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
	// path
	path   string
	rwPath string

	// buffer
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
			path:   path.Join(db.Path, strconv.Itoa(i)+".db"),
			rwPath: path.Join(db.Path, strconv.Itoa(i)+".db-tmp"),
			Cache:  structx.NewCache[any](),
		}
	}

	// initial
	pool := structx.NewDefaultPool()
	for i := range db.shards {
		s := db.shards[i]
		pool.Go(func() { s.load() })
	}
	pool.Wait()

	// start worker
	pool = structx.NewDefaultPool()
	for i := range db.shards {
		s := db.shards[i]

		// AOF
		go func() {
			for {
				time.Sleep(db.AOFInterval)

				if s.rwlock.TryLock() {
					s.Lock()
					s.WriteTo(s.path)
					s.Unlock()
					s.rwlock.Unlock()
				}
			}
		}()
		// RDB
		go func() {
			for {
				time.Sleep(db.RDBInterval)
				pool.Go(func() {
					s.WriteTo(s.path)
					s.dump()
				})
			}
		}()
	}

	return db
}

// Set
func (db *store) Set(key string, val any) error {
	if len(key) == 0 {
		return base.ErrKeyIsEmpty
	}
	s := db.getShard(key)
	s.Lock()
	defer s.Unlock()

	s.Enc(OP_SET)
	s.EncodeBytes(C_SPR, base.S2B(&key)...)
	if err := s.Encode(val, C_SPR); err != nil {
		return err
	}

	s.Set(key, val)
	return nil
}

// SetEX
func (db *store) SetEX(key string, val any, ttl time.Duration) error {
	return db.SetTX(key, val, atomic.LoadInt64(&globalTime)+int64(ttl))
}

// SetTX
func (db *store) SetTX(key string, val any, ts int64) error {
	if len(key) == 0 {
		return base.ErrKeyIsEmpty
	}
	s := db.getShard(key)
	s.Lock()
	defer s.Unlock()

	s.Enc(OP_SETTX)
	s.EncodeBytes(C_SPR, base.S2B(&key)...)
	s.EncodeInt64(C_SPR, ts/timeCarry)
	if err := s.Encode(val, C_SPR); err != nil {
		return err
	}

	s.SetTX(key, val, ts)
	return nil
}

// Remove
func (db *store) Remove(key string) (val any, ok bool) {
	s := db.getShard(key)
	s.Lock()
	defer s.Unlock()

	s.Enc(OP_REMOVE).EncodeBytes(C_SPR, base.S2B(&key)...).Enc(C_SPR)

	return s.Remove(key)
}

// Persist
func (db *store) Persist(key string) bool {
	s := db.getShard(key)
	s.Lock()
	defer s.Unlock()

	s.Enc(OP_PERSIST).EncodeBytes(C_SPR, base.S2B(&key)...).Enc(C_SPR)

	return s.Persist(key)
}

// Flush
func (db *store) Flush() error {
	for _, s := range db.shards {
		if _, err := s.WriteTo(s.path); err != nil {
			return err
		}
	}
	return nil
}

// Count
func (db *store) Count() (sum int) {
	for _, s := range db.shards {
		sum += s.Count()
	}

	return sum
}

// Keys
func (db *store) Keys() []string {
	arr := make([]string, 0, db.Count())
	for _, s := range db.shards {
		arr = append(arr, s.Keys()...)
	}

	return arr
}

// load
func (s *storeShard) load() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	s.Lock()
	defer s.Unlock()

	line, err := os.ReadFile(s.path)
	if err != nil {
		return
	}

	for len(line) > 1 {
		op := line[0]
		line = line[1:]

		// parse key
		var key []byte
		key, line = parseLine(line, C_SPR)

		switch op {
		case OP_SET:
			var val []byte
			// parse val
			val, line = parseLine(line, C_SPR)
			s.Set(*base.B2S(key), base.Raw(val))

		case OP_SETTX:
			var ttl, val []byte

			// parse ttl
			ttl, line = parseLine(line, C_SPR)
			ts, err := strconv.ParseInt(*base.B2S(ttl), 36, 64)
			if err != nil {
				panic(err)
			}
			ts *= timeCarry

			// parse val
			val, line = parseLine(line, C_SPR)
			if ts > atomic.LoadInt64(&globalTime) {
				s.SetTX(*base.B2S(key), base.Raw(val), ts)
			}

		case OP_REMOVE:
			s.Remove(*base.B2S(key))

		case OP_PERSIST:
			s.Persist(*base.B2S(key))

		default:
			err := fmt.Errorf("%v: %c", base.ErrUnknownOperationType, op)
			panic(err)
		}
	}
}

// dump
func (s *storeShard) dump() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	// dump
	s.Scan(func(k string, v any, i int64) bool {
		if i == noTTL {
			// SET
			s.rwbuf.Enc(OP_SET).EncodeBytes(C_SPR, base.S2B(&k)...)
		} else {
			// SETEX
			s.rwbuf.Enc(OP_SETTX).EncodeBytes(C_SPR, base.S2B(&k)...).EncodeInt64(C_SPR, i/timeCarry)
		}
		if err := s.rwbuf.Encode(v, C_SPR); err != nil {
			panic(err)
		}
		return true
	})

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

// parseLine
func parseLine(line []byte, valid byte) (pre []byte, suf []byte) {
	i := bytes.IndexByte(line, ':')
	if i <= 0 {
		panic("cut line error: i <= 0")
	}
	l, err := strconv.ParseInt(*base.B2S(line[:i]), 36, 64)
	if err != nil {
		panic(err)
	}
	i++

	if line[i+int(l)] != valid {
		panic(base.ErrParseAOFLine)
	}

	pre = line[i : i+int(l)]
	suf = line[i+int(l)+1:]
	return
}

// getShard
func (db *store) getShard(key string) *storeShard {
	return db.shards[xxh3.HashString(key)&(db.ShardCount-1)]
}

// Get
func (db *store) Get(key string) Value {
	s := db.getShard(key)
	val, ok := s.Cache.Get(key)
	if !ok {
		return Value{}
	}

	if raw, isRaw := val.(base.Raw); isRaw {
		return Value{raw: raw, key: key, s: s}

	} else {
		return Value{val: val, key: key, s: s}
	}
}

type Value struct {
	key string
	s   *storeShard
	raw []byte
	val any
}

func (v Value) ToInt() (r int, e error) { return getValue(v, r) }

func (v Value) ToInt64() (r int64, e error) { return getValue(v, r) }

func (v Value) ToUint() (r uint, e error) { return getValue(v, r) }

func (v Value) ToUint32() (r uint32, e error) { return getValue(v, r) }

func (v Value) ToUint64() (r uint64, e error) { return getValue(v, r) }

func (v Value) ToFloat64() (r float64, e error) { return getValue(v, r) }

func (v Value) ToString() (r string, e error) { return getValue(v, r) }

func (v Value) ToIntSlice() (r []int, e error) { return getValue(v, r) }

func (v Value) ToStringSlice() (r []string, e error) { return getValue(v, r) }

func (v Value) ToTime() (r time.Time, e error) { return getValue(v, r) }

func (v Value) Scan(val any) error {
	_, err := getValue(v, val)
	return err
}

// getValue
func getValue[T any](v Value, vptr T) (T, error) {
	if v.raw != nil {
		if err := v.s.Decode(v.raw, &vptr); err != nil {
			return vptr, err
		}

		v.s.Set(v.key, vptr)
		return vptr, nil
	}

	if tmp, ok := v.val.(T); ok {
		return tmp, nil

	} else if v.key == "" {
		return vptr, base.ErrKeyNotFound

	} else {
		return vptr, base.ErrWrongType
	}
}
