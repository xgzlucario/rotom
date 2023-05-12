// Package store provides an in-memory key-value database.
package store

import (
	"bytes"
	"context"
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

type Operation byte

// Operation types.
const (
	OpSet Operation = iota + 'A'
	OpSetTx
	OpRemove
	OpPersist
	OpIncr
	// TODO: Implement these operations.
	OpHSet
	OpHRemove
	OpSetBit
	OpLPush
	OpLPop
	OpRPush
	OpRPop
)

const (
	recordSepChar = byte('\n')
	timeCarry     = 1000 * 1000 * 1000
	noTTL         = math.MaxInt64
)

// Type aliases for structx types.
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
		Path:            "db",
		ShardCount:      32,
		SyncPolicy:      base.EverySecond,
		SyncInterval:    time.Second,
		RewriteInterval: time.Second * 30,
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount uint64
	Path       string

	SyncPolicy base.SyncPolicy

	// Interval of persistence.
	SyncInterval    time.Duration
	RewriteInterval time.Duration
}

// Store represents a key-value store.
type Store struct {
	*Config
	mask   uint64
	shards []*storeShard
}

// storeShard represents a shard in the Store.
type storeShard struct {
	syncPolicy base.SyncPolicy // sync policy

	path   string
	rwPath string // path for rewrite

	buf   *bytes.Buffer
	rwbuf *bytes.Buffer // buffer for rewrite

	*structx.Cache[any] // based on Cache

	sync.RWMutex
}

// Init the package by updates globalTime.
func init() {
	go func() {
		for t := range time.NewTicker(time.Millisecond).C {
			atomic.SwapInt64(&globalTime, t.UnixNano())
		}
	}()
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf *Config) (*Store, error) {
	db := &Store{
		Config: conf,
		mask:   conf.ShardCount - 1,
		shards: make([]*storeShard, conf.ShardCount),
	}

	if err := os.MkdirAll(db.Path, os.ModeDir); err != nil {
		return nil, err
	}

	// Load configuration
	for i := range db.shards {
		db.shards[i] = &storeShard{
			syncPolicy: conf.SyncPolicy,
			buf:        bytes.NewBuffer(nil),
			rwbuf:      bytes.NewBuffer(nil),
			path:       path.Join(db.Path, strconv.Itoa(i)+".db"),
			rwPath:     path.Join(db.Path, strconv.Itoa(i)+".db-rw"),
			Cache:      structx.NewCache[any](),
		}
	}

	// Initialize
	pool := structx.NewDefaultPool()
	for i := range db.shards {
		s := db.shards[i]
		pool.Go(func() { s.load() })
	}
	pool.Wait()

	// Start worker
	for _, s := range db.shards {
		s := s
		base.Go(context.Background(), db.SyncInterval, func() {
			s.writeTo(s.buf, s.path)
		})
		base.Go(context.Background(), db.RewriteInterval, func() {
			s.dump()
		})
	}

	return db, nil
}

// Set sets a key-value pair in the database.
func (db *Store) Set(key string, val any) error {
	cd := NewCoder(OpSet).String(key).Any(val)
	defer putCoder(cd)
	if cd.err != nil {
		return cd.err
	}

	sd := db.getShard(key)
	sd.write(cd.buf)
	sd.Set(key, val)

	return nil
}

// SetEx sets a key-value pair with TTL (Time To Live) in the database.
func (db *Store) SetEx(key string, val any, ttl time.Duration) error {
	return db.SetTx(key, val, atomic.LoadInt64(&globalTime)+int64(ttl))
}

// SetTx sets a key-value pair with expiry time in the database.
func (db *Store) SetTx(key string, val any, ts int64) error {
	cd := NewCoder(OpSetTx).String(key).Int64(ts / timeCarry).Any(val)
	defer putCoder(cd)
	if cd.err != nil {
		return cd.err
	}

	sd := db.getShard(key)
	sd.write(cd.buf)
	sd.SetTx(key, val, ts)

	return nil
}

// Remove removes a key-value pair from the database and return it.
func (db *Store) Remove(key string) (val any, ok bool) {
	cd := NewCoder(OpRemove).String(key)
	defer putCoder(cd)

	sd := db.getShard(key)
	sd.write(cd.buf)

	return sd.Remove(key)
}

// Persist persists a key-value pair in the database.
func (db *Store) Persist(key string) bool {
	cd := NewCoder(OpPersist).String(key)
	defer putCoder(cd)

	sd := db.getShard(key)
	sd.write(cd.buf)

	return sd.Persist(key)
}

// HGet gets a value from a hashmap.
func (db *Store) HGet(key, field string) (string, error) {
	sd := db.getShard(key)

	hmap, err := sd.Get(key).ToHMap()
	if err != nil {
		return "", err
	}
	res, ok := hmap.Get(field)
	if !ok {
		return "", base.ErrFieldNotFound
	}
	return res, nil
}

// HSet sets a key-value pair to a hashmap.
func (db *Store) HSet(key, field, val string) error {
	cd := NewCoder(OpHSet).String(key).String(field).String(val)
	defer putCoder(cd)

	sd := db.getShard(key)
	sd.write(cd.buf)

	hmap, err := sd.Get(key).ToHMap()
	hmap.Set(field, val)
	if err != nil {
		sd.Set(key, hmap)
	}

	return nil
}

// Flush writes all the data in the buffer to the disk.
func (db *Store) Flush() error {
	for _, sd := range db.shards {
		if _, err := sd.writeTo(sd.buf, sd.path); err != nil {
			return err
		}
	}
	return nil
}

// Size returns the total size of the data in the database.
// It is not as accurate as Count because it may include expired but not obsolete key-value pairs.
func (db *Store) Size() (sum int) {
	for _, sd := range db.shards {
		sum += sd.Size()
	}
	return sum
}

// Count returns the total number of key-value pairs in the database.
func (db *Store) Count() (sum int) {
	for _, sd := range db.shards {
		sum += sd.Count()
	}
	return sum
}

// write writes the data into the buffer.
func (s *storeShard) write(buf []byte) {
	s.Lock()
	defer s.Unlock()

	s.buf.Write(buf)
}

// writeTo writes the buffer into the file at the specified path.
func (s *storeShard) writeTo(buf *bytes.Buffer, path string) (int64, error) {
	s.Lock()
	defer s.Unlock()

	if buf.Len() == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fs.Close()

	n, err := buf.WriteTo(fs)
	if err != nil {
		return 0, err
	}

	buf.Reset()
	return n, nil
}

// load reads the persisted data from the shard file and loads it into memory.
func (s *storeShard) load() {
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
		key, line = parseLine(line, recordSepChar)

		switch Operation(op) {
		case OpSet:
			var val []byte
			// parse val
			val, line = parseLine(line, recordSepChar)
			s.Set(*base.B2S(key), base.Raw(val))

		case OpSetTx:
			var ttl, val []byte

			// parse ttl
			ttl, line = parseLine(line, recordSepChar)
			ts, err := strconv.ParseInt(*base.B2S(ttl), _base, 64)
			if err != nil {
				panic(err)
			}
			ts *= timeCarry

			// parse val
			val, line = parseLine(line, recordSepChar)
			if ts > atomic.LoadInt64(&globalTime) {
				s.SetTx(*base.B2S(key), base.Raw(val), ts)
			}

		case OpHSet:
			var field, val []byte

			// parse field
			field, line = parseLine(line, recordSepChar)

			// parse val
			val, line = parseLine(line, recordSepChar)

			hmap, err := s.get(*base.B2S(key)).ToHMap()
			hmap.Set(*base.B2S(field), *base.B2S(val))
			// override
			if err != nil {
				s.Set(*base.B2S(key), hmap)
			}

		case OpRemove:
			s.Remove(*base.B2S(key))

		case OpPersist:
			s.Persist(*base.B2S(key))

		default:
			panic(fmt.Errorf("%v: %c", base.ErrUnknownOperationType, op))
		}
	}
}

// dump dumps the current state of the shard to the file.
func (s *storeShard) dump() {
	if s.syncPolicy == base.Never {
		return
	}

	// dump current state
	s.Scan(func(key string, v any, i int64) bool {
		if i == noTTL {
			// Set
			if cd := NewCoder(OpSet).String(key).Any(v); cd.err == nil {
				s.rwbuf.Write(cd.buf)
				putCoder(cd)
			}
		} else {
			// SetTx
			if cd := NewCoder(OpSetTx).String(key).Int64(i / timeCarry).Any(v); cd.err == nil {
				s.rwbuf.Write(cd.buf)
				putCoder(cd)
			}
		}
		return true
	})

	// Flush buffer to file
	s.writeTo(s.rwbuf, s.rwPath)
	s.writeTo(s.buf, s.rwPath)

	// Rename rewrite file to the shard file
	os.Rename(s.rwPath, s.path)
}

// parseLine parse file content to record lines
func parseLine(line []byte, valid byte) (pre []byte, suf []byte) {
	i := bytes.IndexByte(line, ':')
	if i <= 0 {
		panic(base.ErrParseRecordLine)
	}
	l, err := strconv.ParseInt(*base.B2S(line[:i]), _base, 64)
	if err != nil {
		panic(err)
	}
	i++

	if line[i+int(l)] != valid {
		panic(base.ErrParseRecordLine)
	}

	pre = line[i : i+int(l)]
	suf = line[i+int(l)+1:]
	return
}

// getShard hashes the key to determine the sd.
func (db *Store) getShard(key string) *storeShard {
	return db.shards[xxh3.HashString(key)&db.mask]
}

// Get fetch the value by key from the database.
func (db *Store) Get(key string) Value {
	return db.getShard(key).Get(key)
}

// Get fetch the value by key from the sd.
func (sd *storeShard) Get(key string) Value {
	sd.RLock()
	defer sd.RUnlock()

	return sd.get(key)
}

func (sd *storeShard) get(key string) Value {
	val, ok := sd.Cache.Get(key)
	if !ok {
		return Value{}
	}

	if raw, isRaw := val.(base.Raw); isRaw {
		return Value{raw: raw, key: key, s: sd}

	} else {
		return Value{val: val, key: key, s: sd}
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

func (v Value) ToHMap() (Map, error) { return getValue(v, structx.NewMap[string, string]()) }

func (v Value) Scan(val any) error {
	_, err := getValue(v, val)
	return err
}

// getValue
func getValue[T any](v Value, vptr T) (T, error) {
	if v.raw != nil {
		if err := decode(v.raw, &vptr); err != nil {
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
