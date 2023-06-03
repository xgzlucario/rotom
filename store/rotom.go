// Package store provides an in-memory key-value database.
package store

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
	"golang.org/x/exp/slices"
)

type Operation byte

// Operation types.
const (
	OpSetTx Operation = iota + 'A'
	OpRemove
	OpPersist
	OpHSet
	OpHRemove

	OpBitSet
	OpBitFlip
	OpBitOr
	OpBitAnd
	OpBitXor

	// TODO: Implement these operations.
	OpIncr
	OpLPush
	OpLPop
	OpRPush
	OpRPop

	OpZSet
	OpZIncr
	OpZRemove

	OpTrieSet
	OpTrieRemove
)

// Record types.
type RecordType byte

const (
	RecordString RecordType = iota + 'A'
	RecordMap
	RecordSet
	RecordList
	RecordZSet
	RecordBitMap
)

const (
	recordSepChar = byte('\n')
	timeCarry     = 1000 * 1000 * 1000
	NoTTL         = 0
)

// Type aliases for structx types.
type (
	String = []byte
	Map    = structx.Map[string, []byte]
	Set    = structx.Set[string]
	List   = *structx.List[string]
	ZSet   = *structx.ZSet[string, float64, []byte]
	BitMap = *structx.Bitset
)

var (
	globalTime    = time.Now().UnixNano()
	DefaultConfig = &Config{
		Path:            "db",
		ShardCount:      32,
		SyncPolicy:      base.EverySecond,
		SyncInterval:    time.Second,
		RewriteInterval: time.Minute / 2,
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
			globalTime = t.UnixNano()
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

	if err := os.MkdirAll(db.Path, 0755); err != nil {
		return nil, err
	}

	// Load configuration
	for i := range db.shards {
		db.shards[i] = &storeShard{
			syncPolicy: conf.SyncPolicy,
			buf:        bytes.NewBuffer(make([]byte, 0, 4096)),
			rwbuf:      bytes.NewBuffer(make([]byte, 0, 4096)),
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
		base.Go(db.SyncInterval, func() {
			s.writeTo(s.buf, s.path)
		})
		base.Go(db.RewriteInterval, func() {
			s.dump()
		})
	}

	return db, nil
}

// Get
func (db *Store) Get(key string) ([]byte, bool) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	val, _ := sd.Get(key)
	str, ok := val.(String)
	return str, ok
}

// GetAny
func (db *Store) GetAny(key string) (any, bool) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	return sd.Get(key)
}

// Set sets a key-value pair in the database.
func (db *Store) Set(key string, val []byte) error {
	return db.SetTx(key, val, NoTTL)
}

// Incr
func (db *Store) Incr(key string, increment float64) (float64, error) {
	sd := db.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	val, ok := sd.Get(key)
	if !ok {
		return -1, base.ErrKeyNotFound
	}

	str, _ := val.(String)
	num, err := strconv.ParseFloat(*base.B2S(str), 64)
	if err != nil {
		return -1, err
	}

	num += increment
	numStr := strconv.FormatFloat(num, 'f', -1, 64)

	sd.Set(key, base.S2B(&numStr))

	return num, nil
}

func (db *Store) command(key string, coder *Coder, cmd func(*storeShard) error) error {
	sd := db.getShard(key)
	sd.Lock()
	defer sd.Unlock()
	defer putCoder(coder)

	if err := cmd(sd); err != nil {
		return err
	}
	sd.buf.Write(coder.buf)

	return nil
}

// SetEx sets a key-value pair with TTL (Time To Live) in the database.
func (db *Store) SetEx(key string, val []byte, ttl time.Duration) error {
	return db.SetTx(key, val, globalTime+int64(ttl))
}

// SetTx sets a key-value pair with expiry time in the database.
// If ts set to 0, the key will never expire.
func (db *Store) SetTx(key string, val []byte, ts int64) error {
	cd := NewCoder(OpSetTx).Type(RecordString).String(key).Ts(ts / timeCarry).Bytes(val)

	return db.command(key, cd, func(sd *storeShard) error {
		sd.SetTx(key, val, ts)
		return nil
	})
}

// Remove removes a key-value pair from the database and return it.
func (db *Store) Remove(key string) (val any, ok bool) {
	cd := NewCoder(OpRemove).String(key)

	db.command(key, cd, func(sd *storeShard) error {
		val, ok = sd.Remove(key)
		return nil
	})
	return
}

// Persist persists a key-value pair in the database.
func (db *Store) Persist(key string) (ok bool) {
	cd := NewCoder(OpPersist).String(key)

	db.command(key, cd, func(sd *storeShard) error {
		ok = sd.Persist(key)
		return nil
	})
	return
}

// HGet
func (db *Store) HGet(key, field string) ([]byte, error) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	hmap, err := sd.getMap(key)
	if err != nil {
		return nil, err
	}

	res, ok := hmap.Get(field)
	if !ok {
		return nil, base.ErrFieldNotFound
	}
	return res, nil
}

// HSet
func (db *Store) HSet(key, field string, val []byte) error {
	cd := NewCoder(OpHSet).String(key).String(field).Bytes(val)

	return db.command(key, cd, func(sd *storeShard) error {
		m, err := sd.getMap(key)
		if err != nil {
			return err
		}
		m.Set(field, val)
		return nil
	})
}

// HRemove
func (db *Store) HRemove(key, field string) error {
	cd := NewCoder(OpHRemove).String(key).String(field)

	return db.command(key, cd, func(sd *storeShard) error {
		m, err := sd.getMap(key)
		if err != nil {
			return err
		}
		m.Delete(field)
		return nil
	})
}

// BitTest
func (db *Store) BitTest(key string, offset uint) (bool, error) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	bm, err := sd.getBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (db *Store) BitSet(key string, offset uint, value bool) error {
	cd := NewCoder(OpBitSet).String(key).Uint(offset).Bool(value)

	return db.command(key, cd, func(sd *storeShard) error {
		bm, err := sd.getBitMap(key)
		if err != nil {
			return err
		}
		bm.SetTo(offset, value)
		return nil
	})
}

// BitFlip
func (db *Store) BitFlip(key string, offset uint) error {
	cd := NewCoder(OpBitFlip).String(key).Uint(offset)

	return db.command(key, cd, func(sd *storeShard) error {
		bm, err := sd.getBitMap(key)
		if err != nil {
			return err
		}
		bm.Flip(offset)
		return nil
	})
}

// BitOr
func (db *Store) BitOr(key1, key2, dest string) error {
	cd := NewCoder(OpBitOr).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	// bm1
	sd1 := db.getShard(key1)
	sd1.RLock()
	defer sd1.RUnlock()
	bm1, err := sd1.getBitMap(key1)
	if err != nil {
		return err
	}

	// bm2
	sd2 := db.getShard(key2)
	sd2.RLock()
	defer sd2.RUnlock()
	bm2, err := sd2.getBitMap(key2)
	if err != nil {
		return err
	}

	if key1 == dest {
		sd1.buf.Write(cd.buf)
		bm1.Union(bm2)

	} else if key2 == dest {
		sd2.buf.Write(cd.buf)
		bm2.Union(bm1)

	} else {
		sd := db.getShard(dest)
		sd.Lock()
		defer sd.Unlock()

		sd.buf.Write(cd.buf)
		sd.Set(dest, bm1.Clone().Union(bm2))
	}
	return nil
}

// BitXor
func (db *Store) BitXor(key1, key2, dest string) error {
	cd := NewCoder(OpBitXor).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	// bm1
	sd1 := db.getShard(key1)
	sd1.RLock()
	defer sd1.RUnlock()
	bm1, err := sd1.getBitMap(key1)
	if err != nil {
		return err
	}

	// bm2
	sd2 := db.getShard(key2)
	sd2.RLock()
	defer sd2.RUnlock()
	bm2, err := sd2.getBitMap(key2)
	if err != nil {
		return err
	}

	if key1 == dest {
		sd1.buf.Write(cd.buf)
		bm1.Difference(bm2)

	} else if key2 == dest {
		sd2.buf.Write(cd.buf)
		bm2.Difference(bm1)

	} else {
		sd := db.getShard(dest)
		sd.Lock()
		defer sd.Unlock()

		sd.buf.Write(cd.buf)
		sd.Set(dest, bm1.Clone().Difference(bm2))
	}
	return nil
}

// BitAnd
func (db *Store) BitAnd(key1, key2, dest string) error {
	cd := NewCoder(OpBitAnd).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	// bm1
	sd1 := db.getShard(key1)
	sd1.RLock()
	defer sd1.RUnlock()
	bm1, err := sd1.getBitMap(key1)
	if err != nil {
		return err
	}

	// bm2
	sd2 := db.getShard(key2)
	sd2.RLock()
	defer sd2.RUnlock()
	bm2, err := sd2.getBitMap(key2)
	if err != nil {
		return err
	}

	if key1 == dest {
		sd1.buf.Write(cd.buf)
		bm1.Intersection(bm2)

	} else if key2 == dest {
		sd2.buf.Write(cd.buf)
		bm2.Intersection(bm1)

	} else {
		sd := db.getShard(dest)
		sd.Lock()
		defer sd.Unlock()

		sd.buf.Write(cd.buf)
		sd.Set(dest, bm1.Clone().Intersection(bm2))
	}
	return nil
}

// BitCount
func (db *Store) BitCount(key string) (uint, error) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	bm, err := sd.getBitMap(key)
	return bm.Len(), err
}

// Flush
func (db *Store) Flush() error {
	for _, sd := range db.shards {
		sd.Lock()
		defer sd.Unlock()
		if _, err := sd.writeTo(sd.buf, sd.path); err != nil {
			return err
		}
	}
	return nil
}

// Clear
func (db *Store) Clear() {
	for _, sd := range db.shards {
		sd.Lock()
		defer sd.Unlock()
		sd.Clear()
	}
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

// writeTo writes the buffer into the file at the specified path.
func (s *storeShard) writeTo(buf *bytes.Buffer, path string) (int64, error) {
	s.Lock()
	defer s.Unlock()

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

	var op Operation
	var recordType RecordType

	for len(line) > 1 {
		op = Operation(line[0])
		line = line[1:]

		// SetTx need parse record type
		if op == OpSetTx {
			recordType = RecordType(line[0])
			line = line[1:]
		}

		// parse key
		var key []byte
		key, line = parseWord(line, recordSepChar)

		switch op {
		case OpSetTx:
			// ts value
			var val []byte
			var ts int64

			ts, line = parseTs(line)
			ts *= timeCarry

			val, line = parseWord(line, recordSepChar)

			// check if expired
			if ts < globalTime && ts != NoTTL {
				continue
			}

			switch recordType {
			case RecordString:
				s.SetTx(*base.B2S(key), val, ts)

			case RecordMap:
				var m Map
				if err := m.UnmarshalJSON(val); err != nil {
					panic(err)
				}
				s.Set(*base.B2S(key), m)

			case RecordBitMap:
				var m BitMap
				if err := m.UnmarshalBinary(val); err != nil {
					panic(err)
				}
				s.Set(*base.B2S(key), m)

			default:
				panic(fmt.Errorf("%v: %d", base.ErrUnSupportDataType, recordType))
			}

		case OpHSet:
			// field value
			var field, val []byte

			field, line = parseWord(line, recordSepChar)
			val, line = parseWord(line, recordSepChar)

			m, err := s.getMap(*base.B2S(key))
			base.Assert1(err)

			m.Set(*base.B2S(field), val)

		case OpBitSet:
			// offset value
			var _offset, val []byte

			_offset, line = parseWord(line, recordSepChar)
			val, line = parseWord(line, recordSepChar)

			offset, err := strconv.ParseUint(*base.B2S(_offset), _base, 64)
			base.Assert1(err)

			bm, err := s.getBitMap(*base.B2S(key))
			base.Assert1(err)

			bm.SetTo(uint(offset), val[0] == _true)

		case OpBitFlip:
			// offset
			var _offset []byte

			_offset, line = parseWord(line, recordSepChar)

			offset, err := strconv.ParseUint(*base.B2S(_offset), _base, 64)
			base.Assert1(err)

			bm, err := s.getBitMap(*base.B2S(key))
			base.Assert1(err)

			bm.Flip(uint(offset))

		case OpBitAnd, OpBitOr, OpBitXor:
			// src, dest, key is bitmap1
			var src, dest []byte

			src, line = parseWord(line, recordSepChar)
			dest, line = parseWord(line, recordSepChar)

			bm1, err := s.getBitMap(*base.B2S(key))
			base.Assert1(err)

			bm2, err := s.getBitMap(*base.B2S(src))
			base.Assert1(err)

			if slices.Equal(key, dest) {
				switch op {
				case OpBitAnd:
					bm1.Intersection(bm2)
				case OpBitOr:
					bm1.Union(bm2)
				case OpBitXor:
					bm1.Difference(bm2)
				}

			} else if slices.Equal(src, dest) {
				switch op {
				case OpBitAnd:
					bm2.Intersection(bm1)
				case OpBitOr:
					bm2.Union(bm1)
				case OpBitXor:
					bm2.Difference(bm1)
				}

			} else {
				switch op {
				case OpBitAnd:
					s.Set(*base.B2S(dest), bm1.Clone().Intersection(bm2))
				case OpBitOr:
					s.Set(*base.B2S(dest), bm1.Clone().Union(bm2))
				case OpBitXor:
					s.Set(*base.B2S(dest), bm1.Clone().Difference(bm2))
				}
			}

		case OpHRemove:
			// field
			var field []byte

			field, line = parseWord(line, recordSepChar)

			m, err := s.getMap(*base.B2S(key))
			base.Assert1(err)

			m.Delete(*base.B2S(field))

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
	var record RecordType
	s.Scan(func(key string, v any, i int64) bool {
		switch v := v.(type) {
		case String:
			cd := NewCoder(OpSetTx).Type(RecordString).String(key).Ts(i / timeCarry).Bytes(v)
			s.rwbuf.Write(cd.buf)
			putCoder(cd)

			return true

		case Map:
			record = RecordMap
		case BitMap:
			record = RecordBitMap
		case List:
			record = RecordList
		case Set:
			record = RecordSet
		default:
			panic(base.ErrUnSupportDataType)
		}

		// SetTx
		if cd, err := NewCoder(OpSetTx).Type(record).String(key).Ts(i / timeCarry).Any(v); err == nil {
			s.rwbuf.Write(cd.buf)
			putCoder(cd)
		}

		return true
	})

	// Flush buffer to file
	s.writeTo(s.rwbuf, s.rwPath)
	s.writeTo(s.buf, s.rwPath)

	// Rename rewrite file to the shard file
	os.Rename(s.rwPath, s.path)
}

// parseWord parse file content to record lines
func parseWord(line []byte, valid byte) (pre []byte, suf []byte) {
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

// parseTs
func parseTs(line []byte) (int64, []byte) {
	i := bytes.IndexByte(line, recordSepChar)
	if i <= 0 {
		panic(base.ErrParseRecordLine)
	}

	ts, err := strconv.ParseInt(*base.B2S(line[:i]), _base, 64)
	if err != nil {
		panic(err)
	}

	return ts, line[i+1:]
}

// getShard hashes the key to determine the sd.
func (db *Store) getShard(key string) *storeShard {
	return db.shards[xxh3.HashString(key)&db.mask]
}

// getMap
func (sd *storeShard) getMap(key string) (m Map, err error) {
	return getOrCreate(sd, key, m, func() Map {
		return structx.NewMap[string, []byte]()
	})
}

// getBitMap
func (sd *storeShard) getBitMap(key string) (bm BitMap, err error) {
	return getOrCreate(sd, key, bm, func() BitMap {
		return structx.NewBitset()
	})
}

func getOrCreate[T any](s *storeShard, key string, vptr T, new func() T) (T, error) {
	m, ok := s.Get(key)
	if ok {
		m, ok := m.(T)
		if ok {
			return m, nil
		}
		return vptr, base.ErrWrongType
	}

	vptr = new()
	s.Set(key, vptr)

	return vptr, nil
}
