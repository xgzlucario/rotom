// Package store provides an in-memory key-value database.
package store

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// Operations.
type Operation byte

const (
	OpSetTx Operation = iota
	OpRemove
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

	OpMarshalBytes
)

// Record types.
type RecordType byte

const (
	RecordString RecordType = iota
	RecordMap
	RecordSet
	RecordList
	RecordZSet
	RecordBitMap
)

const (
	SEP_CHAR  = byte(255)
	timeCarry = 1000 * 1000 * 1000
	NoTTL     = 0

	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
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
	// Default configuration
	DefaultConfig = &Config{
		Path:           "rotom.db",
		ShardCount:     1024,
		SyncPolicy:     base.EverySecond,
		SyncInterval:   time.Second,
		ShrinkInterval: time.Minute,
		StatInterval:   time.Minute,
		Logger:         slog.Default(),
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount int

	Path    string // Path of db file.
	tmpPath string

	SyncPolicy base.SyncPolicy // data sync policy.

	SyncInterval   time.Duration // Interval of buffer writes to disk.
	ShrinkInterval time.Duration // Interval of shrink db file to compress space.
	StatInterval   time.Duration // Interval of monitor db status.

	Logger *slog.Logger // Logger for db, set <nil> if you don't want to use it.
}

// Store represents a key-value store.
type Store struct {
	sync.RWMutex
	*Config
	closed bool
	buf    *bytes.Buffer
	rwbuf  *bytes.Buffer
	m      *cache.GigaCache[string]
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf *Config) (*Store, error) {
	db := &Store{
		Config: conf,
		buf:    bytes.NewBuffer(nil),
		rwbuf:  bytes.NewBuffer(nil),
		m:      cache.New[string](conf.ShardCount),
	}
	db.tmpPath = db.Path + ".tmp"
	db.load()

	// Ticker to write buffer.
	db.backend(db.SyncInterval, func() {
		db.Lock()
		if db.Logger != nil {
			db.Logger.Info(fmt.Sprintf("write %s buffer to db file", formatSize(db.buf.Len())))
		}
		db.writeTo(db.buf, db.Path)
		db.Unlock()
	})

	// Ticker to shrink db.
	db.backend(db.ShrinkInterval, func() {
		db.Lock()
		db.shrink()
		db.Unlock()
	})

	// Ticker to moniter stat.
	db.backend(db.StatInterval, func() {
		if db.Logger != nil {
			db.RLock()
			db.Logger.Info(fmt.Sprintf("db stat: %+v", db.Stat()))
			db.RUnlock()
		}
	})

	if db.Logger != nil {
		db.Logger.Info("rotom is ready to go")
	}

	return db, nil
}

// Close
func (db *Store) Close() error {
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return base.ErrDatabaseClosed
	}
	_, err := db.writeTo(db.buf, db.Path)
	db.closed = true

	return err
}

// Get
func (db *Store) Get(key string) ([]byte, int64, bool) {
	return db.m.Get(key)
}

// GetAny
func (db *Store) GetAny(key string) (any, int64, bool) {
	return db.m.GetAny(key)
}

// Set
func (db *Store) Set(key string, val []byte) {
	db.SetTx(key, val, NoTTL)
}

// SetEx
func (db *Store) SetEx(key string, val []byte, ttl time.Duration) {
	db.SetTx(key, val, cache.GetUnixNano()+int64(ttl))
}

// SetTx
func (db *Store) SetTx(key string, val []byte, ts int64) {
	cd := NewCoder(OpSetTx, 3).Type(RecordString).String(key).Int(ts / timeCarry).Bytes(val)
	db.m.SetTx(key, val, ts)

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	putCoder(cd)
}

// Incr
func (db *Store) Incr(key string, incr float64) (res float64, err error) {
	val, ts, ok := db.m.Get(key)
	if ok {
		f, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, err
		}
		res = f + incr
		fstr := strconv.FormatFloat(res, 'f', 4, 64)

		cd := NewCoder(OpSetTx, 3).Type(RecordString).String(key).Int(ts / timeCarry).String(fstr)
		db.m.SetTx(key, []byte(fstr), ts)

		db.Lock()
		db.buf.Write(cd.buf)
		db.Unlock()
		putCoder(cd)

		return res, nil
	}

	return 0, base.ErrKeyNotFound
}

// Remove
func (db *Store) Remove(key string) (any, bool) {
	cd := NewCoder(OpRemove, 1).String(key)
	db.m.Delete(key)

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	putCoder(cd)

	return nil, true
}

// Scan
func (db *Store) Scan(f func(string, any, int64) bool) {
	db.RLock()
	defer db.RUnlock()
	db.m.Scan(f)
}

// Len
func (db *Store) Stat() cache.CacheStat {
	return db.m.Stat()
}

// HGet
func (db *Store) HGet(key, field string) ([]byte, error) {
	hmap, err := db.getMap(key)
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
	cd := NewCoder(OpHSet, 3).String(key).String(field).Bytes(val)

	db.Lock()
	defer db.Unlock()
	defer putCoder(cd)

	m, err := db.getMap(key)
	if err != nil {
		return err
	}

	m.Set(field, val)
	db.buf.Write(cd.buf)

	return nil
}

// HRemove
func (db *Store) HRemove(key, field string) error {
	cd := NewCoder(OpHRemove, 2).String(key).String(field)

	m, err := db.getMap(key)
	if err != nil {
		return err
	}
	m.Delete(field)

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	putCoder(cd)

	return nil
}

// BitTest
func (db *Store) BitTest(key string, offset uint) (bool, error) {
	bm, err := db.getBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (db *Store) BitSet(key string, offset uint, value bool) error {
	cd := NewCoder(OpBitSet, 3).String(key).Uint(offset).Bool(value)

	bm, err := db.getBitMap(key)
	if err != nil {
		return err
	}
	bm.SetTo(offset, value)

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	putCoder(cd)

	return nil
}

// BitFlip
func (db *Store) BitFlip(key string, offset uint) error {
	cd := NewCoder(OpBitFlip, 2).String(key).Uint(offset)

	bm, err := db.getBitMap(key)
	if err != nil {
		return err
	}
	bm.Flip(offset)

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	putCoder(cd)

	return nil
}

// BitOr
func (db *Store) BitOr(key1, key2, dest string) error {
	cd := NewCoder(OpBitOr, 3).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()

	if key1 == dest {
		bm1.Union(bm2)
	} else if key2 == dest {
		bm2.Union(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().Union(bm2))
	}

	return nil
}

// BitXor
func (db *Store) BitXor(key1, key2, dest string) error {
	cd := NewCoder(OpBitXor, 3).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()

	if key1 == dest {
		bm1.Difference(bm2)
	} else if key2 == dest {
		bm2.Difference(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().Difference(bm2))
	}

	return nil
}

// BitAnd
func (db *Store) BitAnd(key1, key2, dest string) error {
	cd := NewCoder(OpBitAnd, 3).String(key1).String(key2).String(dest)
	defer putCoder(cd)

	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}

	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()

	if key1 == dest {
		bm1.Intersection(bm2)
	} else if key2 == dest {
		bm2.Intersection(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().Intersection(bm2))
	}

	return nil
}

// BitCount
func (db *Store) BitCount(key string) (uint, error) {
	bm, err := db.getBitMap(key)
	return bm.Len(), err
}

// writeTo writes the buffer into the file at the specified path.
func (s *Store) writeTo(buf *bytes.Buffer, path string) (int64, error) {
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
func (s *Store) load() {
	line, err := os.ReadFile(s.Path)
	if err != nil {
		return
	}

	if s.Logger != nil {
		s.Logger.Info(fmt.Sprintf("start to load db size %s", formatSize(len(line))))
	}

	var op Operation
	var recordType RecordType
	var argsNum int
	var args [][]byte

	/*
		record line is like:
		<OP><argsNum><args...>
	*/
	for len(line) > 2 {
		op = Operation(line[0])
		argsNum = int(line[1])
		line = line[2:]

		// OpSetTx
		if op == OpSetTx {
			recordType = RecordType(line[0])
			line = line[1:]
		}

		// parse args by operation
		args, line, err = parseLine(line, argsNum)
		if err != nil {
			break
		}

		switch op {
		case OpSetTx:
			// check ttl
			ts := base.ParseNumber[int64](args[1])
			ts *= timeCarry

			if ts < cache.GetUnixNano() && ts != NoTTL {
				continue
			}

			switch recordType {
			case RecordString:
				s.m.SetTx(*base.B2S(args[0]), args[2], ts)

			case RecordMap:
				var m Map
				if err := m.UnmarshalJSON(args[2]); err != nil {
					panic(err)
				}
				s.m.SetAny(*base.B2S(args[0]), m)

			case RecordBitMap:
				var m BitMap
				if err := m.UnmarshalBinary(args[2]); err != nil {
					panic(err)
				}
				s.m.SetAny(*base.B2S(args[0]), m)

			default:
				panic(fmt.Errorf("%v: %d", base.ErrUnSupportDataType, recordType))
			}

		case OpHSet:
			// key, field, val
			m, err := s.getMap(*base.B2S(args[0]))
			if err != nil {
				panic(err)
			}

			m.Set(*base.B2S(args[1]), args[2])

		case OpBitSet:
			// key, offset, val
			bm, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				panic(err)
			}

			offset := base.ParseNumber[uint](args[1])
			bm.SetTo(offset, args[2][0] == _true)

		case OpBitFlip:
			// key, offset
			bm, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				panic(err)
			}

			bm.Flip(base.ParseNumber[uint](args[1]))

		case OpBitAnd, OpBitOr, OpBitXor:
			// key src, dest, key is bitmap1
			bm1, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				panic(err)
			}

			bm2, err := s.getBitMap(*base.B2S(args[1]))
			if err != nil {
				panic(err)
			}

			if slices.Equal(args[0], args[2]) {
				switch op {
				case OpBitAnd:
					bm1.Intersection(bm2)
				case OpBitOr:
					bm1.Union(bm2)
				case OpBitXor:
					bm1.Difference(bm2)
				}

			} else if slices.Equal(args[1], args[2]) {
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
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().Intersection(bm2))
				case OpBitOr:
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().Union(bm2))
				case OpBitXor:
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().Difference(bm2))
				}
			}

		case OpHRemove:
			// key field
			m, err := s.getMap(*base.B2S(args[0]))
			if err != nil {
				panic(err)
			}

			m.Delete(*base.B2S(args[1]))

		case OpRemove:
			// key
			s.Remove(*base.B2S(args[0]))

		case OpMarshalBytes:
			// val
			if err := s.m.UnmarshalBytes(args[0]); err != nil {
				panic(err)
			}

		default:
			panic(fmt.Errorf("%v: %c", base.ErrUnknownOperationType, op))
		}
	}

	if s.Logger != nil {
		s.Logger.Info("db load complete")
	}
}

// rewrite write data to the file.
func (s *Store) shrink() {
	if s.SyncPolicy == base.Never {
		return
	}

	data, err := s.m.MarshalBytes()
	if err != nil {
		panic(err)
	}
	cd := NewCoder(OpMarshalBytes, 1).Bytes(data)
	s.rwbuf.Write(cd.buf)
	putCoder(cd)

	// MarshalOthers
	var record RecordType
	s.m.Scan(func(key string, v any, i int64) bool {
		switch v.(type) {
		case String:
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
		if cd, err := NewCoder(OpSetTx, 3).Type(record).String(key).Int(i / timeCarry).Any(v); err == nil {
			s.rwbuf.Write(cd.buf)
			putCoder(cd)
		}

		return true
	})

	// Flush buffer to file
	s.writeTo(s.rwbuf, s.tmpPath)
	s.writeTo(s.buf, s.tmpPath)

	os.Rename(s.tmpPath, s.Path)

	if s.Logger != nil {
		s.Logger.Info("rotom rewrite done")
	}
}

// parseLine parse file content to record lines.
// exp:
// input: <key_len>SEP<key_value>SEP<somthing...>
// return: key_value, somthing..., error
func parseLine(line []byte, argsNum int) ([][]byte, []byte, error) {
	res := make([][]byte, 0, argsNum)

	for index := 0; index < argsNum; index++ {
		i := bytes.IndexByte(line, SEP_CHAR)
		if i <= 0 {
			return nil, nil, base.ErrParseRecordLine
		}

		key_len := base.ParseNumber[int](line[:i])
		i++

		// valid
		if len(line) <= i+key_len || line[i+key_len] != SEP_CHAR {
			return nil, nil, base.ErrParseRecordLine
		}

		res = append(res, line[i:i+key_len])

		line = line[i+key_len+1:]
	}

	return res, line, nil
}

// getMap
func (db *Store) getMap(key string) (m Map, err error) {
	return getOrCreate(db, key, m, func() Map {
		return structx.NewMap[string, []byte]()
	})
}

// getBitMap
func (db *Store) getBitMap(key string) (bm BitMap, err error) {
	return getOrCreate(db, key, bm, func() BitMap {
		return structx.NewBitset()
	})
}

// getOrCreate
func getOrCreate[T any](s *Store, key string, vptr T, new func() T) (T, error) {
	m, _, ok := s.m.GetAny(key)
	if ok {
		m, ok := m.(T)
		if ok {
			return m, nil
		}
		return vptr, base.ErrWrongType
	}

	vptr = new()
	s.m.SetAny(key, vptr)

	return vptr, nil
}

func formatSize(size int) string {
	switch {
	case size < KB:
		return fmt.Sprintf("%dB", size)
	case size < MB:
		return fmt.Sprintf("%.1fKB", float64(size)/KB)
	case size < GB:
		return fmt.Sprintf("%.1fMB", float64(size)/MB)
	default:
		return fmt.Sprintf("%.1fGB", float64(size)/GB)
	}
}

func (db *Store) backend(t time.Duration, f func()) {
	if t <= 0 {
		return
	}
	go func() {
		for {
			time.Sleep(t)
			if db.closed {
				return
			}
			f()
		}
	}()
}
