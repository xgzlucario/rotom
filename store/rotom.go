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

	"github.com/panjf2000/gnet/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// Operations.
type Operation byte

const (
	// cmd
	OpSetTx Operation = iota
	OpRemove
	OpHSet
	OpHRemove

	OpBitSet
	OpBitFlip
	OpBitOr
	OpBitAnd
	OpBitXor

	OpLPush
	OpLPop
	OpRPush
	OpRPop

	OpJsonSet
	OpJsonDelete

	OpMarshalBytes
	OpRequest

	// Request
	ReqPing
	ReqGet
	ReqLen
	ReqHLen
	ReqLLen

	// TODO
	OpZSet
	OpZIncr
	OpZRemove

	OpRename
)

// VType is value type.
type VType byte

const (
	V_STRING VType = iota + 1
	V_MAP
	V_SET
	V_LIST
	V_ZSET
	V_BITMAP
	V_JSON
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
	Map    = *structx.SyncMap[string, []byte]
	Set    = structx.Set[string]
	List   = *structx.List[string]
	ZSet   = *structx.ZSet[string, float64, []byte]
	BitMap = *structx.Bitmap
)

var (
	// Default configuration
	DefaultConfig = &Config{
		Path:           "rotom.db",
		ShardCount:     1024,
		SyncPolicy:     base.EverySecond,
		SyncInterval:   time.Second,
		ShrinkInterval: time.Minute,
		Logger:         slog.Default(),
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount int

	Path    string // Path of db file.
	tmpPath string

	SyncPolicy base.SyncPolicy // Data sync policy.

	SyncInterval   time.Duration // Interval of buffer writes to disk.
	ShrinkInterval time.Duration // Interval of shrink db file to compress space.

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

	// load
	if err := db.load(); err != nil {
		if db.Logger != nil {
			db.Logger.Error(fmt.Sprintf("db load error: %v", err))
		}
	}

	if db.SyncPolicy != base.Never {
		// Ticker to write buffer to disk.
		db.backend(db.SyncInterval, func() {
			db.Lock()
			n, err := db.writeTo(db.buf, db.Path)
			db.Unlock()
			if db.Logger != nil {
				if err != nil {
					db.Logger.Error(fmt.Sprintf("writeTo buffer error: %v", err))
				} else if n > 0 {
					db.Logger.Info(fmt.Sprintf("write %s buffer to db file", formatSize(n)))
				}
			}
		})

		// Ticker to shrink db.
		db.backend(db.ShrinkInterval, func() {
			db.Lock()
			db.shrink()
			db.Unlock()
		})
	}

	if db.Logger != nil {
		db.Logger.Info("rotom is ready to go")
	}

	return db, nil
}

// Listen
func (db *Store) Listen(addr string) error {
	if db.Logger != nil {
		db.Logger.Info(fmt.Sprintf("listening on %s...", addr))
	}

	return gnet.Run(&RotomEngine{db: db}, addr, gnet.WithMulticore(true))
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

// encode
func (db *Store) encode(cd *Codec) {
	db.Lock()
	db.buf.Write(cd.buf)
	db.Unlock()
	cd.recycle()
}

// Get
func (db *Store) Get(key string) ([]byte, int64, bool) {
	return db.m.Get(key)
}

// GetAny
func (db *Store) GetAny(key string) (any, int64, bool) {
	return db.m.GetAny(key)
}

// Set store key-value pair.
func (db *Store) Set(key string, val []byte) {
	db.SetTx(key, val, NoTTL)
}

// SetEx store key-value pair with ttl.
func (db *Store) SetEx(key string, val []byte, ttl time.Duration) {
	db.SetTx(key, val, cache.GetUnixNano()+int64(ttl))
}

// SetTx store key-value pair with deadline.
func (db *Store) SetTx(key string, val []byte, ts int64) {
	db.encode(NewCodec(OpSetTx, 4).
		Type(V_STRING).String(key).Int(ts / timeCarry).Bytes(val))

	db.m.SetTx(key, val, ts)
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

		db.encode(NewCodec(OpSetTx, 4).
			Type(V_STRING).String(key).Int(ts / timeCarry).String(fstr))
		db.m.SetTx(key, []byte(fstr), ts)

		return res, nil
	}

	return 0, base.ErrKeyNotFound
}

// Remove
func (db *Store) Remove(key string) bool {
	db.encode(NewCodec(OpRemove, 1).String(key))
	return db.m.Delete(key)
}

// Keys
func (db *Store) Keys() []string {
	keys := make([]string, 0)
	db.m.Scan(func(k string, _ any, _ int64) bool {
		keys = append(keys, k)
		return true
	})

	return keys
}

// Scan
func (db *Store) Scan(f func(string, any, int64) bool) {
	db.m.Scan(f)
}

// Len
func (db *Store) Stat() cache.CacheStat {
	return db.m.Stat()
}

// HGet
func (db *Store) HGet(key, field string) ([]byte, error) {
	m, err := db.getMap(key)
	if err != nil {
		return nil, err
	}

	res, ok := m.Get(field)
	if !ok {
		return nil, base.ErrFieldNotFound
	}
	return res, nil
}

// HSet
func (db *Store) HSet(key, field string, val []byte) error {
	m, err := db.getMap(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpHSet, 3).String(key).String(field).Bytes(val))
	m.Set(field, val)

	return nil
}

// HRemove
func (db *Store) HRemove(key, field string) error {
	m, err := db.getMap(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpHRemove, 2).String(key).String(field))
	m.Delete(field)

	return nil
}

// HKeys
func (db *Store) HKeys(key string) ([]string, error) {
	m, err := db.getMap(key)
	if err != nil {
		return nil, err
	}
	return m.Keys(), nil
}

// LPush
func (db *Store) LPush(key, item string) error {
	ls, err := db.getList(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpLPush, 2).String(key).String(item))
	ls.LPush(item)

	return nil
}

// RPush
func (db *Store) RPush(key, item string) error {
	ls, err := db.getList(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpRPush, 2).String(key).String(item))
	ls.RPush(item)

	return nil
}

// LPop
func (db *Store) LPop(key string) (string, error) {
	ls, err := db.getList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.LPop()
	if !ok {
		return "", base.ErrListEmpty
	}
	db.encode(NewCodec(OpLPop, 1).String(key))

	return res, nil
}

// RPop
func (db *Store) RPop(key string) (string, error) {
	ls, err := db.getList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.RPop()
	if !ok {
		return "", base.ErrListEmpty
	}
	db.encode(NewCodec(OpRPop, 1).String(key))

	return res, nil
}

// LLen
func (db *Store) LLen(key string) (int, error) {
	ls, err := db.getList(key)
	if err != nil {
		return 0, err
	}
	return ls.Len(), nil
}

// BitTest
func (db *Store) BitTest(key string, offset uint32) (bool, error) {
	bm, err := db.getBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (db *Store) BitSet(key string, offset uint32, val bool) error {
	bm, err := db.getBitMap(key)
	if err != nil {
		return err
	}
	// checked
	if val {
		if !bm.Add(offset) {
			return nil
		}
	} else {
		if !bm.Remove(offset) {
			return nil
		}
	}
	db.encode(NewCodec(OpBitSet, 3).String(key).Uint(offset).Bool(val))

	return nil
}

// BitFlip
func (db *Store) BitFlip(key string, offset uint32) error {
	bm, err := db.getBitMap(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpBitFlip, 2).String(key).Uint(offset))
	bm.Flip(uint64(offset))

	return nil
}

// BitOr
func (db *Store) BitOr(key1, key2, dest string) error {
	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpBitOr, 3).String(key1).String(key2).String(dest))

	if key1 == dest {
		bm1.Or(bm2)
	} else if key2 == dest {
		bm2.Or(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().Or(bm2))
	}

	return nil
}

// BitXor
func (db *Store) BitXor(key1, key2, dest string) error {
	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpBitXor, 3).String(key1).String(key2).String(dest))

	if key1 == dest {
		bm1.Xor(bm2)
	} else if key2 == dest {
		bm2.Xor(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().Xor(bm2))
	}

	return nil
}

// BitAnd
func (db *Store) BitAnd(key1, key2, dest string) error {
	bm1, err := db.getBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := db.getBitMap(key2)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpBitAnd, 3).String(key1).String(key2).String(dest))

	if key1 == dest {
		bm1.And(bm2)
	} else if key2 == dest {
		bm2.And(bm1)
	} else {
		db.m.SetAny(dest, bm1.Clone().And(bm2))
	}

	return nil
}

// BitArray
func (db *Store) BitArray(key string) ([]uint32, error) {
	bm, err := db.getBitMap(key)
	if err != nil {
		return nil, err
	}

	return bm.ToArray(), nil
}

// BitCount
func (db *Store) BitCount(key string) (uint64, error) {
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
func (s *Store) load() error {
	line, err := os.ReadFile(s.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if s.Logger != nil {
		s.Logger.Info(fmt.Sprintf("start to load db size %s", formatSize(len(line))))
	}

	var args [][]byte

	// record line is like:
	// <OP><argsNum><args...>
	for len(line) > 2 {
		op := Operation(line[0])
		argsNum := int(line[1])
		line = line[2:]

		// parse args by operation
		args, line, err = parseLine(line, argsNum)
		if err != nil {
			return err
		}

		switch op {
		case OpMarshalBytes: // val
			if err := s.m.UnmarshalBytes(args[0]); err != nil {
				return err
			}

		case OpSetTx: // type, key, ts, val
			ts := cache.ParseNumber[int64](args[2])
			ts *= timeCarry

			if ts < cache.GetUnixNano() && ts != NoTTL {
				continue
			}

			vType := VType(args[0][0])

			switch vType {
			case V_STRING:
				s.m.SetTx(*base.B2S(args[1]), args[3], ts)

			case V_MAP:
				var m Map
				if err := m.UnmarshalJSON(args[3]); err != nil {
					return err
				}
				s.m.SetAny(*base.B2S(args[1]), m)

			case V_BITMAP:
				var m BitMap
				if err := m.UnmarshalBinary(args[3]); err != nil {
					return err
				}
				s.m.SetAny(*base.B2S(args[1]), m)

			default:
				return fmt.Errorf("%v: %d", base.ErrUnSupportDataType, vType)
			}

		case OpRemove: // key
			s.Remove(*base.B2S(args[0]))

		case OpHSet: // key, field, val
			m, err := s.getMap(*base.B2S(args[0]))
			if err != nil {
				return err
			}
			m.Set(*base.B2S(args[1]), args[2])

		case OpHRemove: // key, field
			m, err := s.getMap(*base.B2S(args[0]))
			if err != nil {
				return err
			}
			m.Delete(*base.B2S(args[1]))

		case OpLPush, OpRPush: // key, item
			ls, err := s.getList(*base.B2S(args[0]))
			if err != nil {
				return err
			}

			if op == OpLPush {
				ls.LPush(*base.B2S(args[1]))
			} else {
				ls.RPush(*base.B2S(args[1]))
			}

		case OpLPop, OpRPop: // key
			ls, err := s.getList(*base.B2S(args[0]))
			if err != nil {
				return err
			}

			if op == OpLPop {
				ls.LPop()
			} else {
				ls.RPop()
			}

		case OpBitSet: // key, offset, val
			bm, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				return err
			}

			offset := cache.ParseNumber[uint32](args[1])
			if args[2][0] == _true {
				bm.Add(offset)
			} else {
				bm.Remove(offset)
			}

		case OpBitFlip: // key, offset
			bm, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				return err
			}
			bm.Flip(cache.ParseNumber[uint64](args[1]))

		case OpBitAnd, OpBitOr, OpBitXor: // key, src, dst
			bm1, err := s.getBitMap(*base.B2S(args[0]))
			if err != nil {
				return err
			}

			bm2, err := s.getBitMap(*base.B2S(args[1]))
			if err != nil {
				return err
			}

			if slices.Equal(args[0], args[2]) {
				switch op {
				case OpBitAnd:
					bm1.And(bm2)
				case OpBitOr:
					bm1.Or(bm2)
				case OpBitXor:
					bm1.Xor(bm2)
				}

			} else if slices.Equal(args[1], args[2]) {
				switch op {
				case OpBitAnd:
					bm2.And(bm1)
				case OpBitOr:
					bm2.Or(bm1)
				case OpBitXor:
					bm2.Xor(bm1)
				}

			} else {
				switch op {
				case OpBitAnd:
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().And(bm2))
				case OpBitOr:
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().Or(bm2))
				case OpBitXor:
					s.m.SetAny(*base.B2S(args[2]), bm1.Clone().Xor(bm2))
				}
			}

		default:
			return fmt.Errorf("%v: %c", base.ErrUnknownOperationType, op)
		}
	}

	if s.Logger != nil {
		s.Logger.Info("db load complete")
	}

	return nil
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

	cd := NewCodec(OpMarshalBytes, 1).Bytes(data)
	s.rwbuf.Write(cd.buf)
	cd.recycle()

	// MarshalOthers
	var rec VType
	s.m.Scan(func(key string, v any, i int64) bool {
		switch v.(type) {
		case String:
			return true
		case Map:
			rec = V_STRING
		case BitMap:
			rec = V_BITMAP
		case List:
			rec = V_LIST
		case Set:
			rec = V_SET
		default:
			panic(base.ErrUnSupportDataType)
		}

		// SetTx
		if cd, err := NewCodec(OpSetTx, 4).Type(rec).String(key).Int(i / timeCarry).Any(v); err == nil {
			s.rwbuf.Write(cd.buf)
			cd.recycle()
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
// input: <key_len>SEP<key_value><somthing...>
// return: key_value, somthing..., error
func parseLine(line []byte, argsNum int) ([][]byte, []byte, error) {
	res := make([][]byte, 0, argsNum)

	for flag := 0; flag < argsNum; flag++ {
		i := bytes.IndexByte(line, SEP_CHAR)
		if i <= 0 {
			return nil, nil, base.ErrParseRecordLine
		}

		key_len := cache.ParseNumber[int](line[:i])
		i++

		// valid
		if len(line) < i+key_len {
			return nil, nil, base.ErrParseRecordLine
		}

		res = append(res, line[i:i+key_len])

		line = line[i+key_len:]
	}

	return res, line, nil
}

// getMap
func (db *Store) getMap(key string) (m Map, err error) {
	return getOrCreate(db, key, m, func() Map {
		return structx.NewSyncMap[string, []byte]()
	})
}

// getList
func (db *Store) getList(key string) (m List, err error) {
	return getOrCreate(db, key, m, func() List {
		return structx.NewList[string]()
	})
}

// getBitMap
func (db *Store) getBitMap(key string) (bm BitMap, err error) {
	return getOrCreate(db, key, bm, func() BitMap {
		return structx.NewBitmap()
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

func formatSize[T base.Number](size T) string {
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
