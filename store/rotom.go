// Package store provides an in-memory key-value database.
package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

type Operation byte

// Operation types.
const (
	OpSetTx Operation = iota + 'A'
	OpRemove
	OpPersist
	OpHSet
	OpHRemove
	// TODO: Implement these operations.
	OpIncr
	OpSetBit
	OpLPush
	OpLPop
	OpRPush
	OpRPop
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
	noTTL         = 0
)

// Type aliases for structx types.
type (
	String = []byte
	Map    = map[string][]byte
	Set    = map[string]struct{}
	List   = structx.List[string]
	ZSet   = *structx.ZSet[string, float64, []byte]
	BitMap = *structx.BitMap
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
func (db *Store) Set(key string, val []byte) error {
	return db.SetTx(key, val, noTTL)
}

func (db *Store) command(key string, coder *Coder, cmd func(*storeShard)) {
	sd := db.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	sd.buf.Write(coder.buf)
	putCoder(coder)
	cmd(sd)
}

// SetEx sets a key-value pair with TTL (Time To Live) in the database.
func (db *Store) SetEx(key string, val []byte, ttl time.Duration) error {
	return db.SetTx(key, val, globalTime+int64(ttl))
}

// SetTx sets a key-value pair with expiry time in the database.
// If ts set to 0, the key will never expire.
func (db *Store) SetTx(key string, val []byte, ts int64) error {
	cd := NewCoder(OpSetTx).Type(RecordString).String(key).Int64(ts / timeCarry).Bytes(val)
	if cd.err != nil {
		return cd.err
	}

	db.command(key, cd, func(sd *storeShard) {
		sd.SetTx(key, val, ts)
	})
	return nil
}

// Remove removes a key-value pair from the database and return it.
func (db *Store) Remove(key string) (val any, ok bool) {
	cd := NewCoder(OpRemove).String(key)

	db.command(key, cd, func(sd *storeShard) {
		val, ok = sd.Remove(key)
	})
	return
}

// Persist persists a key-value pair in the database.
func (db *Store) Persist(key string) (ok bool) {
	cd := NewCoder(OpPersist).String(key)

	db.command(key, cd, func(sd *storeShard) {
		ok = sd.Persist(key)
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

	res, ok := hmap[field]
	if !ok {
		return nil, base.ErrFieldNotFound
	}
	return res, nil
}

// HSet
func (db *Store) HSet(key, field string, val []byte) (_err error) {
	cd := NewCoder(OpHSet).String(key).String(field).String(*base.B2S(val))

	db.command(key, cd, func(sd *storeShard) {
		m, err := sd.getMap(key)

		// if not exist, create a new one
		if err == base.ErrKeyNotFound {
			sd.Set(key, Map{field: val})

		} else if err != nil {
			_err = err

		} else {
			m[field] = val
		}
	})

	return
}

// HRemove
func (db *Store) HRemove(key, field string) (_err error) {
	cd := NewCoder(OpHRemove).String(key).String(field)

	db.command(key, cd, func(sd *storeShard) {
		m, err := sd.getMap(key)
		if err != nil {
			_err = err
			return
		}
		delete(m, field)
	})

	return
}

// GetBit
func (db *Store) GetBit(key string, bit uint32) (bool, error) {
	sd := db.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	bm, err := sd.getBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Contains(bit), nil
}

// SetBit
func (db *Store) SetBit(key string, bit uint32) (_err error) {
	cd := NewCoder(OpSetBit).String(key).Uint32(bit)

	db.command(key, cd, func(sd *storeShard) {
		bm, err := sd.getBitMap(key)

		// if not exist, create a new one
		if err == base.ErrKeyNotFound {
			bm = structx.NewBitMap()
			bm.Add(bit)
			sd.Set(key, bm)

		} else if err != nil {
			_err = err

		} else {
			bm.Add(bit)
		}
	})

	return
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

	for len(line) > 1 {
		op := line[0]
		line = line[1:]

		// parse key
		var key []byte
		key, line = parseLine(line, recordSepChar)

		switch Operation(op) {
		case OpSetTx:
			var ttl, val []byte

			recordType := RecordType(line[0])
			line = line[1:]

			// parse ttl
			ttl, line = parseLine(line, recordSepChar)
			ts, _ := strconv.ParseInt(*base.B2S(ttl), _base, 64)
			ts *= timeCarry

			// parse val
			val, line = parseLine(line, recordSepChar)

			// check if expired
			if ts < globalTime && ts != noTTL {
				continue
			}

			switch recordType {
			case RecordString:
				s.SetTx(*base.B2S(key), val, ts)

			case RecordMap:
				var m Map
				if err := sonic.Unmarshal(val, &m); err != nil {
					panic(err)
				}
				s.SetTx(*base.B2S(key), m, ts)

			case RecordBitMap:
				var m BitMap
				if err := m.UnmarshalJSON(val); err != nil {
					panic(err)
				}
				s.SetTx(*base.B2S(key), m, ts)

			default:
				panic(fmt.Errorf("%v: %d", base.ErrUnSupportDataType, recordType))
			}

		case OpHSet:
			var field, val []byte

			// parse field
			field, line = parseLine(line, recordSepChar)

			// parse val
			val, line = parseLine(line, recordSepChar)

			hmap, err := s.getMap(*base.B2S(key))
			hmap[*base.B2S(field)] = val
			// override
			if err != nil {
				s.Set(*base.B2S(key), hmap)
			}

		case OpSetBit:
			var src []byte

			// parse bit
			src, line = parseLine(line, recordSepChar)
			bit, err := strconv.ParseUint(*base.B2S(src), _base, 64)
			if err != nil {
				return
			}

			bm, err := s.getBitMap(*base.B2S(key))
			bm.Add(uint32(bit))
			// override
			if err != nil {
				s.Set(*base.B2S(key), bm)
			}

		case OpHRemove:
			var field []byte

			// parse field
			field, line = parseLine(line, recordSepChar)

			hmap, err := s.getMap(*base.B2S(key))
			if err != nil {
				return
			}
			delete(hmap, *base.B2S(field))

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
		var recordType RecordType

		switch v.(type) {
		case String:
			recordType = RecordString
		case Map:
			recordType = RecordMap
		case BitMap:
			recordType = RecordBitMap
		case List:
			recordType = RecordList
		case Set:
			recordType = RecordSet
		default:
			panic(base.ErrUnSupportDataType)
		}

		// SetTx
		if cd := NewCoder(OpSetTx).Type(recordType).String(key).Int64(i / timeCarry).Any(v); cd.err == nil {
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

// getMap
func (sd *storeShard) getMap(key string) (Map, error) {
	var m Map
	return getValue(sd, key, m)
}

// getBitMap
func (sd *storeShard) getBitMap(key string) (BitMap, error) {
	var bm BitMap
	return getValue(sd, key, bm)
}

func getValue[T any](sd *storeShard, key string, vptr T) (T, error) {
	m, ok := sd.Get(key)
	if ok {
		m, ok := m.(T)
		if ok {
			return m, nil
		}
		return vptr, base.ErrWrongType
	}
	return vptr, base.ErrKeyNotFound
}
