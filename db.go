// Package rotom provides an in-memory key-value database.
package rotom

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/wal"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/codeman"
	"github.com/xgzlucario/rotom/structx"
)

const (
	noTTL = 0

	mergeTypeAnd byte = iota + 1
	mergeTypeOr
	mergeTypeXOr

	fileLockName = "FLOCK"
)

// Operations.
type Operation byte

const (
	OpSetTx Operation = iota
	OpSetTTL
	OpRemove
	// map
	OpHSet
	OpHRemove
	// set
	OpSAdd
	OpSRemove
	OpSMerge // union, inter, diff
	// list
	OpLPush
	OpRPush
	OpLPop
	OpRPop
	// bitmap
	OpBitSet
	OpBitFlip
	OpBitMerge // or, and, xor
	// zset
	OpZAdd
	OpZIncr
	OpZRemove
)

type Cmd struct {
	op   Operation
	hook func(*DB, *codeman.Reader) error
}

// cmdTable defines how each command recover database from redo log(wal log).
var cmdTable = []Cmd{
	{OpSetTx, func(db *DB, reader *codeman.Reader) error {
		// type, key, ts, val
		tp := reader.Int64()
		key := reader.Str()
		ts := reader.Int64()
		val := reader.RawBytes()

		switch tp {
		case TypeList:
			ls := structx.NewList()
			if err := ls.Unmarshal(val); err != nil {
				return err
			}
			db.cm.Set(key, ls)

		case TypeSet:
			s := structx.NewSet()
			if err := s.UnmarshalJSON(val); err != nil {
				return err
			}
			db.cm.Set(key, s)

		case TypeMap:
			m := structx.NewSyncMap()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			db.cm.Set(key, m)

		case TypeBitmap:
			m := structx.NewBitmap()
			if err := m.UnmarshalBinary(val); err != nil {
				return err
			}
			db.cm.Set(key, m)

		case TypeZSet:
			m := structx.NewZSet[string, float64]()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			db.cm.Set(key, m)

		default:
			// default String, check ttl.
			if ts > cache.GetNanoSec() || ts == noTTL {
				db.SetTx(key, val, ts)
			}
		}
		return nil
	}},
	{OpSetTTL, func(db *DB, reader *codeman.Reader) error {
		// key, ts
		db.SetTTL(reader.Str(), reader.Int64())
		return nil
	}},
	{OpRemove, func(db *DB, reader *codeman.Reader) error {
		// keys
		db.Remove(reader.StrSlice()...)
		return nil
	}},
	{OpHSet, func(db *DB, reader *codeman.Reader) error {
		// key, field, val
		return db.HSet(reader.Str(), reader.Str(), reader.RawBytes())
	}},
	{OpHRemove, func(db *DB, reader *codeman.Reader) error {
		// key, fields
		_, err := db.HRemove(reader.Str(), reader.StrSlice()...)
		return err
	}},
	{OpSAdd, func(db *DB, reader *codeman.Reader) error {
		// key, items
		_, err := db.SAdd(reader.Str(), reader.StrSlice()...)
		return err
	}},
	{OpSRemove, func(db *DB, reader *codeman.Reader) error {
		// key, items
		return db.SRemove(reader.Str(), reader.StrSlice()...)
	}},
	{OpSMerge, func(db *DB, reader *codeman.Reader) error {
		// op, key, items
		op := reader.Byte()
		key := reader.Str()
		items := reader.StrSlice()

		switch op {
		case mergeTypeAnd:
			return db.SInter(key, items...)
		case mergeTypeOr:
			return db.SUnion(key, items...)
		default:
			return db.SDiff(key, items...)
		}
	}},
	{OpLPush, func(db *DB, reader *codeman.Reader) error {
		// key, items
		return db.LPush(reader.Str(), reader.StrSlice()...)
	}},
	{OpRPush, func(db *DB, reader *codeman.Reader) error {
		// key, items
		return db.RPush(reader.Str(), reader.StrSlice()...)
	}},
	{OpLPop, func(db *DB, reader *codeman.Reader) error {
		// key
		_, err := db.LPop(reader.Str())
		return err
	}},
	{OpRPop, func(db *DB, reader *codeman.Reader) error {
		// key
		_, err := db.RPop(reader.Str())
		return err
	}},
	{OpBitSet, func(db *DB, reader *codeman.Reader) error {
		// key, val, offsets
		_, err := db.BitSet(reader.Str(), reader.Bool(), reader.Uint32Slice()...)
		return err
	}},
	{OpBitFlip, func(db *DB, reader *codeman.Reader) error {
		// key, offset
		return db.BitFlip(reader.Str(), reader.Uint32(), reader.Uint32())
	}},
	{OpBitMerge, func(db *DB, reader *codeman.Reader) error {
		// op, key, items
		op := reader.Byte()
		key := reader.Str()
		items := reader.StrSlice()

		switch op {
		case mergeTypeAnd:
			return db.BitAnd(key, items...)
		case mergeTypeOr:
			return db.BitOr(key, items...)
		default:
			return db.BitXor(key, items...)
		}
	}},
	{OpZAdd, func(db *DB, reader *codeman.Reader) error {
		// key, field, score
		return db.ZAdd(reader.Str(), reader.Str(), reader.Float64())
	}},
	{OpZIncr, func(db *DB, reader *codeman.Reader) error {
		// key, field, score
		_, err := db.ZIncr(reader.Str(), reader.Str(), reader.Float64())
		return err
	}},
	{OpZRemove, func(db *DB, reader *codeman.Reader) error {
		// key, field
		return db.ZRemove(reader.Str(), reader.Str())
	}},
}

// Type is the data type for Rotom.
type Type = int64

const (
	TypeString Type = iota + 1
	TypeMap
	TypeSet
	TypeList
	TypeZSet
	TypeBitmap
)

// Type aliases for built-in types.
type (
	String = []byte
	Map    = *structx.SyncMap
	Set    = *structx.Set
	List   = *structx.List
	ZSet   = *structx.ZSet[string, float64]
	BitMap = *structx.Bitmap
)

// DB represents a rotom database.
type DB struct {
	mu       sync.Mutex
	options  *Options
	fileLock *flock.Flock
	wal      *wal.WAL
	loading  bool // is loading finished from wal.
	closed   bool
	m        *cache.GigaCache                // data for bytes.
	cm       cmap.ConcurrentMap[string, any] // data for built-in types.
	cron     *cron.Cron                      // cron scheduler for auto merge task.
}

// Open create a new db instance by options.
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create wal.
	walOptions := wal.DefaultOptions
	walOptions.DirPath = options.DirPath
	walOptions.Sync = (options.SyncPolicy == Sync)
	wal, err := wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	// create file lock, prevent multiple processes from using the same db directory.
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// init db instance.
	cacheOptions := cache.DefaultOptions
	cacheOptions.ShardCount = options.ShardCount
	db := &DB{
		options:  &options,
		loading:  true,
		fileLock: fileLock,
		wal:      wal,
		m:        cache.New(cacheOptions),
		cm:       cmap.New[any](),
	}

	// load db from wal.
	if err := db.loadFromWal(); err != nil {
		return nil, err
	}
	db.loading = false

	// start backend cron job.
	db.cron = cron.New(cron.WithSeconds())
	if db.options.SyncPolicy == EverySecond {
		if _, err = db.cron.AddFunc("* * * * * ?", func() {
			db.Sync()
		}); err != nil {
			panic(err)
		}
	}
	if len(options.ShrinkCronExpr) > 0 {
		if _, err = db.cron.AddFunc(options.ShrinkCronExpr, func() {
			db.Shrink()
		}); err != nil {
			return nil, err
		}
	}

	db.cron.Start()

	return db, nil
}

// Close the database, close all data files and release file lock.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if err := db.wal.Close(); err != nil {
		return err
	}

	// release file lock.
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	db.cron.Stop()
	db.closed = true

	return nil
}

// GetOptions
func (db *DB) GetOptions() Options {
	return *db.options
}

func (db *DB) encode(cd *codeman.Codec) {
	if db.loading {
		return
	}
	db.wal.Write(cd.Content())
	cd.Recycle()
}

// Sync
func (db *DB) Sync() error {
	return db.wal.Sync()
}

func newCodec(op Operation) *codeman.Codec {
	return codeman.NewCodec().Byte(byte(op))
}

// Get
func (db *DB) Get(key string) ([]byte, int64, error) {
	// check
	if db.cm.Has(key) {
		return nil, 0, ErrTypeAssert
	}
	// get
	val, ts, ok := db.m.Get(key)
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	return val, ts, nil
}

// Set store key-value pair.
func (db *DB) Set(key string, val []byte) {
	db.SetTx(key, val, noTTL)
}

// SetEx store key-value pair with ttl.
func (db *DB) SetEx(key string, val []byte, ttl time.Duration) {
	db.SetTx(key, val, cache.GetNanoSec()+int64(ttl))
}

// SetTx store key-value pair with deadline.
func (db *DB) SetTx(key string, val []byte, ts int64) {
	if ts < 0 {
		return
	}
	db.encode(newCodec(OpSetTx).Int(TypeString).Str(key).Int(ts).Bytes(val))
	db.m.SetTx(key, val, ts)
}

// SetTTL set expired time of key-value.
func (db *DB) SetTTL(key string, ts int64) bool {
	if ts < 0 {
		return false
	}
	db.encode(newCodec(OpSetTTL).Str(key).Int(ts))
	return db.m.SetTTL(key, ts)
}

// Remove
func (db *DB) Remove(keys ...string) (n int) {
	db.encode(newCodec(OpRemove).StrSlice(keys))
	for _, key := range keys {
		if db.m.Remove(key) {
			n++
		} else if db.cm.Has(key) {
			db.cm.Remove(key)
			n++
		}
	}
	return
}

// Len
func (db *DB) Len() int {
	return db.m.Stat().Len + db.cm.Count()
}

// GC triggers the garbage collection to evict expired kv datas.
func (db *DB) GC() {
	db.mu.Lock()
	db.m.Migrate()
	db.mu.Unlock()
}

// Scan
func (db *DB) Scan(f func([]byte, []byte, int64) bool) {
	db.m.Scan(func(key, value []byte, ttl int64) bool {
		return f(key, value, ttl)
	})
}

// HGet
func (db *DB) HGet(key, field string) ([]byte, error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return nil, err
	}
	res, ok := m.Get(field)
	if !ok {
		return nil, ErrFieldNotFound
	}
	return res, nil
}

// HLen
func (db *DB) HLen(key string) (int, error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return 0, err
	}
	return m.Len(), nil
}

// HSet
func (db *DB) HSet(key, field string, val []byte) error {
	m, err := db.fetchMap(key, true)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpHSet).Str(key).Str(field).Bytes(val))
	m.Set(field, val)
	return nil
}

// HRemove
func (db *DB) HRemove(key string, fields ...string) (n int, err error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return 0, err
	}
	db.encode(newCodec(OpHRemove).Str(key).StrSlice(fields))
	for _, k := range fields {
		if m.Remove(k) {
			n++
		}
	}
	return
}

// HKeys
func (db *DB) HKeys(key string) ([]string, error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return nil, err
	}
	return m.Keys(), nil
}

// SAdd
func (db *DB) SAdd(key string, items ...string) (int, error) {
	s, err := db.fetchSet(key, true)
	if err != nil {
		return 0, err
	}
	db.encode(newCodec(OpSAdd).Str(key).StrSlice(items))
	return s.Append(items...), nil
}

// SRemove
func (db *DB) SRemove(key string, items ...string) error {
	s, err := db.fetchSet(key)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpSRemove).Str(key).StrSlice(items))
	s.RemoveAll(items...)
	return nil
}

// SHas returns whether the given items are all in the set.
func (db *DB) SHas(key string, items ...string) (bool, error) {
	s, err := db.fetchSet(key)
	if err != nil {
		return false, err
	}
	return s.Contains(items...), nil
}

// SCard
func (db *DB) SCard(key string) (int, error) {
	s, err := db.fetchSet(key)
	if err != nil {
		return 0, err
	}
	return s.Cardinality(), nil
}

// SMembers
func (db *DB) SMembers(key string) ([]string, error) {
	s, err := db.fetchSet(key)
	if err != nil {
		return nil, err
	}
	return s.ToSlice(), nil
}

// SUnion
func (db *DB) SUnion(dst string, src ...string) error {
	srcSet, err := db.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := db.fetchSet(key)
		if err != nil {
			return err
		}
		s.Union(ts)
	}
	db.encode(newCodec(OpSMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
	db.cm.Set(dst, s)

	return nil
}

// SInter
func (db *DB) SInter(dst string, src ...string) error {
	srcSet, err := db.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := db.fetchSet(key)
		if err != nil {
			return err
		}
		s.Intersect(ts)
	}
	db.encode(newCodec(OpSMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
	db.cm.Set(dst, s)

	return nil
}

// SDiff
func (db *DB) SDiff(dst string, src ...string) error {
	srcSet, err := db.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := db.fetchSet(key)
		if err != nil {
			return err
		}
		s.Difference(ts)
	}
	db.encode(newCodec(OpSMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
	db.cm.Set(dst, s)

	return nil
}

// LPush
func (db *DB) LPush(key string, items ...string) error {
	ls, err := db.fetchList(key, true)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpLPush).Str(key).StrSlice(items))
	ls.LPush(items...)

	return nil
}

// RPush
func (db *DB) RPush(key string, items ...string) error {
	ls, err := db.fetchList(key, true)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpRPush).Str(key).StrSlice(items))
	ls.RPush(items...)

	return nil
}

// LIndex
func (db *DB) LIndex(key string, i int) (string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.Index(i)
	if !ok {
		return "", ErrIndexOutOfRange
	}
	return res, nil
}

// LPop
func (db *DB) LPop(key string) (string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.LPop()
	if !ok {
		return "", ErrEmptyList
	}
	db.encode(newCodec(OpLPop).Str(key))

	return res, nil
}

// RPop
func (db *DB) RPop(key string) (string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.RPop()
	if !ok {
		return "", ErrEmptyList
	}
	db.encode(newCodec(OpRPop).Str(key))

	return res, nil
}

// LLen
func (db *DB) LLen(key string) (int, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return 0, err
	}
	return ls.Size(), nil
}

// LKeys
func (db *DB) LKeys(key string) ([]string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return nil, err
	}
	return ls.Keys(), nil
}

// BitTest
func (db *DB) BitTest(key string, offset uint32) (bool, error) {
	bm, err := db.fetchBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (db *DB) BitSet(key string, val bool, offsets ...uint32) (int, error) {
	bm, err := db.fetchBitMap(key, true)
	if err != nil {
		return 0, err
	}
	db.encode(newCodec(OpBitSet).Str(key).Bool(val).Uint32Slice(offsets))

	var n int
	if val {
		n = bm.Add(offsets...)
	} else {
		n = bm.Remove(offsets...)
	}

	return n, nil
}

// BitFlip
func (db *DB) BitFlip(key string, start, end uint32) error {
	bm, err := db.fetchBitMap(key)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpBitFlip).Str(key).Uint32(start).Uint32(end))
	bm.Flip(uint64(start), uint64(end))

	return nil
}

// BitAnd
func (db *DB) BitAnd(dst string, src ...string) error {
	bm, err := db.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := db.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.And(tbm)
	}
	db.encode(newCodec(OpBitMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
	db.cm.Set(dst, bm)

	return nil
}

// BitOr
func (db *DB) BitOr(dst string, src ...string) error {
	bm, err := db.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := db.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Or(tbm)
	}
	db.encode(newCodec(OpBitMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
	db.cm.Set(dst, bm)

	return nil
}

// BitXor
func (db *DB) BitXor(dst string, src ...string) error {
	bm, err := db.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := db.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Xor(tbm)
	}
	db.encode(newCodec(OpBitMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
	db.cm.Set(dst, bm)

	return nil
}

// BitArray
func (db *DB) BitArray(key string) ([]uint32, error) {
	bm, err := db.fetchBitMap(key)
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

// BitCount
func (db *DB) BitCount(key string) (uint64, error) {
	bm, err := db.fetchBitMap(key)
	if err != nil {
		return 0, err
	}
	return bm.Len(), err
}

// ZGet
func (db *DB) ZGet(zset, key string) (float64, error) {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return 0, err
	}
	score, ok := zs.Get(key)
	if !ok {
		return 0, ErrKeyNotFound
	}
	return score, nil
}

// ZCard
func (db *DB) ZCard(zset string) (int, error) {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return 0, err
	}
	return zs.Len(), nil
}

// ZIter
func (db *DB) ZIter(zset string, f func(string, float64) bool) error {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return err
	}
	zs.Iter(func(k string, s float64) bool {
		return f(k, s)
	})
	return nil
}

// ZAdd
func (db *DB) ZAdd(zset, key string, score float64) error {
	zs, err := db.fetchZSet(zset, true)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpZAdd).Str(zset).Str(key).Float(score))
	zs.Set(key, score)

	return nil
}

// ZIncr
func (db *DB) ZIncr(zset, key string, incr float64) (float64, error) {
	zs, err := db.fetchZSet(zset, true)
	if err != nil {
		return 0, err
	}
	db.encode(newCodec(OpZIncr).Str(zset).Str(key).Float(incr))

	return zs.Incr(key, incr), nil
}

// ZRemove
func (db *DB) ZRemove(zset string, key string) error {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpZRemove).Str(zset).Str(key))
	zs.Delete(key)

	return nil
}

// loadFromWal load data to mem from wal.
func (db *DB) loadFromWal() error {
	reader := db.wal.NewReader()
	for {
		data, _, err := reader.Next()
		if err == io.EOF {
			break
		}

		// read all records.
		for rd := codeman.NewReader(data); !rd.Done(); {
			op := Operation(rd.Byte())
			if err := cmdTable[op].hook(db, rd); err != nil {
				return err
			}
		}
	}
	return nil
}

// Shrink rewrite db file.
func (db *DB) Shrink() error {
	if !db.mu.TryLock() {
		return ErrShrinkRunning
	}
	defer db.mu.Unlock()

	// create new segment file.
	if err := db.wal.OpenNewActiveSegment(); err != nil {
		return err
	}

	// marshal bytes.
	db.m.Scan(func(key, value []byte, ts int64) bool {
		cd := newCodec(OpSetTx).Int(TypeString).Bytes(key).Int(ts).Bytes(value)
		db.wal.Write(cd.Content())
		cd.Recycle()
		return false
	})

	// marshal built-in types.
	var types Type
	var data []byte
	var err error

	for t := range db.cm.IterBuffered() {
		switch item := t.Val.(type) {
		case Map:
			types = TypeMap
			data, err = item.MarshalJSON()
		case BitMap:
			types = TypeBitmap
			data, err = item.MarshalBinary()
		case List:
			types = TypeList
			data = item.Marshal()
		case Set:
			types = TypeSet
			data, err = item.MarshalJSON()
		case ZSet:
			types = TypeZSet
			data, err = item.MarshalJSON()
		}

		if err != nil {
			return err
		}
		cd := newCodec(OpSetTx).Int(types).Str(t.Key).Int(0).Bytes(data)
		db.wal.Write(cd.Content())
		cd.Recycle()
	}

	// sync
	if err := db.wal.Sync(); err != nil {
		return err
	}

	// remove all old segment files.
	return db.removeOldSegments(db.wal.ActiveSegmentID())
}

func (db *DB) removeOldSegments(maxSegmentID uint32) error {
	maxSegmentName := fmt.Sprintf("%09d", maxSegmentID)

	return filepath.WalkDir(db.options.DirPath, func(path string, file os.DirEntry, err error) error {
		if file.Name() < maxSegmentName {
			return os.Remove(path)
		}
		return err
	})
}

func (db *DB) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(db, key, func() Map {
		return structx.NewSyncMap()
	}, setnx...)
}

func (db *DB) fetchSet(key string, setnx ...bool) (s Set, err error) {
	return fetch(db, key, func() Set {
		return structx.NewSet()
	}, setnx...)
}

func (db *DB) fetchList(key string, setnx ...bool) (m List, err error) {
	return fetch(db, key, func() List {
		return structx.NewList()
	}, setnx...)
}

func (db *DB) fetchBitMap(key string, setnx ...bool) (bm BitMap, err error) {
	return fetch(db, key, func() BitMap {
		return structx.NewBitmap()
	}, setnx...)
}

func (db *DB) fetchZSet(key string, setnx ...bool) (z ZSet, err error) {
	return fetch(db, key, func() ZSet {
		return structx.NewZSet[string, float64]()
	}, setnx...)
}

func fetch[T any](db *DB, key string, new func() T, setnx ...bool) (v T, err error) {
	if _, _, ok := db.m.Get(key); ok {
		return v, ErrWrongType
	}

	item, ok := db.cm.Get(key)
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, fmt.Errorf("%w: %T->%T", ErrWrongType, item, v)
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		db.cm.Set(key, v)
	}
	return v, nil
}
