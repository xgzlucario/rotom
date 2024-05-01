// Package rotom provides an in-memory key-value database.
package rotom

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
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
)

// Operation needs redo.
type Operation byte

const (
	OpSetTx     Operation = iota // for string
	OpSetObject                  // for data structure
	OpSetTTL
	OpRemove
	// map
	OpHSet
	OpHRemove
	// set
	OpSAdd
	OpSRemove
	OpSMerge
	// list
	OpLPush
	OpRPush
	OpLPop
	OpRPop
	OpLSet
	// bitmap
	OpBitSet
	OpBitFlip
	OpBitMerge
	// zset
	OpZAdd
	OpZIncr
	OpZRemove
)

type Cmd struct {
	op   Operation
	hook func(*DB, *codeman.Reader) error
}

// cmdTable defines how each command recover database from redo log.
var cmdTable = []Cmd{
	{OpSetTx, func(db *DB, reader *codeman.Reader) error {
		// key, ts, val
		key := reader.Str()
		ts := reader.Int64()
		val := reader.RawBytes()
		// check ttl.
		if ts > cache.GetNanoSec() || ts == noTTL {
			db.SetTx(key, val, ts)
		}
		return nil
	}},
	{OpSetObject, func(db *DB, reader *codeman.Reader) error {
		// type, key, val
		tp := reader.Int64()
		key := reader.Str()
		val := reader.RawBytes()

		switch tp {
		case TypeList:
			ls := structx.NewList()
			if err := ls.UnmarshalBinary(val); err != nil {
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
			m := structx.NewMap()
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
			m := structx.NewZSet()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			db.cm.Set(key, m)
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
		// key, field, val, ts
		return db.HSetTx(reader.Str(), reader.Str(), reader.RawBytes(), reader.Int64())
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
	{OpLSet, func(db *DB, reader *codeman.Reader) error {
		// key, index, item
		_, err := db.LSet(reader.Str(), int(reader.Int64()), reader.Str())
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
		return db.ZAdd(reader.Str(), reader.Str(), reader.Int64())
	}},
	{OpZIncr, func(db *DB, reader *codeman.Reader) error {
		// key, field, score
		_, err := db.ZIncr(reader.Str(), reader.Str(), reader.Int64())
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
	Map    = *structx.Map
	Set    = *structx.Set
	List   = *structx.List
	ZSet   = *structx.ZSet
	BitMap = *structx.Bitmap
)

// DB represents a rotom database.
type DB struct {
	mu      sync.Mutex
	options *Options
	wal     *wal.WAL
	loading bool // is loading finished from wal.
	closed  bool
	m       *cache.GigaCache                // data for strings.
	cm      cmap.ConcurrentMap[string, any] // data for built-in types.
}

// Open create a new db instance by options.
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create wal.
	walOptions := wal.DefaultOptions
	walOptions.DirPath = options.DirPath
	wal, err := wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	// init db instance.
	cacheOptions := cache.DefaultOptions
	cacheOptions.ShardCount = options.ShardCount
	db := &DB{
		options: &options,
		loading: true,
		wal:     wal,
		m:       cache.New(cacheOptions),
		cm:      cmap.New[any](),
	}

	// load db from wal.
	if err := db.loadFromWal(); err != nil {
		return nil, err
	}
	db.loading = false

	return db, nil
}

// Close the database, close all data files and release file lock.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true

	return db.wal.Close()
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
	db.BatchSet(&Batch{key, val, ts})
}

// SetTTL set expired time of key-value.
func (db *DB) SetTTL(key string, ts int64) bool {
	if ts < 0 {
		return false
	}
	db.encode(newCodec(OpSetTTL).Str(key).Int(ts))
	return db.m.SetTTL(key, ts)
}

// Incr increase number to key.
func (db *DB) Incr(key string, incr int64) (n int64, err error) {
	val, ts, ok := db.m.Get(key)
	if ok {
		n, err = strconv.ParseInt(b2s(val), 10, 64)
		if err != nil {
			return
		}
	}
	n += incr
	valNew := strconv.FormatInt(n, 10)
	db.m.SetTx(key, s2b(&valNew), ts)
	db.encode(newCodec(OpSetTx).Str(key).Int(ts).Str(valNew))
	return
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
	db.m.Scan(func(key, val []byte, ttl int64) bool {
		return f(key, val, ttl)
	})
}

// HGet
func (db *DB) HGet(key, field string) ([]byte, error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return nil, err
	}
	res, _, ok := m.Get(field)
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
	return db.BatchHSet(key, &Batch{Key: field, Val: val})
}

// HSetTx
func (db *DB) HSetTx(key, field string, val []byte, ts int64) error {
	return db.BatchHSet(key, &Batch{Key: field, Val: val, Timestamp: ts})
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

// LSet
func (db *DB) LSet(key string, index int, item string) (bool, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return false, err
	}
	db.encode(newCodec(OpLSet).Str(key).Int(int64(index)).Str(item))
	return ls.Set(index, item), nil
}

// LRange
func (db *DB) LRange(key string, start, end int, f func(string) (stop bool)) error {
	ls, err := db.fetchList(key)
	if err != nil {
		return err
	}
	ls.Range(start, end, func(data []byte) bool {
		return f(string(data))
	})
	return nil
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
func (db *DB) ZGet(zset, key string) (int64, error) {
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
func (db *DB) ZIter(zset string, f func(string, int64) bool) error {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return err
	}
	zs.Iter(func(k string, s int64) bool {
		return f(k, s)
	})
	return nil
}

// ZAdd
func (db *DB) ZAdd(zset, key string, score int64) error {
	zs, err := db.fetchZSet(zset, true)
	if err != nil {
		return err
	}
	db.encode(newCodec(OpZAdd).Str(zset).Str(key).Int(score))
	zs.Set(key, score)

	return nil
}

// ZIncr
func (db *DB) ZIncr(zset, key string, incr int64) (int64, error) {
	zs, err := db.fetchZSet(zset, true)
	if err != nil {
		return 0, err
	}
	db.encode(newCodec(OpZIncr).Str(zset).Str(key).Int(incr))

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

// Shrink uses `RDB` to create database snapshots to disk.
func (db *DB) Shrink() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// create new segment file.
	segmentId := db.wal.ActiveSegmentID()
	if err := db.wal.OpenNewActiveSegment(); err != nil {
		return err
	}

	var pendWriteSize int
	// writeAll write all wal pending data to disk.
	writeAll := func() {
		db.wal.WriteAll()
		pendWriteSize = 0
	}
	// write write to wal buffer pending.
	write := func(b []byte) {
		db.wal.PendingWrites(b)
		pendWriteSize += len(b)
		if pendWriteSize >= wal.MB {
			writeAll()
		}
	}

	// marshal string datas.
	db.m.Scan(func(key, val []byte, ts int64) bool {
		cd := newCodec(OpSetTx).Bytes(key).Int(ts).Bytes(val)
		write(cd.Content())
		return true
	})
	writeAll()

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
			data, err = item.MarshalBinary()
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
		cd := newCodec(OpSetObject).Int(types).Str(t.Key).Bytes(data)
		write(cd.Content())
	}
	writeAll()

	if err := db.wal.Sync(); err != nil {
		return err
	}

	// remove all old segment files.
	return db.removeOldSegments(segmentId)
}

func (db *DB) removeOldSegments(maxSegmentId uint32) error {
	segmentName := fmt.Sprintf("%09d.SEG", maxSegmentId)

	return filepath.WalkDir(db.options.DirPath, func(path string, file os.DirEntry, err error) error {
		if file.Name() <= segmentName {
			return os.Remove(path)
		}
		return err
	})
}

func (db *DB) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(db, key, func() Map {
		return structx.NewMap()
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
		return structx.NewZSet()
	}, setnx...)
}

func fetch[T any](db *DB, key string, new func() T, setnx ...bool) (v T, err error) {
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
