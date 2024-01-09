// Package rotom provides an in-memory key-value databasdb.
package rotom

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	cmap "github.com/orcaman/concurrent-map/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/codeman"
	"github.com/xgzlucario/rotom/structx"
	"github.com/xgzlucario/rotom/wal"
)

const (
	noTTL = 0
)

// Operations.
type Operation byte

const (
	OpSetTx Operation = iota
	OpRemove
	// map
	OpHSet
	OpHRemove
	// set
	OpSAdd
	OpSRemove
	OpSMerge // union, inter, diff
	// list
	OpLPush // lpush, rpush
	OpLPop  // lpop, rpop
	// bitmap
	OpBitSet
	OpBitFlip
	OpBitMerge // or, and, xor
	// zset
	OpZAdd
	OpZIncr
	OpZRemove
)

const (
	mergeTypeAnd byte = iota + 1
	mergeTypeOr
	mergeTypeXOr

	listDirectionLeft  = 'L'
	listDirectionRight = 'R'

	fileLockName = "flock"
)

// Cmd
type Cmd struct {
	op   Operation
	hook func(*DB, *codeman.Parser) error
}

// cmdTable defines the rNum and callback function required for the operation.
var cmdTable = []Cmd{
	{OpSetTx, func(db *DB, decoder *codeman.Parser) error {
		// type, key, ts, val
		tp := decoder.ParseVarint().Int64()
		key := decoder.Parse().Str()
		ts := decoder.ParseVarint().Int64()
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch tp {
		case TypeString:
			// check ttl
			if ts > cache.GetNanoSec() || ts == noTTL {
				db.SetTx(key, val, ts)
			}

		case TypeList:
			ls := structx.NewList[string]()
			if err := ls.UnmarshalJSON(val); err != nil {
				return err
			}
			db.cm.Set(key, ls)

		case TypeSet:
			s := structx.NewSet[string]()
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
			return fmt.Errorf("%w: %d", ErrUnSupportDataType, tp)
		}
		return nil
	}},

	{OpRemove, func(db *DB, decoder *codeman.Parser) error {
		// keys
		keys := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}
		db.Remove(keys...)
		return nil
	}},

	{OpHSet, func(db *DB, decoder *codeman.Parser) error {
		// key, field, val
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}
		return db.HSet(key, field, val)
	}},

	{OpHRemove, func(db *DB, decoder *codeman.Parser) error {
		// key, fields
		key := decoder.Parse().Str()
		fields := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}
		_, err := db.HRemove(key, fields...)
		return err
	}},

	{OpSAdd, func(db *DB, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}
		_, err := db.SAdd(key, items...)
		return err
	}},

	{OpSRemove, func(db *DB, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}
		return db.SRemove(key, items...)
	}},

	{OpSMerge, func(db *DB, decoder *codeman.Parser) error {
		// op, key, items
		op := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch op {
		case mergeTypeAnd:
			return db.SInter(key, items...)
		case mergeTypeOr:
			return db.SUnion(key, items...)
		case mergeTypeXOr:
			return db.SDiff(key, items...)
		}
		return ErrInvalidMergeOperation
	}},

	{OpLPush, func(db *DB, decoder *codeman.Parser) error {
		// direct, key, items
		direct := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		if direct == listDirectionLeft {
			return db.LLPush(key, items...)
		}
		return db.LRPush(key, items...)
	}},

	{OpLPop, func(db *DB, decoder *codeman.Parser) (err error) {
		// direct, key
		direct := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()

		if decoder.Error != nil {
			return decoder.Error
		}

		if direct == listDirectionLeft {
			_, err = db.LLPop(key)
			return
		}
		_, err = db.LRPop(key)
		return
	}},

	{OpBitSet, func(db *DB, decoder *codeman.Parser) error {
		// key, offset, val
		key := decoder.Parse().Str()
		val := decoder.ParseVarint().Bool()
		offsets := decoder.Parse().Uint32Slice()

		if decoder.Error != nil {
			return decoder.Error
		}
		_, err := db.BitSet(key, val, offsets...)
		return err
	}},

	{OpBitFlip, func(db *DB, decoder *codeman.Parser) error {
		// key, offset
		key := decoder.Parse().Str()
		offset := decoder.ParseVarint().Uint32()

		if decoder.Error != nil {
			return decoder.Error
		}
		return db.BitFlip(key, offset)
	}},

	{OpBitMerge, func(db *DB, decoder *codeman.Parser) error {
		// op, key, items
		op := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch op {
		case mergeTypeAnd:
			return db.BitAnd(key, items...)
		case mergeTypeOr:
			return db.BitOr(key, items...)
		case mergeTypeXOr:
			return db.BitXor(key, items...)
		}
		return ErrInvalidMergeOperation
	}},

	{OpZAdd, func(db *DB, decoder *codeman.Parser) error {
		// key, field, score
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		score := decoder.ParseVarint().Float64()

		if decoder.Error != nil {
			return decoder.Error
		}
		return db.ZAdd(key, field, score)
	}},

	{OpZIncr, func(db *DB, decoder *codeman.Parser) error {
		// key, field, score
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		score := decoder.ParseVarint().Float64()

		if decoder.Error != nil {
			return decoder.Error
		}
		_, err := db.ZIncr(key, field, score)
		return err
	}},

	{OpZRemove, func(db *DB, decoder *codeman.Parser) error {
		// key, field
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()

		if decoder.Error != nil {
			return decoder.Error
		}
		return db.ZRemove(key, field)
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

// Type aliases for structx types.
type (
	String = []byte
	Map    = *structx.SyncMap
	Set    = *structx.Set[string]
	List   = *structx.List[string]
	ZSet   = *structx.ZSet[string, float64]
	BitMap = *structx.Bitmap
)

// DB represents a rotom database engindb.
type DB struct {
	// mu guards wal data.
	mu sync.Mutex
	*Options

	// context.
	ctx    context.Context
	cancel context.CancelFunc

	// ticker.
	syncTicker   *time.Ticker
	shrinkTicker *time.Ticker

	// if db loading encode not allowed.
	fileLock *flock.Flock
	loading  bool

	// write ahead log.
	wal *wal.Log

	// data for bytes.
	m *cache.GigaCache

	// data for built-in structurdb.
	cm cmap.ConcurrentMap[string, any]
}

// Open opens a database by options.
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create wal.
	wal, err := wal.Open(options.DirPath)
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
	ctx, cancel := context.WithCancel(context.Background())
	cacheOptions := cache.DefaultOption
	cacheOptions.ShardCount = options.ShardCount
	db := &DB{
		Options:  &options,
		ctx:      ctx,
		cancel:   cancel,
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

	// start timer.
	if db.SyncPolicy == EverySecond {
		// start sync ticker.
		db.syncTicker = time.NewTicker(time.Second)
		go func() {
			for {
				select {
				case <-db.ctx.Done():
					db.syncTicker.Stop()
					return
				case <-db.syncTicker.C:
					db.mu.Lock()
					db.wal.Sync()
					db.mu.Unlock()
				}
			}
		}()

		// start shrink ticker.
		db.shrinkTicker = time.NewTicker(db.ShrinkInterval)
		go func() {
			for {
				select {
				case <-db.ctx.Done():
					db.shrinkTicker.Stop()
					return
				case <-db.shrinkTicker.C:
					if err := db.shrink(); err != nil {
						panic(err)
					}
				}
			}
		}()
	}

	return db, nil
}

// Close the database, close all data files and release file lock.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	select {
	case <-db.ctx.Done():
		return ErrDatabaseClosed
	default:
		if err := db.wal.Close(); err != nil {
			return err
		}
		db.cancel()
		return db.fileLock.Unlock()
	}
}

// encode
func (db *DB) encode(cd *codeman.Codec) {
	if db.loading {
		return
	}
	db.mu.Lock()
	db.wal.Write(cd.Content())
	db.mu.Unlock()
	cd.Recycle()
}

// NewCodec
func NewCodec(op Operation) *codeman.Codec {
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

// SetTx store key-value pair with deadlindb.
func (db *DB) SetTx(key string, val []byte, ts int64) {
	// you should check ts outside.
	if ts < 0 {
		return
	}
	db.encode(NewCodec(OpSetTx).Int(TypeString).Str(key).Int(ts).Bytes(val))
	db.m.SetTx(key, val, ts)
}

// Remove
func (db *DB) Remove(keys ...string) (n int) {
	db.encode(NewCodec(OpRemove).StrSlice(keys))
	for _, key := range keys {
		if db.m.Delete(key) {
			n++
		}
	}
	return
}

// Len
func (db *DB) Len() uint64 {
	return db.m.Stat().Len + uint64(db.cm.Count())
}

// GC triggers the garbage collection to evict expired kv datas.
func (db *DB) GC() {
	db.mu.Lock()
	db.m.Migrate()
	db.mu.Unlock()
}

// Scan
func (db *DB) Scan(f func(string, []byte, int64) bool) {
	db.m.Scan(func(key, value []byte, ttl int64) bool {
		return f(string(key), value, ttl)
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
	db.encode(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
	m.Set(field, val)

	return nil
}

// HRemove
func (db *DB) HRemove(key string, fields ...string) (n int, err error) {
	m, err := db.fetchMap(key)
	if err != nil {
		return 0, err
	}
	db.encode(NewCodec(OpHRemove).Str(key).StrSlice(fields))
	for _, k := range fields {
		if m.Delete(k) {
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
	db.encode(NewCodec(OpSAdd).Str(key).StrSlice(items))
	return s.Append(items...), nil
}

// SRemove
func (db *DB) SRemove(key string, items ...string) error {
	s, err := db.fetchSet(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpSRemove).Str(key).StrSlice(items))
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
	db.encode(NewCodec(OpSMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
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
	db.encode(NewCodec(OpSMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
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
	db.encode(NewCodec(OpSMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
	db.cm.Set(dst, s)

	return nil
}

// LLPush
func (db *DB) LLPush(key string, items ...string) error {
	ls, err := db.fetchList(key, true)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpLPush).Byte(listDirectionLeft).Str(key).StrSlice(items))
	ls.LPush(items...)

	return nil
}

// LRPush
func (db *DB) LRPush(key string, items ...string) error {
	ls, err := db.fetchList(key, true)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpLPush).Byte(listDirectionRight).Str(key).StrSlice(items))
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

// LLPop
func (db *DB) LLPop(key string) (string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.LPop()
	if !ok {
		return "", ErrEmptyList
	}
	db.encode(NewCodec(OpLPop).Byte(listDirectionLeft).Str(key))

	return res, nil
}

// LRPop
func (db *DB) LRPop(key string) (string, error) {
	ls, err := db.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.RPop()
	if !ok {
		return "", ErrEmptyList
	}
	db.encode(NewCodec(OpLPop).Byte(listDirectionRight).Str(key))

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
	db.encode(NewCodec(OpBitSet).Str(key).Bool(val).Uint32Slice(offsets))

	var n int
	if val {
		n = bm.Add(offsets...)
	} else {
		n = bm.Remove(offsets...)
	}

	return n, nil
}

// BitFlip
func (db *DB) BitFlip(key string, offset uint32) error {
	bm, err := db.fetchBitMap(key)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpBitFlip).Str(key).Uint(offset))
	bm.Flip(uint64(offset))

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
	db.encode(NewCodec(OpBitMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
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
	db.encode(NewCodec(OpBitMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
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
	db.encode(NewCodec(OpBitMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
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
	db.encode(NewCodec(OpZAdd).Str(zset).Str(key).Float(score))
	zs.Set(key, score)

	return nil
}

// ZIncr
func (db *DB) ZIncr(zset, key string, incr float64) (float64, error) {
	zs, err := db.fetchZSet(zset, true)
	if err != nil {
		return 0, err
	}
	db.encode(NewCodec(OpZIncr).Str(zset).Str(key).Float(incr))

	return zs.Incr(key, incr), nil
}

// ZRemove
func (db *DB) ZRemove(zset string, key string) error {
	zs, err := db.fetchZSet(zset)
	if err != nil {
		return err
	}
	db.encode(NewCodec(OpZRemove).Str(zset).Str(key))
	zs.Delete(key)

	return nil
}

// loadFromWal load data to mem from wal.
func (db *DB) loadFromWal() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// iter all wal.
	return db.wal.Range(func(data []byte) error {
		parser := codeman.NewParser(data)
		for !parser.Done() {
			// parse records.
			op := Operation(parser.ParseVarint())

			if parser.Error != nil {
				return parser.Error
			}

			if err := cmdTable[op].hook(db, parser); err != nil {
				return err
			}
		}
		return nil
	})
}

// rewrite write data to the fildb.
func (db *DB) shrink() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// create new segment file.
	if err := db.wal.OpenNewActiveSegment(); err != nil {
		return err
	}

	// marshal bytes.
	db.m.Scan(func(key, value []byte, ts int64) bool {
		cd := NewCodec(OpSetTx).Int(TypeString).Bytes(key).Int(ts).Bytes(value)
		db.wal.Write(cd.Content())
		cd.Recycle()
		return false
	})

	// marshal built-in types.
	var types Type
	for t := range db.cm.IterBuffered() {
		switch t.Val.(type) {
		case Map:
			types = TypeMap
		case BitMap:
			types = TypeBitmap
		case List:
			types = TypeList
		case Set:
			types = TypeSet
		case ZSet:
			types = TypeZSet
		}
		if cd, err := NewCodec(OpSetTx).Int(types).Str(t.Key).Int(0).Any(t.Val); err == nil {
			db.wal.Write(cd.Content())
			cd.Recycle()
		}
	}
	// sync
	if err := db.wal.Sync(); err != nil {
		return err
	}

	// remove all old segment files.
	return db.wal.RemoveOldSegments(db.wal.ActiveSegmentID())
}

// Shrink forced to shrink db file.
func (db *DB) Shrink() {
	db.shrinkTicker.Reset(db.ShrinkInterval)
	db.shrink()
}

// fetchMap
func (db *DB) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(db, key, func() Map {
		return structx.NewSyncMap()
	}, setnx...)
}

// fetchSet
func (db *DB) fetchSet(key string, setnx ...bool) (s Set, err error) {
	return fetch(db, key, func() Set {
		return structx.NewSet[string]()
	}, setnx...)
}

// fetchList
func (db *DB) fetchList(key string, setnx ...bool) (m List, err error) {
	return fetch(db, key, func() List {
		return structx.NewList[string]()
	}, setnx...)
}

// fetchBitMap
func (db *DB) fetchBitMap(key string, setnx ...bool) (bm BitMap, err error) {
	return fetch(db, key, func() BitMap {
		return structx.NewBitmap()
	}, setnx...)
}

// fetchZSet
func (db *DB) fetchZSet(key string, setnx ...bool) (z ZSet, err error) {
	return fetch(db, key, func() ZSet {
		return structx.NewZSet[string, float64]()
	}, setnx...)
}

// fetch
func fetch[T any](db *DB, key string, new func() T, setnx ...bool) (v T, err error) {
	// check from bytes cache.
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
