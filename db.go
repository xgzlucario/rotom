// Package rotom provides an in-memory key-value database.
package rotom

import (
	"context"
	"fmt"
	"sync"
	"time"

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

	listDirectionLeft byte = iota + 1
	listDirectionRight
)

// Cmd
type Cmd struct {
	op   Operation
	hook func(*DB, *codeman.Parser) error
}

// cmdTable defines the rNum and callback function required for the operation.
var cmdTable = []Cmd{
	{OpSetTx, func(e *DB, decoder *codeman.Parser) error {
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
				e.SetTx(key, val, ts)
			}

		case TypeList:
			ls := structx.NewList[string]()
			if err := ls.UnmarshalJSON(val); err != nil {
				return err
			}
			e.cm.Set(key, ls)

		case TypeSet:
			s := structx.NewSet[string]()
			if err := s.UnmarshalJSON(val); err != nil {
				return err
			}
			e.cm.Set(key, s)

		case TypeMap:
			m := structx.NewSyncMap()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			e.cm.Set(key, m)

		case TypeBitmap:
			m := structx.NewBitmap()
			if err := m.UnmarshalBinary(val); err != nil {
				return err
			}
			e.cm.Set(key, m)

		case TypeZSet:
			m := structx.NewZSet[string, float64]()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			e.cm.Set(key, m)

		default:
			return fmt.Errorf("%w: %d", ErrUnSupportDataType, tp)
		}

		return nil
	}},

	{OpRemove, func(e *DB, decoder *codeman.Parser) error {
		// keys
		keys := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		e.Remove(keys...)
		return nil
	}},

	{OpHSet, func(e *DB, decoder *codeman.Parser) error {
		// key, field, val
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.HSet(key, field, val)
	}},

	{OpHRemove, func(e *DB, decoder *codeman.Parser) error {
		// key, fields
		key := decoder.Parse().Str()
		fields := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.HRemove(key, fields...)
		return err
	}},

	{OpSAdd, func(e *DB, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.SAdd(key, items...)
		return err
	}},

	{OpSRemove, func(e *DB, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.SRemove(key, items...)
	}},

	{OpSMerge, func(e *DB, decoder *codeman.Parser) error {
		// op, key, items
		op := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch op {
		case mergeTypeAnd:
			return e.SInter(key, items...)
		case mergeTypeOr:
			return e.SUnion(key, items...)
		case mergeTypeXOr:
			return e.SDiff(key, items...)
		}
		return ErrInvalidBitmapOperation
	}},

	{OpLPush, func(e *DB, decoder *codeman.Parser) error {
		// direct, key, items
		direct := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch direct {
		case listDirectionLeft:
			return e.LLPush(key, items...)
		case listDirectionRight:
			return e.LRPush(key, items...)
		}
		return ErrInvalidListDirect
	}},

	{OpLPop, func(e *DB, decoder *codeman.Parser) (err error) {
		// direct, key
		direct := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch direct {
		case listDirectionLeft:
			_, err = e.LLPop(key)
		case listDirectionRight:
			_, err = e.LRPop(key)
		default:
			err = ErrInvalidListDirect
		}
		return
	}},

	{OpBitSet, func(e *DB, decoder *codeman.Parser) error {
		// key, offset, val
		key := decoder.Parse().Str()
		val := decoder.ParseVarint().Bool()
		offsets := decoder.Parse().Uint32Slice()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.BitSet(key, val, offsets...)
		return err
	}},

	{OpBitFlip, func(e *DB, decoder *codeman.Parser) error {
		// key, offset
		key := decoder.Parse().Str()
		offset := decoder.ParseVarint().Uint32()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.BitFlip(key, offset)
	}},

	{OpBitMerge, func(e *DB, decoder *codeman.Parser) error {
		// op, key, items
		op := decoder.ParseVarint().Byte()
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		switch op {
		case mergeTypeAnd:
			return e.BitAnd(key, items...)
		case mergeTypeOr:
			return e.BitOr(key, items...)
		case mergeTypeXOr:
			return e.BitXor(key, items...)
		}
		return ErrInvalidBitmapOperation
	}},

	{OpZAdd, func(e *DB, decoder *codeman.Parser) error {
		// key, field, score
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		score := decoder.ParseVarint().Float64()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.ZAdd(key, field, score)
	}},

	{OpZIncr, func(e *DB, decoder *codeman.Parser) error {
		// key, field, score
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		score := decoder.ParseVarint().Float64()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.ZIncr(key, field, score)
		return err
	}},

	{OpZRemove, func(e *DB, decoder *codeman.Parser) error {
		// key, field
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.ZRemove(key, field)
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

// DB represents a rotom database engine.
type DB struct {
	// mu guards wal.
	mu sync.Mutex
	*Options

	// context.
	ctx    context.Context
	cancel context.CancelFunc

	// ticker.
	syncTicker   *time.Ticker
	shrinkTicker *time.Ticker

	// if db loading encode not allowed.
	loading bool

	// write ahead log.
	wal *wal.Log

	// data for bytes.
	m *cache.GigaCache

	// data for built-in structure.
	cm cmap.ConcurrentMap[string, any]
}

// Open opens a database by options.
func Open(options Options) (*DB, error) {
	// create wal.
	wal, err := wal.Open(options.DirPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{
		Options: &options,
		ctx:     ctx,
		cancel:  cancel,
		loading: true,
		wal:     wal,
		m:       cache.New(cache.DefaultOption),
		cm:      cmap.New[any](),
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

	db.logInfo("rotom is ready to go")

	return db, nil
}

// Close closes the db.
func (db *DB) Close() error {
	select {
	case <-db.ctx.Done():
		return ErrDatabaseClosed
	default:
		db.wal.Sync()
		db.cancel()
		return db.wal.Close()
	}
}

// encode
func (db *DB) encode(cd *codeman.Codec) {
	if db.SyncPolicy == Never {
		return
	}
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
func (e *DB) Get(key string) ([]byte, int64, error) {
	// check
	if e.cm.Has(key) {
		return nil, 0, ErrTypeAssert
	}
	// get
	val, ts, ok := e.m.Get(key)
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	return val, ts, nil
}

// Set store key-value pair.
func (e *DB) Set(key string, val []byte) {
	e.SetTx(key, val, noTTL)
}

// SetEx store key-value pair with ttl.
func (e *DB) SetEx(key string, val []byte, ttl time.Duration) {
	e.SetTx(key, val, cache.GetNanoSec()+int64(ttl))
}

// SetTx store key-value pair with deadline.
func (e *DB) SetTx(key string, val []byte, ts int64) {
	if ts < 0 {
		return
	}
	e.encode(NewCodec(OpSetTx).Int(TypeString).Str(key).Int(ts).Bytes(val))
	e.m.SetTx(key, val, ts)
}

// Remove
func (e *DB) Remove(keys ...string) (n int) {
	e.encode(NewCodec(OpRemove).StrSlice(keys))
	for _, key := range keys {
		if e.m.Delete(key) {
			n++
		}
	}
	return
}

// Len
func (e *DB) Len() uint64 {
	return e.m.Stat().Len + uint64(e.cm.Count())
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
func (e *DB) HGet(key, field string) ([]byte, error) {
	m, err := e.fetchMap(key)
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
func (e *DB) HLen(key string) (int, error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return 0, err
	}
	return m.Len(), nil
}

// HSet
func (e *DB) HSet(key, field string, val []byte) error {
	m, err := e.fetchMap(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
	m.Set(field, val)

	return nil
}

// HRemove
func (e *DB) HRemove(key string, fields ...string) (n int, err error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpHRemove).Str(key).StrSlice(fields))
	for _, k := range fields {
		if m.Delete(k) {
			n++
		}
	}
	return
}

// HKeys
func (e *DB) HKeys(key string) ([]string, error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return nil, err
	}
	return m.Keys(), nil
}

// SAdd
func (e *DB) SAdd(key string, items ...string) (int, error) {
	s, err := e.fetchSet(key, true)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpSAdd).Str(key).StrSlice(items))
	return s.Append(items...), nil
}

// SRemove
func (e *DB) SRemove(key string, items ...string) error {
	s, err := e.fetchSet(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSRemove).Str(key).StrSlice(items))
	s.RemoveAll(items...)
	return nil
}

// SHas returns whether the given items are all in the set.
func (e *DB) SHas(key string, items ...string) (bool, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return false, err
	}
	return s.Contains(items...), nil
}

// SCard
func (e *DB) SCard(key string) (int, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return 0, err
	}
	return s.Cardinality(), nil
}

// SMembers
func (e *DB) SMembers(key string) ([]string, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return nil, err
	}
	return s.ToSlice(), nil
}

// SUnion
func (e *DB) SUnion(dst string, src ...string) error {
	srcSet, err := e.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Union(ts)
	}
	e.encode(NewCodec(OpSMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
	e.cm.Set(dst, s)

	return nil
}

// SInter
func (e *DB) SInter(dst string, src ...string) error {
	srcSet, err := e.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Intersect(ts)
	}
	e.encode(NewCodec(OpSMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
	e.cm.Set(dst, s)

	return nil
}

// SDiff
func (e *DB) SDiff(dst string, src ...string) error {
	srcSet, err := e.fetchSet(src[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range src[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Difference(ts)
	}
	e.encode(NewCodec(OpSMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
	e.cm.Set(dst, s)

	return nil
}

// LLPush
func (e *DB) LLPush(key string, items ...string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Byte(listDirectionLeft).Str(key).StrSlice(items))
	ls.LPush(items...)

	return nil
}

// LRPush
func (e *DB) LRPush(key string, items ...string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Byte(listDirectionRight).Str(key).StrSlice(items))
	ls.RPush(items...)

	return nil
}

// LIndex
func (e *DB) LIndex(key string, i int) (string, error) {
	ls, err := e.fetchList(key)
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
func (e *DB) LLPop(key string) (string, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.LPop()
	if !ok {
		return "", ErrEmptyList
	}
	e.encode(NewCodec(OpLPop).Byte(listDirectionLeft).Str(key))

	return res, nil
}

// LRPop
func (e *DB) LRPop(key string) (string, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.RPop()
	if !ok {
		return "", ErrEmptyList
	}
	e.encode(NewCodec(OpLPop).Byte(listDirectionRight).Str(key))

	return res, nil
}

// LLen
func (e *DB) LLen(key string) (int, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return 0, err
	}
	return ls.Size(), nil
}

// BitTest
func (e *DB) BitTest(key string, offset uint32) (bool, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (e *DB) BitSet(key string, val bool, offsets ...uint32) (int, error) {
	bm, err := e.fetchBitMap(key, true)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpBitSet).Str(key).Bool(val).Uint32Slice(offsets))

	var n int
	if val {
		n = bm.Add(offsets...)
	} else {
		n = bm.Remove(offsets...)
	}

	return n, nil
}

// BitFlip
func (e *DB) BitFlip(key string, offset uint32) error {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitFlip).Str(key).Uint(offset))
	bm.Flip(uint64(offset))

	return nil
}

// BitAnd
func (e *DB) BitAnd(dst string, src ...string) error {
	bm, err := e.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.And(tbm)
	}
	e.encode(NewCodec(OpBitMerge).Byte(mergeTypeAnd).Str(dst).StrSlice(src))
	e.cm.Set(dst, bm)

	return nil
}

// BitOr
func (e *DB) BitOr(dst string, src ...string) error {
	bm, err := e.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Or(tbm)
	}
	e.encode(NewCodec(OpBitMerge).Byte(mergeTypeOr).Str(dst).StrSlice(src))
	e.cm.Set(dst, bm)

	return nil
}

// BitXor
func (e *DB) BitXor(dst string, src ...string) error {
	bm, err := e.fetchBitMap(src[0])
	if err != nil {
		return err
	}
	bm = bm.Clone()

	for _, key := range src[1:] {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Xor(tbm)
	}
	e.encode(NewCodec(OpBitMerge).Byte(mergeTypeXOr).Str(dst).StrSlice(src))
	e.cm.Set(dst, bm)

	return nil
}

// BitArray
func (e *DB) BitArray(key string) ([]uint32, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

// BitCount
func (e *DB) BitCount(key string) (uint64, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return 0, err
	}
	return bm.Len(), err
}

// ZGet
func (e *DB) ZGet(zset, key string) (float64, error) {
	zs, err := e.fetchZSet(zset)
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
func (e *DB) ZCard(zset string) (int, error) {
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return 0, err
	}
	return zs.Len(), nil
}

// ZIter
func (e *DB) ZIter(zset string, f func(string, float64) bool) error {
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return err
	}
	zs.Iter(func(k string, s float64) bool {
		return f(k, s)
	})
	return nil
}

// ZAdd
func (e *DB) ZAdd(zset, key string, score float64) error {
	zs, err := e.fetchZSet(zset, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpZAdd).Str(zset).Str(key).Float(score))
	zs.Set(key, score)

	return nil
}

// ZIncr
func (e *DB) ZIncr(zset, key string, incr float64) (float64, error) {
	zs, err := e.fetchZSet(zset, true)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpZIncr).Str(zset).Str(key).Float(incr))

	return zs.Incr(key, incr), nil
}

// ZRemove
func (e *DB) ZRemove(zset string, key string) error {
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpZRemove).Str(zset).Str(key))
	zs.Delete(key)

	return nil
}

// loadFromWal load data to mem from wal.
func (db *DB) loadFromWal() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.logInfo("start loading db from wal")

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

// rewrite write data to the file.
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

	db.logInfo("rotom shrink done")

	// remove all old segment files.
	return db.wal.RemoveOldSegments(db.wal.ActiveSegmentID())
}

// Shrink forced to shrink db file.
// Warning: will panic if SyncPolicy is never.
func (db *DB) Shrink() {
	if db.SyncPolicy == Never {
		panic("rotom: shrink is not allowed when SyncPolicy is never")
	}
	db.shrinkTicker.Reset(db.ShrinkInterval)
	db.shrink()
}

// fetchMap
func (e *DB) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(e, key, func() Map {
		return structx.NewSyncMap()
	}, setnx...)
}

// fetchSet
func (e *DB) fetchSet(key string, setnx ...bool) (s Set, err error) {
	return fetch(e, key, func() Set {
		return structx.NewSet[string]()
	}, setnx...)
}

// fetchList
func (e *DB) fetchList(key string, setnx ...bool) (m List, err error) {
	return fetch(e, key, func() List {
		return structx.NewList[string]()
	}, setnx...)
}

// fetchBitMap
func (e *DB) fetchBitMap(key string, setnx ...bool) (bm BitMap, err error) {
	return fetch(e, key, func() BitMap {
		return structx.NewBitmap()
	}, setnx...)
}

// fetchZSet
func (e *DB) fetchZSet(key string, setnx ...bool) (z ZSet, err error) {
	return fetch(e, key, func() ZSet {
		return structx.NewZSet[string, float64]()
	}, setnx...)
}

// fetch
func fetch[T any](e *DB, key string, new func() T, setnx ...bool) (v T, err error) {
	// check from bytes cache.
	if _, _, ok := e.m.Get(key); ok {
		return v, ErrWrongType
	}

	item, ok := e.cm.Get(key)
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, fmt.Errorf("%w: %T->%T", ErrWrongType, item, v)
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		e.cm.Set(key, v)
	}
	return v, nil
}

// logInfo
func (e *DB) logInfo(msg string, r ...any) {
	if e.Logger != nil {
		e.Logger.Info(fmt.Sprintf(msg, r...))
	}
}
