// Package rotom provides an in-memory key-value database.
package rotom

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/codeman"
	"github.com/xgzlucario/rotom/structx"
)

const (
	noTTL = 0

	KB = 1024
	MB = 1024 * KB
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

const (
	timeCarry = 1e9
)

// Cmd
type Cmd struct {
	op   Operation
	hook func(*Engine, *codeman.Parser) error
}

// cmdTable defines the rNum and callback function required for the operation.
var cmdTable = []Cmd{
	{OpSetTx, func(e *Engine, decoder *codeman.Parser) error {
		// type, key, ts, val
		tp := decoder.ParseVarint().Int64()
		key := decoder.Parse().Str()
		ts := decoder.ParseVarint().Int64() * timeCarry
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}

		// check ttl
		if ts < cache.GetNanoSec() && ts != noTTL {
			return nil
		}
		switch tp {
		case TypeString:
			e.SetTx(key, val, ts)

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
			m := structx.NewZSet[string, float64, []byte]()
			if err := m.UnmarshalJSON(val); err != nil {
				return err
			}
			e.cm.Set(key, m)

		default:
			return fmt.Errorf("%w: %d", ErrUnSupportDataType, tp)
		}

		return nil
	}},

	{OpRemove, func(e *Engine, decoder *codeman.Parser) error {
		// keys
		keys := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		e.Remove(keys...)
		return nil
	}},

	{OpHSet, func(e *Engine, decoder *codeman.Parser) error {
		// key, field, val
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.HSet(key, field, val)
	}},

	{OpHRemove, func(e *Engine, decoder *codeman.Parser) error {
		// key, fields
		key := decoder.Parse().Str()
		fields := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.HRemove(key, fields...)
		return err
	}},

	{OpSAdd, func(e *Engine, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		_, err := e.SAdd(key, items...)
		return err
	}},

	{OpSRemove, func(e *Engine, decoder *codeman.Parser) error {
		// key, items
		key := decoder.Parse().Str()
		items := decoder.Parse().StrSlice()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.SRemove(key, items...)
	}},

	{OpSMerge, func(e *Engine, decoder *codeman.Parser) error {
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

	{OpLPush, func(e *Engine, decoder *codeman.Parser) error {
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

	{OpLPop, func(e *Engine, decoder *codeman.Parser) (err error) {
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

	{OpBitSet, func(e *Engine, decoder *codeman.Parser) error {
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

	{OpBitFlip, func(e *Engine, decoder *codeman.Parser) error {
		// key, offset
		key := decoder.Parse().Str()
		offset := decoder.ParseVarint().Uint32()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.BitFlip(key, offset)
	}},

	{OpBitMerge, func(e *Engine, decoder *codeman.Parser) error {
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

	{OpZAdd, func(e *Engine, decoder *codeman.Parser) error {
		// key, field, score, val
		key := decoder.Parse().Str()
		field := decoder.Parse().Str()
		score := decoder.ParseVarint().Float64()
		val := decoder.Parse()

		if decoder.Error != nil {
			return decoder.Error
		}

		return e.ZAdd(key, field, score, val)
	}},

	{OpZIncr, func(e *Engine, decoder *codeman.Parser) error {
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

	{OpZRemove, func(e *Engine, decoder *codeman.Parser) error {
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
	ZSet   = *structx.ZSet[string, float64, []byte]
	BitMap = *structx.Bitmap
)

// Engine represents a rotom engine for storage.
type Engine struct {
	sync.Mutex
	Config

	// context.
	ctx     context.Context
	cancel  context.CancelFunc
	tickers [2]*Ticker

	// if db loading encode not allowed.
	loading bool

	buf   *bytes.Buffer
	rwbuf *bytes.Buffer

	// data for bytes.
	m *cache.GigaCache

	// data for built-in structure.
	cm cmap.ConcurrentMap[string, any]

	// crc.
	crc *crc32.Table
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf Config) (*Engine, error) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		Config:  conf,
		ctx:     ctx,
		cancel:  cancel,
		loading: true,
		buf:     bytes.NewBuffer(make([]byte, 0, 1024)),
		rwbuf:   bytes.NewBuffer(make([]byte, 0, 1024)),
		tickers: [2]*Ticker{},
		m:       cache.New(cache.DefaultOption),
		cm:      cmap.New[any](),
		crc:     crc32.MakeTable(crc32.Castagnoli),
	}

	// load db from disk.
	if err := e.load(); err != nil {
		e.logError("db load error: %v", err)
		return nil, err
	}
	e.loading = false

	if e.SyncPolicy == EverySecond {
		// sync buffer to disk.
		e.tickers[0] = NewTicker(ctx, time.Second, func() {
			e.Lock()
			_, err := e.writeTo(e.buf, e.Path)
			e.Unlock()
			if err != nil {
				e.logError("writeTo buffer error: %v", err)
			}
		})

		// shrink db.
		e.tickers[1] = NewTicker(ctx, e.ShrinkInterval, func() {
			e.Lock()
			e.shrink()
			e.Unlock()
		})
	}

	e.logInfo("rotom is ready to go")

	return e, nil
}

// Close closes the db engine.
func (e *Engine) Close() error {
	select {
	case <-e.ctx.Done():
		return ErrDatabaseClosed
	default:
		e.Lock()
		_, err := e.writeTo(e.buf, e.Path)
		e.Unlock()
		e.cancel()
		return err
	}
}

// encode
func (e *Engine) encode(cd *codeman.Codec) {
	if e.SyncPolicy == Never {
		return
	}
	if e.loading {
		return
	}
	e.Lock()
	e.buf.Write(cd.Content())
	e.Unlock()
	cd.Recycle()
}

// NewCodec
func NewCodec(op Operation) *codeman.Codec {
	return codeman.NewCodec().Byte(byte(op))
}

// Get
func (e *Engine) Get(key string) ([]byte, int64, error) {
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
func (e *Engine) Set(key string, val []byte) {
	e.SetTx(key, val, noTTL)
}

// SetEx store key-value pair with ttl.
func (e *Engine) SetEx(key string, val []byte, ttl time.Duration) {
	e.SetTx(key, val, cache.GetNanoSec()+int64(ttl))
}

// SetTx store key-value pair with deadline.
func (e *Engine) SetTx(key string, val []byte, ts int64) {
	if ts < 0 {
		return
	}
	e.encode(NewCodec(OpSetTx).Int(TypeString).Str(key).Int(ts / timeCarry).Bytes(val))
	e.m.SetTx(key, val, ts)
}

// Remove
func (e *Engine) Remove(keys ...string) (n int) {
	e.encode(NewCodec(OpRemove).StrSlice(keys))
	for _, key := range keys {
		if e.m.Delete(key) {
			n++
		}
	}
	return
}

// Len
func (e *Engine) Len() uint64 {
	return e.m.Stat().Len + uint64(e.cm.Count())
}

// GC triggers the garbage collection to evict expired kv datas.
func (e *Engine) GC() {
	e.Lock()
	e.m.Migrate()
	e.Unlock()
}

// Scan
func (e *Engine) Scan(f func(string, []byte, int64) bool) {
	e.m.Scan(func(key, value []byte, ttl int64) bool {
		return f(string(key), value, ttl)
	})
}

// HGet
func (e *Engine) HGet(key, field string) ([]byte, error) {
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
func (e *Engine) HLen(key string) (int, error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return 0, err
	}
	return m.Len(), nil
}

// HSet
func (e *Engine) HSet(key, field string, val []byte) error {
	m, err := e.fetchMap(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
	m.Set(field, val)

	return nil
}

// HRemove
func (e *Engine) HRemove(key string, fields ...string) (n int, err error) {
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
func (e *Engine) HKeys(key string) ([]string, error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return nil, err
	}
	return m.Keys(), nil
}

// SAdd
func (e *Engine) SAdd(key string, items ...string) (int, error) {
	s, err := e.fetchSet(key, true)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpSAdd).Str(key).StrSlice(items))
	return s.Append(items...), nil
}

// SRemove
func (e *Engine) SRemove(key string, items ...string) error {
	s, err := e.fetchSet(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSRemove).Str(key).StrSlice(items))
	s.RemoveAll(items...)
	return nil
}

// SHas returns whether the given items are all in the set.
func (e *Engine) SHas(key string, items ...string) (bool, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return false, err
	}
	return s.Contains(items...), nil
}

// SCard
func (e *Engine) SCard(key string) (int, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return 0, err
	}
	return s.Cardinality(), nil
}

// SMembers
func (e *Engine) SMembers(key string) ([]string, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return nil, err
	}
	return s.ToSlice(), nil
}

// SUnion
func (e *Engine) SUnion(dst string, src ...string) error {
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
func (e *Engine) SInter(dst string, src ...string) error {
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
func (e *Engine) SDiff(dst string, src ...string) error {
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
func (e *Engine) LLPush(key string, items ...string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Byte(listDirectionLeft).Str(key).StrSlice(items))
	ls.LPush(items...)

	return nil
}

// LRPush
func (e *Engine) LRPush(key string, items ...string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Byte(listDirectionRight).Str(key).StrSlice(items))
	ls.RPush(items...)

	return nil
}

// LIndex
func (e *Engine) LIndex(key string, i int) (string, error) {
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
func (e *Engine) LLPop(key string) (string, error) {
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
func (e *Engine) LRPop(key string) (string, error) {
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
func (e *Engine) LLen(key string) (int, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return 0, err
	}
	return ls.Size(), nil
}

// BitTest
func (e *Engine) BitTest(key string, offset uint32) (bool, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return false, err
	}
	return bm.Test(offset), nil
}

// BitSet
func (e *Engine) BitSet(key string, val bool, offsets ...uint32) (int, error) {
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
func (e *Engine) BitFlip(key string, offset uint32) error {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitFlip).Str(key).Uint(offset))
	bm.Flip(uint64(offset))

	return nil
}

// BitAnd
func (e *Engine) BitAnd(dst string, src ...string) error {
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
func (e *Engine) BitOr(dst string, src ...string) error {
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
func (e *Engine) BitXor(dst string, src ...string) error {
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
func (e *Engine) BitArray(key string) ([]uint32, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

// BitCount
func (e *Engine) BitCount(key string) (uint64, error) {
	bm, err := e.fetchBitMap(key)
	if err != nil {
		return 0, err
	}
	return bm.Len(), err
}

// ZGet
func (e *Engine) ZGet(zset, key string) ([]byte, float64, error) {
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return nil, 0, err
	}
	val, score, ok := zs.Get(key)
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	return val, score, nil
}

// ZAdd
func (e *Engine) ZAdd(zset, key string, score float64, val []byte) error {
	zs, err := e.fetchZSet(zset, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpZAdd).Str(zset).Str(key).Float(score).Bytes(val))
	zs.SetWithScore(key, score, val)

	return nil
}

// ZIncr
func (e *Engine) ZIncr(zset, key string, incr float64) (float64, error) {
	zs, err := e.fetchZSet(zset, true)
	if err != nil {
		return 0, err
	}
	e.encode(NewCodec(OpZIncr).Str(zset).Str(key).Float(incr))

	return zs.Incr(key, incr), nil
}

// ZRemove
func (e *Engine) ZRemove(zset, key string) error {
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpZRemove).Str(zset).Str(key))
	zs.Delete(key)

	return nil
}

// writeTo writes the buffer into the file at the specified path.
func (s *Engine) writeTo(buf *bytes.Buffer, path string) (int, error) {
	if buf.Len() == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fs.Close()

	// encode block.
	data := codeman.Compress(buf.Bytes(), nil)
	crc := crc32.Checksum(data, s.crc)
	coder := codeman.NewCodec().Bytes(data).Uint(crc)

	n, err := fs.Write(coder.Content())
	if err != nil {
		return 0, err
	}

	// reset
	buf.Reset()
	coder.Recycle()

	return n, nil
}

// load reads the persisted data from the shard file and loads it into memory.
func (e *Engine) load() error {
	line, err := os.ReadFile(e.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	e.logInfo("loading db file size %s", formatSize(len(line)))

	blkParser := codeman.NewParser(line)
	for !blkParser.Done() {
		// parse data block.
		dataBlock := blkParser.Parse()
		crc := uint32(blkParser.ParseVarint())

		if blkParser.Error != nil {
			return blkParser.Error
		}
		if crc != crc32.Checksum(dataBlock, e.crc) {
			return ErrCheckSum
		}

		data, err := codeman.Decompress(dataBlock, nil)
		if err != nil {
			return err
		}

		recParser := codeman.NewParser(data)
		for !recParser.Done() {
			// parse records.
			op := Operation(recParser.ParseVarint())

			if recParser.Error != nil {
				return recParser.Error
			}

			if err := cmdTable[op].hook(e, recParser); err != nil {
				return err
			}
		}
	}

	e.logInfo("db load complete")

	return nil
}

// rewrite write data to the file.
func (e *Engine) shrink() {
	// Marshal String
	e.m.Scan(func(key, value []byte, ttl int64) bool {
		cd := NewCodec(OpSetTx).Int(TypeString).Bytes(key).Int(ttl / timeCarry).Bytes(value)
		e.rwbuf.Write(cd.Content())
		cd.Recycle()
		return false
	})

	// Marshal built-in type
	var _type Type
	for t := range e.cm.IterBuffered() {
		switch t.Val.(type) {
		case Map:
			_type = TypeMap
		case BitMap:
			_type = TypeBitmap
		case List:
			_type = TypeList
		case Set:
			_type = TypeSet
		case ZSet:
			_type = TypeZSet
		}
		// SetTx
		if cd, err := NewCodec(OpSetTx).Int(_type).Str(t.Key).Int(0).Any(t.Val); err == nil {
			e.rwbuf.Write(cd.Content())
			cd.Recycle()
		}
	}

	// Flush buffer to file
	tmpPath := fmt.Sprintf("%v.tmp", time.Now())
	e.writeTo(e.rwbuf, tmpPath)
	e.writeTo(e.buf, tmpPath)

	os.Rename(tmpPath, e.Path)

	e.logInfo("rotom rewrite done")
}

// Shrink forced to shrink db file.
// Warning: will panic if SyncPolicy is never.
func (e *Engine) Shrink() error {
	return e.tickers[1].Do()
}

// fetchMap
func (e *Engine) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(e, key, func() Map {
		return structx.NewSyncMap()
	}, setnx...)
}

// fetchSet
func (e *Engine) fetchSet(key string, setnx ...bool) (s Set, err error) {
	return fetch(e, key, func() Set {
		return structx.NewSet[string]()
	}, setnx...)
}

// fetchList
func (e *Engine) fetchList(key string, setnx ...bool) (m List, err error) {
	return fetch(e, key, func() List {
		return structx.NewList[string]()
	}, setnx...)
}

// fetchBitMap
func (e *Engine) fetchBitMap(key string, setnx ...bool) (bm BitMap, err error) {
	return fetch(e, key, func() BitMap {
		return structx.NewBitmap()
	}, setnx...)
}

// fetchZSet
func (e *Engine) fetchZSet(key string, setnx ...bool) (z ZSet, err error) {
	return fetch(e, key, func() ZSet {
		return structx.NewZSet[string, float64, []byte]()
	}, setnx...)
}

// fetch
func fetch[T any](e *Engine, key string, new func() T, setnx ...bool) (v T, err error) {
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

// formatSize
func formatSize[T codeman.Integer](size T) string {
	switch {
	case size < KB:
		return fmt.Sprintf("%dB", size)
	case size < MB:
		return fmt.Sprintf("%.1fKB", float64(size)/KB)
	default:
		return fmt.Sprintf("%.1fMB", float64(size)/MB)
	}
}

// logInfo
func (e *Engine) logInfo(msg string, r ...any) {
	if e.Logger != nil {
		e.Logger.Info(fmt.Sprintf(msg, r...))
	}
}

// logError
func (e *Engine) logError(msg string, r ...any) {
	if e.Logger != nil {
		e.Logger.Error(fmt.Sprintf(msg, r...))
	}
}
