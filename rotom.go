// Package rotom provides an in-memory key-value database.
package rotom

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/codeman"
	"github.com/xgzlucario/rotom/structx"
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
	OpSUnion
	OpSInter
	OpSDiff
	// list
	OpLPush
	OpLPop
	OpRPush
	OpRPop
	// bitmap
	OpBitSet
	OpBitFlip
	OpBitOr
	OpBitAnd
	OpBitXor
	// zset
	OpZAdd
	OpZIncr
	OpZRemove
	// others
	OpMarshalBinary
)

// Cmd
type Cmd struct {
	op      Operation
	argsNum int
	hook    func(*Engine, []codeman.Result) error
}

// cmdTable defines the rNum and callback function required for the operation.
var cmdTable = []Cmd{
	{OpSetTx, 4, func(e *Engine, args []codeman.Result) error {
		// type, key, ts, val
		ts := args[2].ToInt64()
		if ts < cache.GetClock() && ts != noTTL {
			return nil
		}

		tp := args[0].ToInt64()
		switch tp {
		case TypeString:
			e.SetTx(args[1].ToStr(), args[3], ts)

		case TypeList:
			ls := structx.NewList[string]()
			if err := ls.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.cm.Set(args[1].ToStr(), ls)

		case TypeSet:
			s := structx.NewSet[string]()
			if err := s.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.cm.Set(args[1].ToStr(), s)

		case TypeMap:
			m := structx.NewSyncMap()
			if err := m.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.cm.Set(args[1].ToStr(), m)

		case TypeBitmap:
			m := structx.NewBitmap()
			if err := m.UnmarshalBinary(args[3]); err != nil {
				return err
			}
			e.cm.Set(args[1].ToStr(), m)

		case TypeZSet:
			m := structx.NewZSet[string, float64, []byte]()
			if err := m.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.cm.Set(args[1].ToStr(), m)

		default:
			return fmt.Errorf("%w: %d", base.ErrUnSupportDataType, tp)
		}

		return nil
	}},
	{OpRemove, 1, func(e *Engine, args []codeman.Result) error {
		// keys
		e.Remove(args[0].ToStrSlice()...)
		return nil
	}},
	// map
	{OpHSet, 3, func(e *Engine, args []codeman.Result) error {
		// key, field, val
		return e.HSet(args[0].ToStr(), args[1].ToStr(), args[2])
	}},
	{OpHRemove, 2, func(e *Engine, args []codeman.Result) error {
		// key, field
		_, err := e.HRemove(args[0].ToStr(), args[1].ToStrSlice()...)
		return err
	}},
	// set
	{OpSAdd, 2, func(e *Engine, args []codeman.Result) error {
		// key, items
		_, err := e.SAdd(args[0].ToStr(), args[1].ToStrSlice()...)
		return err
	}},
	{OpSRemove, 2, func(e *Engine, args []codeman.Result) error {
		// key, item
		return e.SRemove(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	{OpSUnion, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.SUnion(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	{OpSInter, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.SInter(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	{OpSDiff, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.SDiff(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	// list
	{OpLPush, 2, func(e *Engine, args []codeman.Result) error {
		// key, item
		return e.LPush(args[0].ToStr(), args[1].ToStr())
	}},
	{OpLPop, 1, func(e *Engine, args []codeman.Result) error {
		// key
		_, err := e.LPop(args[0].ToStr())
		return err
	}},
	{OpRPush, 2, func(e *Engine, args []codeman.Result) error {
		// key, item
		return e.RPush(args[0].ToStr(), args[1].ToStr())
	}},
	{OpRPop, 1, func(e *Engine, args []codeman.Result) error {
		// key
		_, err := e.RPop(args[0].ToStr())
		return err
	}},
	// bitmap
	{OpBitSet, 3, func(e *Engine, args []codeman.Result) error {
		// key, offset, val
		return e.BitSet(args[0].ToStr(), args[1].ToUint32(), args[2].ToBool())
	}},
	{OpBitFlip, 2, func(e *Engine, args []codeman.Result) error {
		// key, offset
		return e.BitFlip(args[0].ToStr(), args[1].ToUint32())
	}},
	{OpBitOr, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.BitOr(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	{OpBitAnd, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.BitAnd(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	{OpBitXor, 2, func(e *Engine, args []codeman.Result) error {
		// dstKey, srcKeys...
		return e.BitXor(args[0].ToStr(), args[1].ToStrSlice()...)
	}},
	// zset
	{OpZAdd, 4, func(e *Engine, args []codeman.Result) error {
		// key, field, score, val
		s, err := strconv.ParseFloat(args[2].ToStr(), 64)
		if err != nil {
			return err
		}
		return e.ZAdd(args[0].ToStr(), args[1].ToStr(), s, args[3])
	}},
	{OpZIncr, 3, func(e *Engine, args []codeman.Result) error {
		// key, field, score
		s, err := strconv.ParseFloat(args[2].ToStr(), 64)
		if err != nil {
			return err
		}
		_, err = e.ZIncr(args[0].ToStr(), args[1].ToStr(), s)
		return err
	}},
	{OpZRemove, 2, func(e *Engine, args []codeman.Result) error {
		// key, val
		return e.ZRemove(args[0].ToStr(), args[1].ToStr())
	}},
	// others
	{OpMarshalBinary, 1, func(e *Engine, args []codeman.Result) error {
		// val
		return e.m.UnmarshalBinary(args[0])
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

const (
	noTTL = 0

	KB = 1024
	MB = 1024 * KB
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

var (
	// Default config for db
	DefaultConfig = Config{
		Path:             "rotom.db",
		ShardCount:       1024,
		SyncPolicy:       base.EverySecond,
		ShrinkInterval:   time.Minute,
		RunSkipLoadError: true,
		Logger:           slog.Default(),
	}

	// No persistent config
	NoPersistentConfig = Config{
		ShardCount: 1024,
		SyncPolicy: base.Never,
		Logger:     slog.Default(),
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount int

	Path string // Path of db file.

	SyncPolicy     base.SyncPolicy // Data sync policy.
	ShrinkInterval time.Duration   // Shrink db file interval.

	RunSkipLoadError bool // Starts when loading db file error.

	Logger *slog.Logger // Logger for db, set <nil> if you don't want to use it.
}

// Engine represents a rotom engine for storage.
type Engine struct {
	sync.Mutex
	Config

	// context.
	ctx     context.Context
	cancel  context.CancelFunc
	tickers [2]*base.Ticker

	// if db loading encode not allowed.
	loading bool

	buf   *bytes.Buffer
	rwbuf *bytes.Buffer

	// data for bytes.
	m *cache.GigaCache

	// data for built-in structure.
	cm cmap.ConcurrentMap[string, any]
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
		buf:     bytes.NewBuffer(nil),
		rwbuf:   bytes.NewBuffer(nil),
		tickers: [2]*base.Ticker{},
		m:       cache.New(conf.ShardCount),
		cm:      cmap.New[any](),
	}

	// load db from disk.
	if err := e.load(); err != nil {
		e.logError("db load error: %v", err)
		return nil, err
	}
	e.loading = false

	if e.SyncPolicy == base.EverySecond {
		// sync buffer to disk.
		e.tickers[0] = base.NewTicker(ctx, time.Second, func() {
			e.Lock()
			_, err := e.writeTo(e.buf, e.Path)
			e.Unlock()
			if err != nil {
				e.logError("writeTo buffer error: %v", err)
			}
		})

		// shrink db.
		e.tickers[1] = base.NewTicker(ctx, e.ShrinkInterval, func() {
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
		return base.ErrDatabaseClosed
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
	if e.SyncPolicy == base.Never {
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

// Get
func (e *Engine) Get(key string) ([]byte, int64, error) {
	// check
	if e.cm.Has(key) {
		return nil, 0, base.ErrTypeAssert
	}
	// get
	val, ts, ok := e.m.Get(key)
	if !ok {
		return nil, 0, base.ErrKeyNotFound
	}
	return val, ts, nil
}

// Set store key-value pair.
func (e *Engine) Set(key string, val []byte) {
	e.SetTx(key, val, noTTL)
}

// SetEx store key-value pair with ttl.
func (e *Engine) SetEx(key string, val []byte, ttl time.Duration) {
	e.SetTx(key, val, cache.GetClock()+int64(ttl))
}

// SetTx store key-value pair with deadline.
func (e *Engine) SetTx(key string, val []byte, ts int64) {
	if ts < 0 {
		return
	}
	e.encode(NewCodec(OpSetTx).Int(TypeString).Str(key).Int(ts).Bytes(val))
	e.m.SetTx(key, val, ts)
}

// Remove
func (e *Engine) Remove(keys ...string) int {
	e.encode(NewCodec(OpRemove).StrSlice(keys))
	var sum int
	for _, key := range keys {
		if e.m.Delete(key) {
			sum++
		}
	}
	return sum
}

// Keys
func (e *Engine) Keys() []string {
	return append(e.m.Keys(), e.cm.Keys()...)
}

// Len
func (e *Engine) Len() uint64 {
	return e.m.Stat().Len + uint64(e.cm.Count())
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
		return nil, base.ErrFieldNotFound
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
func (e *Engine) SUnion(dstKey string, srcKeys ...string) error {
	srcSet, err := e.fetchSet(srcKeys[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range srcKeys[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Union(ts)
	}
	e.encode(NewCodec(OpSUnion).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, s)

	return nil
}

// SInter
func (e *Engine) SInter(dstKey string, srcKeys ...string) error {
	srcSet, err := e.fetchSet(srcKeys[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range srcKeys[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Intersect(ts)
	}
	e.encode(NewCodec(OpSInter).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, s)

	return nil
}

// SDiff
func (e *Engine) SDiff(dstKey string, srcKeys ...string) error {
	srcSet, err := e.fetchSet(srcKeys[0])
	if err != nil {
		return err
	}
	s := srcSet.Clone()

	for _, key := range srcKeys[1:] {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Difference(ts)
	}
	e.encode(NewCodec(OpSDiff).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, s)

	return nil
}

// LPush
func (e *Engine) LPush(key, item string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Str(key).Str(item))
	ls.LPush(item)

	return nil
}

// RPush
func (e *Engine) RPush(key, item string) error {
	ls, err := e.fetchList(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpRPush).Str(key).Str(item))
	ls.RPush(item)

	return nil
}

// LPop
func (e *Engine) LPop(key string) (string, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.LPop()
	if !ok {
		return "", base.ErrEmptyList
	}
	e.encode(NewCodec(OpLPop).Str(key))

	return res, nil
}

// RPop
func (e *Engine) RPop(key string) (string, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return "", err
	}
	res, ok := ls.RPop()
	if !ok {
		return "", base.ErrEmptyList
	}
	e.encode(NewCodec(OpRPop).Str(key))

	return res, nil
}

// LLen
func (e *Engine) LLen(key string) (int, error) {
	ls, err := e.fetchList(key)
	if err != nil {
		return 0, err
	}
	return ls.Len(), nil
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
func (e *Engine) BitSet(key string, offset uint32, val bool) error {
	bm, err := e.fetchBitMap(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitSet).Str(key).Uint(offset).Bool(val))

	if val {
		bm.Add(offset)
	} else {
		bm.Remove(offset)
	}

	return nil
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

// BitOr
func (e *Engine) BitOr(dstKey string, srcKeys ...string) error {
	bm := structx.NewBitmap()
	for _, key := range srcKeys {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Or(tbm)
	}
	e.encode(NewCodec(OpBitOr).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, bm)

	return nil
}

// BitXor
func (e *Engine) BitXor(dstKey string, srcKeys ...string) error {
	bm := structx.NewBitmap()
	for _, key := range srcKeys {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.Xor(tbm)
	}
	e.encode(NewCodec(OpBitXor).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, bm)

	return nil
}

// BitAnd
func (e *Engine) BitAnd(dstKey string, srcKeys ...string) error {
	bm := structx.NewBitmap()
	for _, key := range srcKeys {
		tbm, err := e.fetchBitMap(key)
		if err != nil {
			return err
		}
		bm.And(tbm)
	}
	e.encode(NewCodec(OpBitAnd).Str(dstKey).StrSlice(srcKeys))
	e.cm.Set(dstKey, bm)

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

	cbuf := codeman.Compress(buf.Bytes(), nil)
	coder := codeman.NewCodec().Bytes(cbuf)

	n, err := fs.Write(coder.Content())
	if err != nil {
		return 0, err
	}

	buf.Reset()
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

	blkDecoder := codeman.NewDecoder(line)
	for !blkDecoder.Done() {
		// parse a block data.
		blk, err := blkDecoder.Parse()
		if err != nil {
			return err
		}

		// decompress block.
		data, err := codeman.Decompress(blk, nil)
		if err != nil {
			return err
		}

		dataDecoder := codeman.NewDecoder(data)
		// parses data.
		for !dataDecoder.Done() {
			op, args, err := ParseRecord(dataDecoder)
			if err != nil {
				return err
			}
			if err := cmdTable[op].hook(e, args); err != nil {
				return err
			}
		}
	}

	e.logInfo("db load complete")

	return nil
}

// rewrite write data to the file.
func (e *Engine) shrink() {
	data, err := e.m.MarshalBinary()
	if err != nil {
		panic(err)
	}

	// Marshal bytes
	cd := NewCodec(OpMarshalBinary).Bytes(data)
	e.rwbuf.Write(cd.Content())
	cd.Recycle()

	// Marshal built-in structure
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
		return v, base.ErrWrongType
	}

	item, ok := e.cm.Get(key)
	if ok {
		v, ok := item.(T)
		if ok {
			return v, nil
		}
		return v, fmt.Errorf("%w: %T->%T", base.ErrWrongType, item, v)
	}

	v = new()
	if len(setnx) > 0 && setnx[0] {
		e.cm.Set(key, v)
	}
	return v, nil
}

// formatSize
func formatSize[T base.Integer](size T) string {
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
