// Package rotom provides an in-memory key-value database.
package rotom

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/panjf2000/gnet/v2"
	cache "github.com/xgzlucario/GigaCache"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// Operations.
type Operation byte

const (
	Response Operation = iota
	OpSetTx
	OpGet
	OpRemove
	OpRename
	OpLen
	// map
	OpHSet
	OpHGet
	OpHLen
	OpHKeys
	OpHRemove
	// set
	OpSAdd
	OpSRemove
	OpSHas
	OpSCard
	OpSMembers
	OpSUnion
	OpSInter
	OpSDiff
	// list
	OpLPush
	OpLPop
	OpRPush
	OpRPop
	OpLLen
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
	OpMarshalBytes
	OpPing
)

// Cmd
type Cmd struct {
	op      Operation
	argsNum byte
	hook    func(*Engine, [][]byte, base.Writer) error
}

// cmdTable defines the argsNum and callback function required for the operation.
var cmdTable = []Cmd{
	{Response, 2, nil},
	{OpSetTx, 4, func(e *Engine, args [][]byte, _ base.Writer) error {
		// type, key, ts, val
		ts := base.ParseInt[int64](args[2]) * timeCarry
		if ts < cache.GetClock() && ts != noTTL {
			return nil
		}

		vType := VType(args[0][0])

		switch vType {
		case TypeString:
			e.m.SetTx(string(args[1]), args[3], ts)

		case TypeList:
			ls := structx.NewList[string]()
			if err := ls.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.m.Set(string(args[1]), ls)

		case TypeSet:
			s := structx.NewSet[string]()
			if err := s.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.m.Set(string(args[1]), s)

		case TypeMap:
			m := structx.NewSyncMap[string, []byte]()
			if err := m.UnmarshalJSON(args[3]); err != nil {
				return err
			}
			e.m.Set(string(args[1]), m)

		case TypeBitmap:
			m := structx.NewBitmap()
			if err := m.UnmarshalBinary(args[3]); err != nil {
				return err
			}
			e.m.Set(string(args[1]), m)

		default:
			return fmt.Errorf("%v: %d", base.ErrUnSupportDataType, vType)
		}

		return nil
	}},
	{OpGet, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		val, _, err := e.GetBytes(string(args[0]))
		if err != nil {
			return err
		}
		_, err = w.Write(val)
		return err
	}},
	{OpRemove, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		ok := e.Remove(string(args[0]))
		return w.WriteByte(bool2byte(ok))
	}},
	{OpRename, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// old, new
		ok := e.Rename(string(args[0]), string(args[1]))
		return w.WriteByte(bool2byte(ok))
	}},
	{OpLen, 0, func(e *Engine, args [][]byte, w base.Writer) error {
		res := base.FormatInt[uint64](e.Stat().Len)
		_, err := w.Write(res)
		return err
	}},
	// map
	{OpHSet, 3, func(e *Engine, args [][]byte, _ base.Writer) error {
		// key, field, val
		return e.HSet(string(args[0]), string(args[1]), args[2])
	}},
	{OpHGet, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, field
		m, err := e.fetchMap(string(args[0]))
		if err != nil {
			return err
		}
		val, ok := m.Get(string(args[1]))
		if !ok {
			return base.ErrFieldNotFound
		}
		_, err = w.Write(val)
		return err
	}},
	{OpHLen, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		m, err := e.fetchMap(string(args[0]))
		if err != nil {
			return err
		}
		res := base.FormatInt[int](m.Len())
		_, err = w.Write(res)
		return err
	}},
	{OpHKeys, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		m, err := e.fetchMap(string(args[0]))
		if err != nil {
			return err
		}
		src, err := sonic.Marshal(m.Keys())
		if err != nil {
			return err
		}
		_, err = w.Write(src)
		return err
	}},
	{OpHRemove, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, field
		ok, err := e.HRemove(string(args[0]), string(args[1]))
		if err != nil {
			return err
		}
		return w.WriteByte(bool2byte(ok))
	}},
	// set
	{OpSAdd, 2, func(e *Engine, args [][]byte, _ base.Writer) error {
		// key, item
		return e.SAdd(string(args[0]), string(args[1]))
	}},
	{OpSRemove, 2, func(e *Engine, args [][]byte, _ base.Writer) error {
		// key, item
		return e.SRemove(string(args[0]), string(args[1]))
	}},
	{OpSHas, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, item
		ok, err := e.SHas(string(args[0]), string(args[1]))
		if err != nil {
			return err
		}
		return w.WriteByte(bool2byte(ok))
	}},
	{OpSCard, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		n, err := e.SCard(string(args[0]))
		if err != nil {
			return err
		}
		_, err = w.Write(base.FormatInt(n))
		return err
	}},
	{OpSMembers, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		s, err := e.fetchSet(string(args[0]))
		if err != nil {
			return err
		}
		_, err = w.Write(base.FormatStrSlice(s.ToSlice()))
		return err
	}},
	{OpSUnion, 2, func(e *Engine, args [][]byte, _ base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.SUnion(string(args[0]), srcKeys...)
	}},
	{OpSInter, 2, func(e *Engine, args [][]byte, _ base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.SInter(string(args[0]), srcKeys...)
	}},
	{OpSDiff, 2, func(e *Engine, args [][]byte, _ base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.SDiff(string(args[0]), srcKeys...)
	}},
	// list
	{OpLPush, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, item
		return e.LPush(string(args[0]), string(args[1]))
	}},
	{OpLPop, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		_, err := e.LPop(string(args[0]))
		return err
	}},
	{OpRPush, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, item
		return e.RPush(string(args[0]), string(args[1]))
	}},
	{OpRPop, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		_, err := e.RPop(string(args[0]))
		return err
	}},
	{OpLLen, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// key
		l, err := e.fetchList(string(args[0]))
		if err != nil {
			return err
		}
		str := base.FormatInt(l.Len())
		_, err = w.Write(str)
		return err
	}},
	// bitmap
	{OpBitSet, 3, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, offset, val
		_, err := e.BitSet(string(args[0]), base.ParseInt[uint32](args[1]), args[2][0] == _true)
		return err
	}},
	{OpBitFlip, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, offset
		return e.BitFlip(string(args[0]), base.ParseInt[uint32](args[1]))
	}},
	{OpBitOr, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.BitOr(string(args[0]), srcKeys...)
	}},
	{OpBitAnd, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.BitAnd(string(args[0]), srcKeys...)
	}},
	{OpBitXor, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// dstKey, srcKeys...
		srcKeys := base.ParseStrSlice(args[1])
		return e.BitXor(string(args[0]), srcKeys...)
	}},
	// zset
	{OpZAdd, 4, func(e *Engine, args [][]byte, _ base.Writer) error {
		// key, score, val
		s, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return err
		}
		return e.ZAdd(string(args[0]), string(args[1]), s, args[3])
	}},
	{OpZIncr, 3, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, score, val
		s, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil {
			return err
		}
		_, err = e.ZIncr(string(args[0]), string(args[1]), s)
		return err
	}},
	{OpZRemove, 2, func(e *Engine, args [][]byte, w base.Writer) error {
		// key, val
		return e.ZRemove(string(args[0]), string(args[1]))
	}},
	// others
	{OpMarshalBytes, 1, func(e *Engine, args [][]byte, w base.Writer) error {
		// val
		return e.m.UnmarshalBytes(args[0])
	}},
	{OpPing, 0, func(_ *Engine, _ [][]byte, w base.Writer) error {
		_, err := w.Write([]byte("pong"))
		return err
	}},
}

// VType is value type for Set Operation.
type VType byte

const (
	TypeString VType = iota + 1
	TypeMap
	TypeSet
	TypeList
	TypeZSet
	TypeBitmap
)

const (
	sepChar   = byte(255)
	timeCarry = 1e6 // millisecs
	noTTL     = 0

	KB = 1024
	MB = 1024 * KB
)

// Type aliases for structx types.
type (
	String = []byte
	Map    = *structx.SyncMap[string, []byte]
	Set    = *structx.Set[string]
	List   = *structx.List[string]
	ZSet   = *structx.ZSet[string, float64, []byte]
	BitMap = *structx.Bitmap
)

var (
	// Default config for db
	DefaultConfig = &Config{
		Path:             "rotom.db",
		ShardCount:       1024,
		SyncPolicy:       base.EveryInterval,
		SyncInterval:     time.Second,
		ShrinkInterval:   time.Minute,
		RunSkipLoadError: true,
		Logger:           slog.Default(),
	}

	// No persistent config
	NoPersistentConfig = &Config{
		ShardCount: 1024,
		SyncPolicy: base.Never,
		Logger:     slog.Default(),
	}
)

// Config represents the configuration for a Store.
type Config struct {
	ShardCount int

	Path string // Path of db file.

	SyncPolicy base.SyncPolicy // Data sync policy.

	SyncInterval   time.Duration // Sync to disk interval.
	ShrinkInterval time.Duration // Shrink db file interval.

	RunSkipLoadError bool // Starts when loading db file error.

	Logger *slog.Logger // Logger for db, set <nil> if you don't want to use it.
}

// Engine represents a rotom engine for storage.
type Engine struct {
	sync.Mutex
	*Config

	// context.
	ctx     context.Context
	cancel  context.CancelFunc
	tickers [3]*base.Ticker

	// if db loading encode() not allowed.
	loading bool

	buf   *bytes.Buffer
	rwbuf *bytes.Buffer
	m     *cache.GigaCache
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf *Config) (*Engine, error) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		Config:  conf,
		ctx:     ctx,
		cancel:  cancel,
		loading: true,
		buf:     bytes.NewBuffer(nil),
		rwbuf:   bytes.NewBuffer(nil),
		m:       cache.New(conf.ShardCount),
		tickers: [3]*base.Ticker{},
	}

	// load db from disk.
	if err := e.load(); err != nil {
		e.logError("db load error: %v", err)
		return nil, err
	}
	e.loading = false

	// runtime monitor.
	if e.Logger != nil {
		e.tickers[0] = base.NewTicker(ctx, time.Minute, func() {
			e.printRuntimeStats()
		})
	}

	if e.SyncPolicy == base.EveryInterval {
		// sync buffer to disk.
		e.tickers[1] = base.NewTicker(ctx, e.SyncInterval, func() {
			e.Lock()
			n, err := e.writeTo(e.buf, e.Path)
			e.Unlock()
			if err != nil {
				e.logError("writeTo buffer error: %v", err)
			} else if n > 0 {
				e.logInfo("write %s buffer to db file", formatSize(n))
			}
		})

		// shrink db.
		e.tickers[2] = base.NewTicker(ctx, e.ShrinkInterval, func() {
			e.Lock()
			e.shrink()
			e.Unlock()
		})
	}

	e.logInfo("rotom is ready to go")

	return e, nil
}

// Listen bind and listen to the specified tcp address.
func (e *Engine) Listen(addr string) error {
	e.logInfo("listening on %s...", addr)
	return gnet.Run(&RotomEngine{db: e}, addr, gnet.WithMulticore(true))
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
func (e *Engine) encode(cd *Codec) {
	if e.SyncPolicy == base.Never {
		return
	}
	if e.loading {
		return
	}
	e.Lock()
	e.buf.Write(cd.B)
	e.Unlock()
	cd.Recycle()
}

// Get
func (e *Engine) Get(key string) (any, int64, bool) {
	return e.m.Get(key)
}

// GetBytes
func (e *Engine) GetBytes(key string) ([]byte, int64, error) {
	r, t, ok := e.m.Get(key)
	if ok {
		if r, ok := r.([]byte); ok {
			return r, t, nil
		}
		return nil, 0, base.ErrTypeAssert
	}
	return nil, 0, base.ErrKeyNotFound
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
	e.encode(NewCodec(OpSetTx).Type(TypeString).Str(key).Int(ts / timeCarry).Bytes(val))
	e.m.SetTx(key, val, ts)
}

// Incr
func (e *Engine) Incr(key string, incr float64) (res float64, err error) {
	bytes, ts, err := e.GetBytes(key)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(*b2s(bytes), 64)
	if err != nil {
		return 0, err
	}
	res = f + incr
	fstr := strconv.FormatFloat(res, 'f', -1, 64)

	e.encode(NewCodec(OpSetTx).Type(TypeString).Str(key).Int(ts / timeCarry).Str(fstr))
	e.m.SetTx(key, s2b(&fstr), ts)

	return res, nil
}

// Remove
func (e *Engine) Remove(key string) bool {
	e.encode(NewCodec(OpRemove).Str(key))
	return e.m.Delete(key)
}

// Rename
func (e *Engine) Rename(old, new string) bool {
	e.encode(NewCodec(OpRename).Str(old).Str(new))
	return e.m.Rename(old, new)
}

// Keys
func (e *Engine) Keys() []string {
	return e.m.Keys()
}

// Scan
func (e *Engine) Scan(f func(string, any, int64) bool) {
	e.m.Scan(f)
}

// Stat
func (e *Engine) Stat() cache.CacheStat {
	return e.m.Stat()
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
func (e *Engine) HRemove(key, field string) (bool, error) {
	m, err := e.fetchMap(key)
	if err != nil {
		return false, err
	}
	e.encode(NewCodec(OpHRemove).Str(key).Str(field))

	return m.Delete(field), nil
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
func (e *Engine) SAdd(key string, item string) error {
	s, err := e.fetchSet(key, true)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSAdd).Str(key).Str(item))
	s.Add(item)

	return nil
}

// SRemove
func (e *Engine) SRemove(key string, item string) error {
	s, err := e.fetchSet(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSRemove).Str(key).Str(item))
	s.Remove(item)

	return nil
}

// SHas
func (e *Engine) SHas(key string, item string) (bool, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return false, err
	}
	return s.Contains(item), nil
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
	s := structx.NewSet[string]()
	for _, key := range srcKeys {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Union(ts)
	}
	e.encode(NewCodec(OpSUnion).Str(dstKey).StrSlice(srcKeys))
	e.m.Set(dstKey, s)

	return nil
}

// SInter
func (e *Engine) SInter(dstKey string, srcKeys ...string) error {
	s := structx.NewSet[string]()
	for _, key := range srcKeys {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Intersect(ts)
	}
	e.encode(NewCodec(OpSInter).Str(dstKey).StrSlice(srcKeys))
	e.m.Set(dstKey, s)

	return nil
}

// SDiff
func (e *Engine) SDiff(dstKey string, srcKeys ...string) error {
	s := structx.NewSet[string]()
	for _, key := range srcKeys {
		ts, err := e.fetchSet(key)
		if err != nil {
			return err
		}
		s.Difference(ts)
	}
	e.encode(NewCodec(OpSDiff).Str(dstKey).StrSlice(srcKeys))
	e.m.Set(dstKey, s)

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
func (e *Engine) BitSet(key string, offset uint32, val bool) (bool, error) {
	bm, err := e.fetchBitMap(key, true)
	if err != nil {
		return false, err
	}
	e.encode(NewCodec(OpBitSet).Str(key).Uint(offset).Bool(val))

	if val {
		return bm.Add(offset), nil
	}
	return bm.Remove(offset), nil
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
	e.m.Set(dstKey, bm)

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
	e.m.Set(dstKey, bm)

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
	e.m.Set(dstKey, bm)

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
func (s *Engine) writeTo(buf *bytes.Buffer, path string) (int64, error) {
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
func (e *Engine) load() error {
	line, err := os.ReadFile(e.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	e.logInfo("loading db file size %s", formatSize(len(line)))

	decoder := NewDecoder(line)
	for !decoder.Done() {
		op, args, err := decoder.ParseRecord()
		if err != nil {
			return err
		}
		if err := cmdTable[op].hook(e, args, nil); err != nil {
			return err
		}
	}
	e.logInfo("db load complete")

	return nil
}

// rewrite write data to the file.
func (e *Engine) shrink() {
	if e.SyncPolicy == base.Never {
		return
	}

	var rec VType
	// Marshal any
	data, err := e.m.MarshalBytesFunc(func(key string, v any, i int64) {
		switch v.(type) {
		case Map:
			rec = TypeString
		case BitMap:
			rec = TypeBitmap
		case List:
			rec = TypeList
		case Set:
			rec = TypeSet
		default:
			panic(base.ErrUnSupportDataType)
		}

		// SetTx
		if cd, err := NewCodec(OpSetTx).Type(rec).Str(key).Int(i / timeCarry).Any(v); err == nil {
			e.rwbuf.Write(cd.B)
			cd.Recycle()
		}
	})
	if err != nil {
		panic(err)
	}

	// Marshal bytes
	cd := NewCodec(OpMarshalBytes).Bytes(data)
	e.rwbuf.Write(cd.B)
	cd.Recycle()

	// Flush buffer to file
	tmpPath := fmt.Sprintf("%v.tmp", time.Now())
	e.writeTo(e.rwbuf, tmpPath)
	e.writeTo(e.buf, tmpPath)

	os.Rename(tmpPath, e.Path)

	e.logInfo("rotom rewrite done")
}

// Shrink forced to shrink db file.
func (e *Engine) Shrink() error {
	if e.tickers[2] == nil {
		return base.ErrUnSupportOperation
	}
	return e.tickers[2].ForceFunc()
}

// fetchMap
func (e *Engine) fetchMap(key string, setnx ...bool) (m Map, err error) {
	return fetch(e, key, func() Map {
		return structx.NewSyncMap[string, []byte]()
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
func fetch[T any](e *Engine, key string, new func() T, setnx ...bool) (T, error) {
	m, _, ok := e.m.Get(key)
	if ok {
		m, ok := m.(T)
		if ok {
			return m, nil
		}
		var v T
		return v, base.ErrWrongType
	}
	vptr := new()
	if len(setnx) > 0 && setnx[0] {
		e.m.Set(key, vptr)
	}

	return vptr, nil
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

// printRuntimeStats
func (e *Engine) printRuntimeStats() {
	var stats debug.GCStats
	var memStats runtime.MemStats

	debug.ReadGCStats(&stats)
	runtime.ReadMemStats(&memStats)

	e.Logger.
		With("alloc", formatSize(memStats.Alloc)).
		With("sys", formatSize(memStats.Sys)).
		With("gctime", stats.NumGC).
		With("heapObjects", memStats.HeapObjects).
		With("gcpause", stats.PauseTotal/time.Duration(stats.NumGC)).
		Info("[Runtime]")
}

// logInfo
func (e *Engine) logInfo(msg string, args ...any) {
	if e.Logger != nil {
		e.Logger.Info(fmt.Sprintf(msg, args...))
	}
}

// logError
func (e *Engine) logError(msg string, args ...any) {
	if e.Logger != nil {
		e.Logger.Error(fmt.Sprintf(msg, args...))
	}
}
