// Package rotom provides an in-memory key-value database.
package rotom

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
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
	OpSetTx Operation = iota + 1
	OpRemove
	OpRename
	OpMarshalBytes
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
	OpZSet
	OpZIncr
	OpZRemove
	// request
	Response
	ReqPing
	ReqGet
	ReqRanGet
	ReqLen
	ReqHLen
	ReqLLen
)

// cmdTable defines the number of parameters required for the operation.
var cmdTable = map[Operation]int{
	OpSetTx:        4,
	OpRemove:       1,
	OpRename:       2,
	OpMarshalBytes: 1,
	// map
	OpHSet:    3,
	OpHRemove: 2,
	// set
	OpSAdd:    2,
	OpSRemove: 2,
	OpSUnion:  3,
	OpSInter:  3,
	OpSDiff:   3,
	// list
	OpLPush: 2,
	OpLPop:  1,
	OpRPush: 2,
	OpRPop:  1,
	// bitmap
	OpBitSet:  3,
	OpBitFlip: 2,
	OpBitOr:   3,
	OpBitAnd:  3,
	OpBitXor:  3,
	// zset
	OpZSet:    4,
	OpZIncr:   3,
	OpZRemove: 2,
	// request
	Response:  2,
	ReqPing:   0,
	ReqGet:    1,
	ReqRanGet: 0,
	ReqLen:    0,
	ReqHLen:   1,
	ReqLLen:   1,
}

// VType is value type for OpSet.
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
	SepChar   = byte(255)
	timeCarry = 1e9
	NoTTL     = 0

	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
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
		Path:           "rotom.db",
		ShardCount:     1024,
		SyncPolicy:     base.EveryInterval,
		SyncInterval:   time.Second,
		ShrinkInterval: time.Minute,
		Logger:         slog.Default(),
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

	Path    string // Path of db file.
	tmpPath string

	SyncPolicy base.SyncPolicy // Data sync policy.

	SyncInterval   time.Duration // Job for db sync to disk.
	ShrinkInterval time.Duration // Job for shrink db file size.

	Logger *slog.Logger // Logger for db, set <nil> if you don't want to use it.
}

// Engine represents a rotom engine for storage.
type Engine struct {
	sync.Mutex
	*Config
	closed bool
	buf    *bytes.Buffer
	rwbuf  *bytes.Buffer
	m      *cache.GigaCache[string]
}

// Open opens a database specified by config.
// The file will be created automatically if not exist.
func Open(conf *Config) (*Engine, error) {
	e := &Engine{
		Config: conf,
		buf:    bytes.NewBuffer(nil),
		rwbuf:  bytes.NewBuffer(nil),
		m:      cache.New[string](conf.ShardCount),
	}
	e.tmpPath = e.Path + ".tmp"

	// load db from disk.
	if err := e.load(); err != nil {
		e.logError("db load error: %v", err)
	}

	// runtime monitor.
	e.backend(time.Minute, func() {
		e.printRuntimeStats()
	})

	if e.SyncPolicy != base.Never {
		// sync buffer to disk.
		e.backend(e.SyncInterval, func() {
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
		e.backend(e.ShrinkInterval, func() {
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
	e.Lock()
	defer e.Unlock()
	if e.closed {
		return base.ErrDatabaseClosed
	}
	_, err := e.writeTo(e.buf, e.Path)
	e.closed = true

	return err
}

// encode
func (e *Engine) encode(cd *Codec) {
	if e.SyncPolicy == base.Never {
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

// RandomGet
func (e *Engine) RandomGet() (string, any, int64, bool) {
	return e.m.RandomGet()
}

// Set store key-value pair.
func (e *Engine) Set(key string, val []byte) {
	e.SetTx(key, val, NoTTL)
}

// SetEx store key-value pair with ttl.
func (e *Engine) SetEx(key string, val []byte, ttl time.Duration) {
	e.SetTx(key, val, cache.GetUnixNano()+int64(ttl))
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
	m, err := e.fetchMap(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpHSet).Str(key).Str(field).Bytes(val))
	m.Set(field, val)

	return nil
}

// HRemove
func (e *Engine) HRemove(key, field string) error {
	m, err := e.fetchMap(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpHRemove).Str(key).Str(field))
	m.Delete(field)

	return nil
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
	s, err := e.fetchSet(key)
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
	return s.Has(item), nil
}

// SCard
func (e *Engine) SCard(key string) (int, error) {
	s, err := e.fetchSet(key)
	if err != nil {
		return 0, err
	}
	return s.Len(), nil
}

// SUnion
func (e *Engine) SUnion(key1, key2, dest string) error {
	s1, err := e.fetchSet(key1)
	if err != nil {
		return err
	}
	s2, err := e.fetchSet(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSUnion).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		s1.Union(s2)
	} else if key2 == dest {
		s2.Union(s1)
	} else {
		e.m.Set(dest, s1.Clone().Union(s2))
	}

	return nil
}

// SInter
func (e *Engine) SInter(key1, key2, dest string) error {
	s1, err := e.fetchSet(key1)
	if err != nil {
		return err
	}
	s2, err := e.fetchSet(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSInter).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		s1.Intersect(s2)
	} else if key2 == dest {
		s2.Intersect(s1)
	} else {
		e.m.Set(dest, s1.Clone().Intersect(s2))
	}

	return nil
}

// SDiff
func (e *Engine) SDiff(key1, key2, dest string) error {
	s1, err := e.fetchSet(key1)
	if err != nil {
		return err
	}
	s2, err := e.fetchSet(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpSDiff).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		s1.Difference(s2)
	} else if key2 == dest {
		s2.Difference(s1)
	} else {
		e.m.Set(dest, s1.Clone().Difference(s2))
	}

	return nil
}

// LPush
func (e *Engine) LPush(key, item string) error {
	ls, err := e.fetchList(key)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpLPush).Str(key).Str(item))
	ls.LPush(item)

	return nil
}

// RPush
func (e *Engine) RPush(key, item string) error {
	ls, err := e.fetchList(key)
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
	bm, err := e.fetchBitMap(key)
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
func (e *Engine) BitOr(key1, key2, dest string) error {
	bm1, err := e.fetchBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := e.fetchBitMap(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitOr).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		bm1.Or(bm2)
	} else if key2 == dest {
		bm2.Or(bm1)
	} else {
		e.m.Set(dest, bm1.Clone().Or(bm2))
	}

	return nil
}

// BitXor
func (e *Engine) BitXor(key1, key2, dest string) error {
	bm1, err := e.fetchBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := e.fetchBitMap(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitXor).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		bm1.Xor(bm2)
	} else if key2 == dest {
		bm2.Xor(bm1)
	} else {
		e.m.Set(dest, bm1.Clone().Xor(bm2))
	}

	return nil
}

// BitAnd
func (e *Engine) BitAnd(key1, key2, dest string) error {
	bm1, err := e.fetchBitMap(key1)
	if err != nil {
		return err
	}
	bm2, err := e.fetchBitMap(key2)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpBitAnd).Str(key1).Str(key2).Str(dest))

	if key1 == dest {
		bm1.And(bm2)
	} else if key2 == dest {
		bm2.And(bm1)
	} else {
		e.m.Set(dest, bm1.Clone().And(bm2))
	}

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
	zs, err := e.fetchZSet(zset)
	if err != nil {
		return err
	}
	e.encode(NewCodec(OpZSet).Str(zset).Str(key).Float(score).Bytes(val))
	zs.SetWithScore(key, score, val)

	return nil
}

// ZIncr
func (e *Engine) ZIncr(zset, key string, incr float64) (float64, error) {
	zs, err := e.fetchZSet(zset)
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

	e.logInfo("start to load e size %s", formatSize(len(line)))

	var args [][]byte

	// record line is like:
	// <OP><argsNum><args...>
	for len(line) > 2 {
		op := Operation(line[0])
		argsNum := cmdTable[op]
		line = line[1:]

		// parse args by operation
		args, line, err = parseLine(line, argsNum)
		if err != nil {
			return err
		}

		switch op {
		case OpMarshalBytes: // val
			if err := e.m.UnmarshalBytes(args[0]); err != nil {
				return err
			}

		case OpSetTx: // type, key, ts, val
			ts := base.ParseInt[int64](args[2]) * timeCarry
			if ts < cache.GetUnixNano() && ts != NoTTL {
				continue
			}

			vType := VType(args[0][0])

			switch vType {
			case TypeString:
				e.m.SetTx(*b2s(args[1]), args[3], ts)

			case TypeList:
				var ls List
				if err := ls.UnmarshalJSON(args[3]); err != nil {
					return err
				}
				e.m.Set(*b2s(args[1]), ls)

			case TypeMap:
				var m Map
				if err := m.UnmarshalJSON(args[3]); err != nil {
					return err
				}
				e.m.Set(*b2s(args[1]), m)

			case TypeBitmap:
				var m BitMap
				if err := m.UnmarshalBinary(args[3]); err != nil {
					return err
				}
				e.m.Set(*b2s(args[1]), m)

			default:
				return fmt.Errorf("%v: %d", base.ErrUnSupportDataType, vType)
			}

		case OpRemove: // key
			e.m.Delete(*b2s(args[0]))

		case OpRename: // old, new
			e.m.Rename(*b2s(args[0]), *b2s(args[1]))

		case OpHSet: // key, field, val
			m, err := e.fetchMap(*b2s(args[0]))
			if err != nil {
				return err
			}
			m.Set(*b2s(args[1]), args[2])

		case OpHRemove: // key, field
			m, err := e.fetchMap(*b2s(args[0]))
			if err != nil {
				return err
			}
			m.Delete(*b2s(args[1]))

		case OpLPush: // key, item
			ls, err := e.fetchList(*b2s(args[0]))
			if err != nil {
				return err
			}
			ls.LPush(*b2s(args[1]))

		case OpRPush: // key, item
			ls, err := e.fetchList(*b2s(args[0]))
			if err != nil {
				return err
			}
			ls.RPush(*b2s(args[1]))

		case OpLPop: // key
			ls, err := e.fetchList(*b2s(args[0]))
			if err != nil {
				return err
			}
			ls.LPop()

		case OpRPop: // key
			ls, err := e.fetchList(*b2s(args[0]))
			if err != nil {
				return err
			}
			ls.RPop()

		case OpBitSet: // key, offset, val
			bm, err := e.fetchBitMap(*b2s(args[0]))
			if err != nil {
				return err
			}

			offset := base.ParseInt[uint32](args[1])
			if args[2][0] == _true {
				bm.Add(offset)
			} else {
				bm.Remove(offset)
			}

		case OpBitFlip: // key, offset
			bm, err := e.fetchBitMap(*b2s(args[0]))
			if err != nil {
				return err
			}
			bm.Flip(base.ParseInt[uint64](args[1]))

		case OpBitAnd, OpBitOr, OpBitXor: // key, src, dst
			bm1, err := e.fetchBitMap(*b2s(args[0]))
			if err != nil {
				return err
			}

			bm2, err := e.fetchBitMap(*b2s(args[1]))
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
					e.m.Set(*b2s(args[2]), bm1.Clone().And(bm2))
				case OpBitOr:
					e.m.Set(*b2s(args[2]), bm1.Clone().Or(bm2))
				case OpBitXor:
					e.m.Set(*b2s(args[2]), bm1.Clone().Xor(bm2))
				}
			}

		case OpZSet: // key, field, score, val
			zs, err := e.fetchZSet(*b2s(args[0]))
			if err != nil {
				return err
			}
			s, err := strconv.ParseFloat(*b2s(args[2]), 64)
			if err != nil {
				return err
			}
			zs.SetWithScore(*b2s(args[1]), s, args[3])

		case OpZIncr: // key, field, incr
			zs, err := e.fetchZSet(*b2s(args[0]))
			if err != nil {
				return err
			}
			s, err := strconv.ParseFloat(*b2s(args[2]), 64)
			if err != nil {
				return err
			}
			zs.Incr(*b2s(args[1]), s)

		case OpZRemove: // key, field
			zs, err := e.fetchZSet(*b2s(args[0]))
			if err != nil {
				return err
			}
			zs.Delete(*b2s(args[1]))

		default:
			return fmt.Errorf("%v: %c", base.ErrUnknownOperationType, op)
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
	e.writeTo(e.rwbuf, e.tmpPath)
	e.writeTo(e.buf, e.tmpPath)

	os.Rename(e.tmpPath, e.Path)

	e.logInfo("rotom rewrite done")
}

// parseLine parse file content to record lines.
// exp:
// input: <key_len>SEP<key_value><somthing...>
// return: key_value, somthing..., error
func parseLine(line []byte, argsNum int) ([][]byte, []byte, error) {
	res := make([][]byte, 0, argsNum)

	for flag := 0; flag < argsNum; flag++ {
		i := bytes.IndexByte(line, SepChar)
		if i <= 0 {
			return nil, nil, base.ErrParseRecordLine
		}

		key_len := base.ParseInt[int](line[:i])
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

// fetchMap
func (e *Engine) fetchMap(key string) (m Map, err error) {
	return fetch(e, key, func() Map {
		return structx.NewSyncMap[string, []byte]()
	})
}

// fetchSet
func (e *Engine) fetchSet(key string) (s Set, err error) {
	return fetch(e, key, func() Set {
		return structx.NewSet[string]()
	})
}

// fetchList
func (e *Engine) fetchList(key string) (m List, err error) {
	return fetch(e, key, func() List {
		return structx.NewList[string]()
	})
}

// fetchBitMap
func (e *Engine) fetchBitMap(key string) (bm BitMap, err error) {
	return fetch(e, key, func() BitMap {
		return structx.NewBitmap()
	})
}

// fetchZSet
func (e *Engine) fetchZSet(key string) (z ZSet, err error) {
	return fetch(e, key, func() ZSet {
		return structx.NewZSet[string, float64, []byte]()
	})
}

// fetch
func fetch[T any](e *Engine, key string, new func() T) (T, error) {
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
	e.m.Set(key, vptr)

	return vptr, nil
}

func formatSize[T base.Integer](size T) string {
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

func (e *Engine) backend(t time.Duration, f func()) {
	if t <= 0 {
		panic("invalid interval")
	}
	go func() {
		for {
			time.Sleep(t)
			if e.closed {
				return
			}
			f()
		}
	}()
}

func (e *Engine) printRuntimeStats() {
	if e.Logger == nil {
		return
	}

	var stats debug.GCStats
	var memStats runtime.MemStats

	debug.ReadGCStats(&stats)
	runtime.ReadMemStats(&memStats)

	e.Logger.
		With("alloc", formatSize(memStats.Alloc)).
		With("sys", formatSize(memStats.Sys)).
		With("gctime", stats.NumGC).
		With("gcpause", stats.PauseTotal/time.Duration(stats.NumGC)).
		Info("[Runtime]")
}

func (e *Engine) logInfo(msg string, args ...any) {
	if e.Logger == nil {
		return
	}

	if len(args) == 0 {
		e.Logger.Info(msg)
	} else {
		e.Logger.Info(fmt.Sprintf(msg, args...))
	}
}

func (e *Engine) logError(msg string, args ...any) {
	if e.Logger == nil {
		return
	}

	if len(args) == 0 {
		e.Logger.Error(msg)
	} else {
		e.Logger.Error(fmt.Sprintf(msg, args...))
	}
}
