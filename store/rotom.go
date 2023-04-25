package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

const (
	// Status
	STATUS_INIT uint32 = iota + 1
	STATUS_NORMAL
	STATUS_REWRITE

	// Operation
	OP_SET byte = iota + '1'
	OP_SETEX
	OP_REMOVE
	OP_PERSIST
	OP_HSET
	OP_HREMOVE

	// Char
	C_SPR = byte(0x00)
	C_END = byte('\n')

	// Config
	timeCarry = 1000 * 1000 * 1000
)

var (
	// DB file path
	DBFilePath = "db/"

	// ShardCount for db
	ShardCount uint64 = 32

	// FlushDuration is the time interval for flushing data to disk
	FlushDuration = time.Second

	// RewriteDuration is the time interval for rewriting data to disk
	RewriteDuration = time.Second * 10

	// globalTime
	globalTime = time.Now().UnixNano()

	// default database
	db store

	lineSpr = []byte{C_SPR, C_SPR, C_END}

	order = binary.BigEndian

	errUnSupportType = errors.New("unsupport type")
)

type store []*storeShard

type storeShard struct {
	id int

	// runtime status
	status uint32

	// dbPath and rwPath
	path   string
	rwPath string

	// buffer and rwbuffer
	buf   []byte
	rwbuf []byte

	// data based on Cache
	*structx.Cache[any]

	// filter
	filter *structx.Bloom

	sync.RWMutex
}

func initGlobalTime() {
	for t := range time.NewTicker(time.Millisecond).C {
		atomic.SwapInt64(&globalTime, t.UnixNano())
	}
}

func init() {
	// db file directory
	if err := os.MkdirAll(DBFilePath, os.ModeDir); err != nil {
		panic(err)
	}

	db = make([]*storeShard, ShardCount)

	pool := structx.NewDefaultPool()
	rwPool := structx.NewDefaultPool()

	go initGlobalTime()

	// init db
	for i := range db {
		db[i] = &storeShard{
			id:     i,
			status: STATUS_INIT,
			path:   fmt.Sprintf("%sdat%d", DBFilePath, i),
			rwPath: fmt.Sprintf("%sdat%d.rw", DBFilePath, i),
			Cache:  structx.NewCache[any](),
		}
		sd := db[i]

		pool.Go(func() { sd.reWrite() })

		// flush buffer worker
		go func() {
			for {
				time.Sleep(FlushDuration)
				switch sd.getStatus() {
				case STATUS_NORMAL:
					sd.flushBuffer()

				case STATUS_INIT, STATUS_REWRITE:
					sd.flushRwBuffer()
				}
			}
		}()

		// rewrite worker
		go func() {
			for {
				time.Sleep(RewriteDuration)
				rwPool.Go(func() {
					sd.flushBuffer()
					sd.setStatus(STATUS_REWRITE)
					sd.reWrite()
				})
			}
		}()
	}
	pool.Wait()
}

// DB
func DB() store { return db }

// Set
func (s *store) Set(key string, val any) {
	sd := s.getShard(key)

	// {SET}{key}|{value}
	sd.Lock()
	sd.encBytes(OP_SET).encBytes(base.S2B(&key)...).encBytes(C_SPR)
	if err := sd.Encode(val); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)
	sd.Unlock()

	sd.Set(key, val)
}

// SetEX
func (s *store) SetEX(key string, val any, ttl time.Duration) {
	sd := s.getShard(key)

	i64ts := atomic.LoadInt64(&globalTime) + int64(ttl)
	u32ts := uint32(i64ts / timeCarry)

	// {SETEX}{key}|{ttl}|{value}
	sd.Lock()
	sd.encBytes(OP_SETEX).encBytes(base.S2B(&key)...).encBytes(C_SPR).encUint32(u32ts).encBytes(C_SPR)
	if err := sd.Encode(val); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)
	sd.Unlock()

	sd.SetTX(key, val, i64ts)
}

// Remove
func (s *store) Remove(key string) (any, bool) {
	sd := s.getShard(key)

	// {REMOVE}{key}
	sd.Lock()
	sd.encBytes(OP_REMOVE).encBytes(base.S2B(&key)...).encBytes(lineSpr...)
	sd.Unlock()

	return sd.Remove(key)
}

// Persist removes the expiration from a key
func (s *store) Persist(key string) bool {
	sd := s.getShard(key)

	// {PERSIST}{key}
	sd.Lock()
	sd.encBytes(OP_PERSIST).encBytes(base.S2B(&key)...).encBytes(lineSpr...)
	sd.Unlock()

	return sd.Persist(key)
}

// HGet
func (s *store) HGet(key string, fields ...string) (any, bool) {
	sd := s.getShard(key)
	sd.RLock()
	defer sd.RUnlock()

	val, _ := sd.Cache.Get(key)
	m, ok := val.(structx.HMap)
	if ok {
		return m.HGet(fields...)
	}
	return nil, false
}

// HSet
func (s *store) HSet(val any, key string, fields ...string) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {HSET}{key}|{fields}|{value}
	sd.encBytes(OP_HSET).encBytes(base.S2B(&key)...).encBytes(C_SPR).encStringSlice(fields).encBytes(C_SPR)
	if err := sd.Encode(val); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)

	// set
	v, _ := sd.Cache.Get(key)
	m, ok := v.(structx.HMap)
	if ok {
		m.HSet(val, fields...)
	} else {
		m := structx.NewHMap()
		m.HSet(val, fields...)
		sd.Cache.Set(key, m)
	}
}

// HRemove
func (s *store) HRemove(key string, fields ...string) (any, bool) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {HREMOVE}{key}|{fields}
	sd.encBytes(OP_HREMOVE).encBytes(base.S2B(&key)...).encBytes(C_SPR).encStringSlice(fields).encBytes(lineSpr...)

	val, _ := sd.Cache.Get(key)
	m, ok := val.(structx.HMap)
	if ok {
		return m.HRemove(fields...)
	}
	return nil, false
}

// Type returns the type of the value stored at key
func (s *store) Type(key string) reflect.Type {
	sd := s.getShard(key)
	v, ok := sd.Get(key)
	if ok {
		return reflect.TypeOf(v)
	}
	return nil
}

// Flush writes all the buf data to disk
func (s store) Flush() error {
	for _, sd := range s {
		if _, err := sd.flushBuffer(); err != nil {
			return err
		}
	}
	return nil
}

// Count
func (s store) Count() (sum int) {
	for _, s := range s {
		sum += s.Count()
	}
	return sum
}

// WithExpired
func (s store) WithExpired(f func(string, any, int64)) store {
	for _, s := range s {
		s.WithExpired(f)
	}
	return s
}

// Keys
func (s store) Keys() []string {
	arr := make([]string, 0, s.Count())
	for _, s := range s {
		arr = append(arr, s.Keys()...)
	}
	return arr
}

// Incr
func (s *store) Incr(key string, incr float64) (val float64, err error) {
	val, err = s.GetFloat64(key)
	if err != nil {
		return -1, err
	}
	val += incr
	s.Set(key, val)
	return
}

// GetString
func (s *store) GetString(k string) (v string, err error) { getValue(k, &v); return }

// GetInt
func (s *store) GetInt(k string) (v int, err error) { getValue(k, &v); return }

// GetInt32
func (s *store) GetInt32(k string) (v int32, err error) { getValue(k, &v); return }

// GetInt64
func (s *store) GetInt64(k string) (v int64, err error) { getValue(k, &v); return }

// GetUint
func (s *store) GetUint(k string) (v uint, err error) { getValue(k, &v); return }

// GetUint32
func (s *store) GetUint32(k string) (v uint32, err error) { getValue(k, &v); return }

// GetUint64
func (s *store) GetUint64(k string) (v uint64, err error) { getValue(k, &v); return }

// GetFloat32
func (s *store) GetFloat32(k string) (v float32, err error) { getValue(k, &v); return }

// GetFloat64
func (s *store) GetFloat64(k string) (v float64, err error) { getValue(k, &v); return }

// GetBool
func (s *store) GetBool(k string) (v bool, err error) { getValue(k, &v); return }

// GetIntSlice
func (s *store) GetIntSlice(k string) (v []int, err error) { getValue(k, &v); return }

// GetStringSlice
func (s *store) GetStringSlice(k string) (v []string, err error) { getValue(k, &v); return }

// GetTime
func (s *store) GetTime(k string) (v time.Time, err error) { getValue(k, &v); return }

// GetList
func GetList[T comparable](key string) (*structx.List[T], error) {
	return getValue(key, structx.NewList[T]())
}

// GetSet
func GetSet[T comparable](s *store, key string) (structx.Set[T], error) {
	return getValue(key, structx.NewSet[T]())
}

// GetMap
func GetMap[K comparable, V any](key string) (structx.Map[K, V], error) {
	return getValue(key, structx.NewMap[K, V]())
}

// GetHHMap
func (s *store) GetHMap(key string) (structx.HMap, error) {
	return getValue(key, structx.NewHMap())
}

// GetTrie
func GetTrie[T any](key string) (*structx.Trie[T], error) {
	return getValue(key, structx.NewTrie[T]())
}

// GetZset
func GetZset[K, S base.Ordered, V any](key string) (*structx.ZSet[K, S, V], error) {
	return getValue(key, structx.NewZSet[K, S, V]())
}

// GetBitMap
func (s *store) GetBitMap(key string) (*structx.BitMap, error) {
	return getValue(key, structx.NewBitMap())
}

// GetBloom
func (s *store) GetBloom(key string) (*structx.Bloom, error) {
	return getValue(key, structx.NewBloom())
}

// Get
func Get[T any](key string, data T) (T, error) {
	return getValue(key, data)
}

// reWrite shrink the database
func (s *storeShard) reWrite() {
	defer s.setStatus(STATUS_NORMAL)

	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}

	// init filter
	s.filter = structx.NewBloom()

	// read line from tail
	lines := bytes.Split(data, []byte{C_SPR, C_END})
	status := s.getStatus()

	for i := len(lines) - 1; i >= 0; i-- {
		s.readLine(lines[i], status)
	}

	// flush
	s.Lock()
	defer s.Unlock()

	fs, err := os.OpenFile(s.rwPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return
	}

	fs.Write(s.rwbuf)
	s.rwbuf = s.rwbuf[0:0]

	fs.Write(s.buf)
	s.buf = s.buf[0:0]

	fs.Close()

	// rename rwFile to storeFile
	if err := os.Rename(s.rwPath, s.path); err != nil {
		panic(err)
	}
}

func (s *storeShard) flushBuffer() (int, error) { return s.flush(s.buf, s.path) }

func (s *storeShard) flushRwBuffer() (int, error) { return s.flush(s.rwbuf, s.rwPath) }

// flush
func (s *storeShard) flush(buf []byte, path string) (int, error) {
	s.Lock()
	defer s.Unlock()

	if len(buf) == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fs.Close()

	if n, err := fs.Write(buf); err != nil {
		return 0, err

	} else {
		// reset
		if path == s.path {
			s.buf = s.buf[0:0]

		} else if path == s.rwPath {
			s.rwbuf = s.rwbuf[0:0]
		}

		return n, nil
	}
}

// readLine
func (s *storeShard) readLine(line []byte, status uint32) {
	// valid that the end of line must be C_SPR
	if n := len(line); n == 0 || line[n-1] != C_SPR {
		return
	} else {
		line = line[:n-1]
	}

	switch line[0] {
	// {SET}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, C_SPR)
		if i <= 0 {
			return
		}

		// test the key is in filter and is nessesary to write
		if !s.testAndAdd(line[1:i]) {
			return
		}

		s.rwbuf = append(s.rwbuf, line...)
		s.rwbuf = append(s.rwbuf, lineSpr...)

		if status == STATUS_REWRITE {
			return
		}
		s.Set(*base.B2S(line[1:i]), line[i+1:])

	// {SETEX}{key}|{ttl}|{value}
	case OP_SETEX:
		sp1 := bytes.IndexByte(line, C_SPR)
		sp2 := bytes.IndexByte(line[sp1+1:], C_SPR)
		sp2 += sp1 + 1

		if !s.testAndAdd(line[1:sp1]) {
			return
		}

		u64ts, _ := binary.Uvarint(line[sp1+1 : sp2])
		ts := int64(u64ts) * timeCarry
		// not expired
		if ts > atomic.LoadInt64(&globalTime) {
			s.rwbuf = append(s.rwbuf, line...)
			s.rwbuf = append(s.rwbuf, lineSpr...)

			if status == STATUS_REWRITE {
				return
			}

			s.SetTX(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), ts)
		}

	// {REMOVE}{key}
	case OP_REMOVE:
		// test {key}
		if !s.testAndAdd(line[1:]) {
			return
		}
		if status == STATUS_REWRITE {
			return
		}
		s.Remove(*base.B2S(line[1:]))

	// {PERSIST}{key}
	case OP_PERSIST:
		// test {PERSIST}{key}
		if !s.testAndAdd(line) {
			return
		}

		s.rwbuf = append(s.rwbuf, line...)
		s.rwbuf = append(s.rwbuf, lineSpr...)

		if status == STATUS_REWRITE {
			return
		}
		s.Persist(*base.B2S(line[1:]))
	}
}

// testAndAdd
func (s *storeShard) testAndAdd(line []byte) bool {
	if s.filter.Test(line) {
		return false
	}
	s.filter.Add(line)
	return true
}

// getShard
func (s store) getShard(key string) *storeShard {
	return s[xxh3.HashString(key)&(ShardCount-1)]
}

// getValue
func getValue[T any](key string, vptr T) (T, error) {
	s := db.getShard(key)
	val, ok := s.Get(key)
	if !ok {
		return vptr, base.ErrKeyNotFound(key)
	}

	switch v := val.(type) {
	case T:
		return v, nil

	case []byte:
		if err := s.Decode(v, vptr); err != nil {
			return vptr, err
		}
		s.Set(key, vptr)

	default:
		return vptr, errUnSupportType
	}

	return vptr, nil
}

// encBytes
func (s *storeShard) encBytes(v ...byte) *storeShard {
	s.buf = append(s.buf, v...)
	return s
}

// encUint32
func (s *storeShard) encUint32(v uint32) *storeShard {
	s.buf = binary.AppendUvarint(s.buf, uint64(v))
	return s
}

// encStringSlice
func (s *storeShard) encStringSlice(v []string) *storeShard {
	str := strings.Join(v, ",")
	s.buf = append(s.buf, base.S2B(&str)...)
	return s
}

// Encode
func (s *storeShard) Encode(v any) error {
	switch v := v.(type) {
	case string:
		s.buf = append(s.buf, base.S2B(&v)...)
	case []byte:
		s.buf = append(s.buf, v...)
	case int64:
		s.buf = binary.AppendVarint(s.buf, v)
	case uint64:
		s.buf = binary.AppendUvarint(s.buf, v)
	case int:
		s.buf = binary.AppendVarint(s.buf, int64(v))
	case uint:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
	case int32:
		s.buf = binary.AppendVarint(s.buf, int64(v))
	case uint32:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
	case bool:
		if v {
			s.buf = append(s.buf, 1)
		} else {
			s.buf = append(s.buf, 0)
		}
	case float64:
		s.buf = order.AppendUint64(s.buf, math.Float64bits(v))
	case uint8:
		s.buf = append(s.buf, v)
	case int8:
		s.buf = binary.AppendVarint(s.buf, int64(v))
	case uint16:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
	case int16:
		s.buf = binary.AppendVarint(s.buf, int64(v))
	case float32:
		s.buf = order.AppendUint32(s.buf, math.Float32bits(v))
	case []string:
		str := strings.Join(v, ",")
		s.buf = append(s.buf, base.S2B(&str)...)
	case []int:
		for _, i := range v {
			s.buf = binary.AppendVarint(s.buf, int64(i))
		}
	case time.Time:
		src, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
	case base.Binarier:
		src, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
	case base.Marshaler:
		src, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
	default:
		return errors.New("encode unsupported type: " + reflect.TypeOf(v).String())
	}
	return nil
}

// Decode
func (s *storeShard) Decode(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case *[]byte:
		*v = src
	case *string:
		*v = *base.B2S(src)
	case *int64:
		*v, _ = binary.Varint(src)
	case *uint64:
		*v, _ = binary.Uvarint(src)
	case *int32:
		num, _ := binary.Varint(src)
		*v = int32(num)
	case *uint32:
		num, _ := binary.Uvarint(src)
		*v = uint32(num)
	case *float64:
		*v = math.Float64frombits(order.Uint64(src))
	case *bool:
		*v = src[0] != 0
	case *uint:
		num, _ := binary.Uvarint(src)
		*v = uint(num)
	case *int:
		num, _ := binary.Varint(src)
		*v = int(num)
	case *uint8:
		*v = src[0]
	case *int8:
		num, _ := binary.Varint(src)
		*v = int8(num)
	case *uint16:
		num, _ := binary.Uvarint(src)
		*v = uint16(num)
	case *int16:
		num, _ := binary.Varint(src)
		*v = int16(num)
	case *float32:
		*v = math.Float32frombits(order.Uint32(src))
	case *[]string:
		*v = strings.Split(*base.B2S(src), ",")
	case *[]int:
		*v = make([]int, 0)
		for len(src) > 0 {
			num, n := binary.Varint(src)
			src = src[n:]
			*v = append(*v, int(num))
		}
	case *time.Time:
		return v.UnmarshalBinary(src)
	case base.Binarier:
		return v.UnmarshalBinary(src)
	case base.Marshaler:
		return v.UnmarshalJSON(src)
	default:
		return errors.New("decode unsupported type: " + reflect.TypeOf(v).String())
	}
	return nil
}

func (s *storeShard) getStatus() uint32 {
	return atomic.LoadUint32(&s.status)
}

func (s *storeShard) setStatus(status uint32) {
	atomic.SwapUint32(&s.status, status)
}
