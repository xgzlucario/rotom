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

	"github.com/rs/zerolog"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

var (
	// DBPath for db file directory
	DBPath = "db/"

	// ShardCount for db
	ShardCount uint64 = 32

	// FlushDuration is the time interval for flushing data to disk
	FlushDuration = time.Second

	// RewriteDuration is the time interval for rewriting data to disk
	RewriteDuration = time.Second * 10

	// global time
	globalTime = time.Now().UnixNano()

	// database
	db store
)

const (
	INIT uint32 = iota + 1
	NORMAL
	REWRITE
)

type store []*storeShard

type storeShard struct {
	// shard index
	id int

	// runtime status
	status uint32

	// DBPath and rwPath
	path   string
	rwPath string

	// buffer
	buf []byte

	// rw buffer
	rwbuf []byte

	// data
	*structx.Cache[any]

	// filter
	filter *structx.Bloom

	// log
	logger *zerolog.Logger

	sync.RWMutex
}

func initGlobalTime() {
	for t := range time.NewTicker(time.Millisecond).C {
		atomic.SwapInt64(&globalTime, t.UnixNano())
	}
}

func init() {
	// db file directory
	if err := os.MkdirAll(DBPath, os.ModeDir); err != nil {
		panic(err)
	}

	db = make([]*storeShard, ShardCount)

	pool := structx.NewDefaultPool()
	rwPool := structx.NewDefaultPool()

	go initGlobalTime()

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// init
	for i := range db {
		db[i] = &storeShard{
			id:     i,
			status: INIT,
			path:   fmt.Sprintf("%sdat%d", DBPath, i),
			rwPath: fmt.Sprintf("%sdat%d.rw", DBPath, i),
			Cache:  structx.NewCache[any](),
			logger: &logger,
		}
		sd := db[i]

		// flush buffer
		go func() {
			for {
				time.Sleep(FlushDuration)
				switch sd.getStatus() {
				case NORMAL:
					sd.flushBuffer()

				case INIT, REWRITE:
					sd.flushRwBuffer()
				}
			}
		}()

		// rewrite
		go func() {
			for {
				time.Sleep(RewriteDuration)
				rwPool.Go(func() {
					sd.flushBuffer()
					sd.setStatus(REWRITE)
					sd.reWrite()
				})
			}
		}()

		pool.Go(func() {
			sd.reWrite()
		})
	}
	pool.Wait()
}

// DB
func DB() store { return db }

// Set
func (s *store) Set(key string, value any) {
	sd := s.getShard(key)

	// {SET}{key}|{value}
	sd.Lock()
	sd.encBytes(OP_SET).encBytes(base.S2B(&key)...).encBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)
	sd.Unlock()

	sd.Set(key, value)
}

// SetWithTTL
func (s *store) SetWithTTL(key string, value any, ttl time.Duration) {
	sd := s.getShard(key)
	ts := atomic.LoadInt64(&globalTime) + int64(ttl)

	// {SET_WITH_TTL}{key}|{ttl}|{value}
	sd.Lock()
	sd.encBytes(OP_SET_WITH_TTL).encBytes(base.S2B(&key)...).encBytes(sprChar).encInt64(ts).encBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)
	sd.Unlock()

	sd.SetWithTTL(key, value, ttl)
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
	m, ok := val.(structx.MMap)
	if ok {
		return m.Get(fields...)
	}
	return nil, false
}

// HSet
func (s *store) HSet(value any, key string, fields ...string) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {HSET}{key}|{fields}|{value}
	sd.encBytes(OP_HSET).encBytes(base.S2B(&key)...).encBytes(sprChar).encStringSlice(fields).encBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encBytes(lineSpr...)

	// set
	val, _ := sd.Cache.Get(key)
	m, ok := val.(structx.MMap)
	if ok {
		m.Set(value, fields...)
	} else {
		m := structx.NewMMap()
		m.Set(value, fields...)
		sd.Cache.Set(key, m)
	}
}

// HRemove
func (s *store) HRemove(key string, fields ...string) (any, bool) {
	sd := s.getShard(key)
	sd.Lock()
	defer sd.Unlock()

	// {HREMOVE}{key}|{fields}
	sd.encBytes(OP_HREMOVE).encBytes(base.S2B(&key)...).encBytes(sprChar).encStringSlice(fields).encBytes(lineSpr...)

	val, _ := sd.Cache.Get(key)
	m, ok := val.(structx.MMap)
	if ok {
		return m.Remove(fields...)
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
func (s store) WithExpired(f func(string, any)) store {
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

// GetSyncMap
func GetSyncMap[T any](key string) (*structx.SyncMap[string, T], error) {
	return getValue(key, structx.NewSyncMap[string, T]())
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

// GetMMap
func (s *store) GetMMap(key string) (structx.MMap, error) {
	return getValue(key, structx.MMap{})
}

// Get
func Get[T any](key string, data T) (T, error) {
	return getValue(key, data)
}

const (
	// Operation
	OP_SET byte = iota + '1'
	OP_SET_WITH_TTL
	OP_REMOVE
	OP_PERSIST
	OP_HSET
	OP_HREMOVE

	// seperate char
	sprChar = byte(0x00)

	// endline char
	endChar = byte('\n')
)

var (
	lineSpr = []byte{sprChar, sprChar, endChar}

	order = binary.BigEndian

	errUnSupportType = errors.New("unsupported type")
)

func (s *storeShard) reWrite() {
	defer s.setStatus(NORMAL)

	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}

	// init filter
	s.filter = structx.NewBloom()

	// read line from tail
	lines := bytes.Split(data, []byte{sprChar, endChar})
	for i := len(lines) - 1; i >= 0; i-- {
		s.readLine(lines[i])
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

	s.logger.Info().Int("shard", s.id).Str("op", "flush")

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
func (s *storeShard) readLine(line []byte) {
	// valid
	if !bytes.HasSuffix(line, []byte{sprChar}) {
		return
	}
	line = line[:len(line)-1]

	switch line[0] {
	// {SET}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, sprChar)
		if i <= 0 {
			return
		}

		// 当 key 存在时，表示该 key 在未来被 SET, SET_WITH_TTL, REMOVE 过, 不需要重演
		if !s.testAndAdd(line[1:i]) {
			return
		}

		s.rwbuf = append(s.rwbuf, line...)
		s.rwbuf = append(s.rwbuf, lineSpr...)

		if atomic.LoadUint32(&s.status) == REWRITE {
			return
		}
		s.Set(*base.B2S(line[1:i]), line[i+1:])

	// {SET_WITH_TTL}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		sp1 := bytes.IndexByte(line, sprChar)
		sp2 := bytes.IndexByte(line[sp1+1:], sprChar)
		sp2 += sp1 + 1

		if !s.testAndAdd(line[1:sp1]) {
			return
		}

		ts, _ := binary.Varint(line[sp1+1 : sp2])
		// not expired
		if ts > atomic.LoadInt64(&globalTime) {
			s.rwbuf = append(s.rwbuf, line...)
			s.rwbuf = append(s.rwbuf, lineSpr...)

			if atomic.LoadUint32(&s.status) == REWRITE {
				return
			}

			s.SetWithDeadLine(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), ts)
		}

	// {REMOVE}{key}
	case OP_REMOVE:
		if !s.testAndAdd(line[1:]) {
			return
		}
		if atomic.LoadUint32(&s.status) == REWRITE {
			return
		}

		// REMOVE 不需要重写，重写日志中应仅包含 SET, SET_WITH_TTL, PERWSIST 操作
		s.Remove(*base.B2S(line[1:]))

	// PERSIST: {op}{key}
	case OP_PERSIST:
		// 当 {op}{key} 存在时，表示该 key 未来被 PERSIST 过, 不需要重演
		if !s.testAndAdd(line) {
			return
		}

		s.rwbuf = append(s.rwbuf, line...)
		s.rwbuf = append(s.rwbuf, lineSpr...)

		if atomic.LoadUint32(&s.status) == REWRITE {
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
	sd := db.getShard(key)
	// get
	val, ok := sd.Get(key)
	if !ok {
		return vptr, base.ErrKeyNotFound(key)
	}

	// type assertion
	switch v := val.(type) {
	case T:
		return v, nil

	case []byte:
		if err := sd.Decode(v, vptr); err != nil {
			return vptr, err
		}
		sd.Set(key, vptr)

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

// encInt64
func (s *storeShard) encInt64(v int64) *storeShard {
	s.buf = binary.AppendVarint(s.buf, v)
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

// Session
type Session struct {
	store
	sdmap map[*storeShard]struct{}
}

// NewSession
func NewSession() *Session {
	return &Session{
		store: db,
		sdmap: make(map[*storeShard]struct{}),
	}
}

// Set
func (s *Session) Set(key string, val any) {
	s.sdmap[s.getShard(key)] = struct{}{}

	s.store.Set(key, val)
}

// SetWithTTL
func (s *Session) SetWithTTL(key string, val any, ttl time.Duration) {
	s.sdmap[s.getShard(key)] = struct{}{}

	s.store.SetWithTTL(key, val, ttl)
}

// Remove
func (s *Session) Remove(key string) (any, bool) {
	s.sdmap[s.getShard(key)] = struct{}{}

	return s.store.Remove(key)
}

// Persist
func (s *Session) Persist(key string) bool {
	s.sdmap[s.getShard(key)] = struct{}{}

	return s.store.Persist(key)
}

// Commit
func (s *Session) Commit() error {
	for sd := range s.sdmap {
		if _, err := sd.flushBuffer(); err != nil {
			return err
		}
	}
	s.sdmap = make(map[*storeShard]struct{})
	return nil
}
