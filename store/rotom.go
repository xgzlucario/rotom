package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

const (
	STATUS_INIT uint32 = iota + 1
	STATUS_NORMAL
	STATUS_REWRITE
)

const (
	OP_SET byte = iota + 1
	OP_SETEX
	OP_REMOVE
	OP_PERSIST

	// TODO
	OP_HGET
	OP_HSET
	OP_HREMOVE

	// TODO
	OP_GETBIT
	OP_SETBIT
	OP_COUNTBIT
)

const (
	C_SPR = byte(0x00)
	C_END = byte(0xff)

	timeCarry = 1000 * 1000 * 1000
)

var (
	globalTime = time.Now().UnixNano()

	lineSpr = []byte{C_SPR, C_SPR, C_END}

	order = binary.BigEndian
)

type store struct {
	*Config
	mask   uint64
	shards []*storeShard
}

type Config struct {
	ShardCount uint64
	DBDirPath  string

	// FlushDuration is the time interval for flushing data to disk
	FlushDuration time.Duration

	// RewriteDuration is the time interval for rewriting data to disk
	RewriteDuration time.Duration
}

type storeShard struct {
	// runtime status
	status uint32

	// dbPath and rwPath
	path   string
	rwPath string

	// buffer and rwbuffer
	buf   []byte
	rwbuf []byte

	// data based on Cache
	*structx.Cache[Value]

	// filter
	filter *structx.Bloom

	sync.RWMutex
}

func init() {
	go func() {
		for t := range time.NewTicker(time.Microsecond).C {
			atomic.SwapInt64(&globalTime, t.UnixNano())
		}
	}()
}

func CreateDB(conf *Config) *store {
	db := &store{
		Config: conf,
		mask:   conf.ShardCount - 1,
		shards: make([]*storeShard, conf.ShardCount),
	}

	if err := os.MkdirAll(db.DBDirPath, os.ModeDir); err != nil {
		panic(err)
	}

	// load config
	for i := range db.shards {
		db.shards[i] = &storeShard{
			status: STATUS_INIT,
			path:   path.Join(db.DBDirPath, "dat"+strconv.Itoa(i)),
			rwPath: path.Join(db.DBDirPath, "rw"+strconv.Itoa(i)),
			Cache:  structx.NewCache[Value](),
		}
	}

	// init
	pool := structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]
		pool.Go(func() { sd.reWrite() })
	}
	pool.Wait()

	// start worker
	pool = structx.NewDefaultPool()
	for i := range db.shards {
		sd := db.shards[i]

		// flush worker
		go func() {
			for {
				time.Sleep(db.FlushDuration)
				switch sd.getStatus() {
				case STATUS_NORMAL:
					sd.flushBuffer()

				case STATUS_REWRITE:
					sd.flushRwBuffer()
				}
			}
		}()
		// rewrite worker
		go func() {
			for {
				time.Sleep(db.RewriteDuration)
				pool.Go(func() {
					sd.flushBuffer()
					sd.setStatus(STATUS_REWRITE)
					sd.reWrite()
				})
			}
		}()
	}

	return db
}

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

	sd.Set(key, Value{Val: val})
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

	sd.SetTX(key, Value{Val: val}, i64ts)
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

// Flush writes all the buf data to disk
func (s store) Flush() error {
	for _, sd := range s.shards {
		if _, err := sd.flushBuffer(); err != nil {
			return err
		}
	}
	return nil
}

// Count
func (s store) Count() (sum int) {
	for _, s := range s.shards {
		sum += s.Count()
	}
	return sum
}

// WithExpired
func (s store) WithExpired(f func(string, Value, int64)) store {
	for _, s := range s.shards {
		s.WithExpired(f)
	}
	return s
}

// Keys
func (s store) Keys() []string {
	arr := make([]string, 0, s.Count())
	for _, s := range s.shards {
		arr = append(arr, s.Keys()...)
	}
	return arr
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
	// valid the end of line
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
		s.Set(*base.B2S(line[1:i]), Value{Raw: line[i+1:]})

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

			s.SetTX(*base.B2S(line[1:sp1]), Value{Raw: line[sp2+1:]}, ts)
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
func (s *store) getShard(key string) *storeShard {
	return s.shards[xxh3.HashString(key)&(s.ShardCount-1)]
}

// Get
func (s *store) Get(key string) Value {
	hash := xxh3.HashString(key)
	sd := s.shards[xxh3.HashString(key)&(s.ShardCount-1)]
	val, ok := sd.Cache.GetPos(hash)

	val.sd = sd
	val.ok = ok

	return val
}

// getValue
func getValue[T any](v Value, vptr T) (T, error) {
	if v.Raw != nil {
		if err := v.sd.Decode(v.Raw, &vptr); err != nil {
			return vptr, err
		}

		v.sd.Set(v.key, Value{Val: vptr})
		return vptr, nil
	}

	if tmp, ok := v.Val.(T); ok {
		return tmp, nil

	} else {
		return vptr, base.ErrUnSupportType
	}
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
