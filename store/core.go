package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

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
)

func (s *storeShard) load() {
	data, err := os.ReadFile(s.storePath)
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
	if err := os.Rename(s.rwPath, s.storePath); err != nil {
		panic(err)
	}
}

func (s *storeShard) flushBuffer() (int, error) { return s.flush(s.buf, s.storePath) }

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
		if path == s.storePath {
			s.buf = s.buf[0:0]

		} else if path == s.rwPath {
			s.rwbuf = s.rwbuf[0:0]
		}

		return n, nil
	}
}

// read line
func (s *storeShard) readLine(line []byte) {
	// line valid
	if !bytes.HasSuffix(line, []byte{sprChar}) {
		return
	}
	line = line[:len(line)-1]

	switch line[0] {
	// SET: {op}{key}|{value}
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

	// SET_WITH_TTL: {op}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		sp1 := bytes.IndexByte(line, sprChar)
		sp2 := bytes.IndexByte(line[sp1+1:], sprChar)
		sp2 += sp1 + 1

		if !s.testAndAdd(line[1:sp1]) {
			return
		}

		ts, _ := binary.Varint(line[sp1+1 : sp2])
		// not expired
		if ts > GlobalTime() {
			s.rwbuf = append(s.rwbuf, line...)
			s.rwbuf = append(s.rwbuf, lineSpr...)

			if atomic.LoadUint32(&s.status) == REWRITE {
				return
			}

			s.SetWithDeadLine(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), ts)
		}

	// REMOVE: {op}{key}
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

func (s *storeShard) testAndAdd(line []byte) bool {
	// 仅当 filter.Test() 为 false 时返回 true
	if s.filter.Test(line) {
		return false
	}
	s.filter.Add(line)
	return true
}

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

// encodeBytes without assert
func (s *storeShard) encodeBytes(v ...byte) { s.buf = append(s.buf, v...) }

// encodeInt64 without assert
func (s *storeShard) encodeInt64(v int64) { s.buf = binary.AppendVarint(s.buf, v) }

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
