package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

const (
	// 操作码
	// 从 `1` 开始, 以便于在文件中直接使用 `1` 表示 `OP_SET`
	OP_SET byte = iota + '1'
	OP_SET_WITH_TTL
	OP_REMOVE
	OP_PERSIST
	OP_HSET
	OP_HREMOVE

	// 分隔符, 分隔数据行中的不同字段
	// 同时也是校验符, 校验数据行是否完整
	// 判断一条数据是否完整, 只需判断最后一个字符是否为 sprChar 即可
	sprChar = byte(0)

	// 换行符, 表示数据行的结尾
	endChar = byte('\n')
)

var (
	// 行尾, 起到换行及校验的作用
	lineSpr = []byte{sprChar, sprChar, endChar}

	order = binary.BigEndian
)

func (s *storeShard) load() {
	s.Lock()
	defer s.Unlock()

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

	// rewrite
	fs, err := os.OpenFile(s.rwPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	fs.Write(s.buf)
	s.buf = s.buf[0:0]
	fs.Close()

	// rename rwFile to storeFile
	if err := os.Rename(s.rwPath, s.storePath); err != nil {
		panic(err)
	}
}

// WriteBuffer
func (s *storeShard) WriteBuffer() (int, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.buf) == 0 {
		return 0, nil
	}

	return s.flush(s.storePath)
}

// ReWriteBuffer
func (s *storeShard) ReWriteBuffer() (int, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.buf) == 0 {
		return 0, nil
	}

	return s.flush(s.rwPath)
}

// flush buffer to file
func (s *storeShard) flush(path string) (int, error) {
	fs, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer fs.Close()

	// write file
	if n, err := fs.Write(s.buf); err != nil {
		return 0, err

	} else {
		// reset buf
		s.buf = s.buf[0:0]
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

		s.buf = append(s.buf, line...)
		s.buf = append(s.buf, lineSpr...)

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
			s.buf = append(s.buf, line...)
			s.buf = append(s.buf, lineSpr...)

			s.SetWithDeadLine(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), ts)
		}

	// REMOVE: {op}{key}
	case OP_REMOVE:
		if !s.testAndAdd(line[1:]) {
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

		s.buf = append(s.buf, line...)
		s.buf = append(s.buf, lineSpr...)

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

func (s *store) getShard(key string) *storeShard {
	return s.shards[xxh3.HashString(key)&(ShardCount-1)]
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
	obj, ok := val.(T)
	if ok {
		return obj, nil
	}

	// unmarshal
	raw, ok := val.([]byte)
	if !ok {
		return vptr, base.ErrType(key)
	}

	if err := sd.Decode(raw, vptr); err != nil {
		return vptr, err
	}
	sd.Set(key, vptr)

	return vptr, nil
}

// encodeBytes without assert
func (s *storeShard) encodeBytes(v ...byte) {
	s.buf = append(s.buf, v...)
}

// encodeInt64 without assert
func (s *storeShard) encodeInt64(v int64) {
	s.buf = binary.AppendVarint(s.buf, v)
}

// Encode
func (s *storeShard) Encode(v any) error {
	switch v := v.(type) {
	case string:
		s.buf = append(s.buf, base.S2B(&v)...)
		return nil

	case []byte:
		s.buf = append(s.buf, v...)
		return nil

	case int64:
		s.buf = binary.AppendVarint(s.buf, v)
		return nil

	case uint64:
		s.buf = binary.AppendUvarint(s.buf, v)
		return nil

	case int:
		s.buf = binary.AppendVarint(s.buf, int64(v))
		return nil

	case uint:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
		return nil

	case int32:
		s.buf = binary.AppendVarint(s.buf, int64(v))
		return nil

	case uint32:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
		return nil

	case bool:
		if v {
			s.buf = append(s.buf, 1)
			return nil
		}
		s.buf = append(s.buf, 0)
		return nil

	case float64:
		s.buf = order.AppendUint64(s.buf, math.Float64bits(v))
		return nil

	case uint8:
		s.buf = append(s.buf, v)
		return nil

	case int8:
		s.buf = binary.AppendVarint(s.buf, int64(v))
		return nil

	case uint16:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))
		return nil

	case int16:
		s.buf = binary.AppendVarint(s.buf, int64(v))
		return nil

	case float32:
		s.buf = order.AppendUint32(s.buf, math.Float32bits(v))
		return nil

	case []string:
		str := strings.Join(v, ",")
		s.buf = append(s.buf, base.S2B(&str)...)
		return nil

	case time.Time:
		src, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
		return nil

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
		return nil
	}

	return errors.New("unsupported type: " + reflect.TypeOf(v).String())
}

// Decode
func (s *storeShard) Decode(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case *[]byte:
		*v = src
		return nil

	case *string:
		*v = *base.B2S(src)
		return nil

	case *int64:
		*v, _ = binary.Varint(src)
		return nil

	case *uint64:
		*v, _ = binary.Uvarint(src)
		return nil

	case *int32:
		num, _ := binary.Varint(src)
		*v = int32(num)
		return nil

	case *uint32:
		num, _ := binary.Uvarint(src)
		*v = uint32(num)
		return nil

	case *float64:
		*v = math.Float64frombits(order.Uint64(src))
		return nil

	case *bool:
		*v = src[0] != 0
		return nil

	case *uint:
		num, _ := binary.Uvarint(src)
		*v = uint(num)
		return nil

	case *int:
		num, _ := binary.Varint(src)
		*v = int(num)
		return nil

	case *uint8:
		*v = src[0]
		return nil

	case *int8:
		num, _ := binary.Varint(src)
		*v = int8(num)
		return nil

	case *uint16:
		num, _ := binary.Uvarint(src)
		*v = uint16(num)
		return nil

	case *int16:
		num, _ := binary.Varint(src)
		*v = int16(num)
		return nil

	case *float32:
		*v = math.Float32frombits(order.Uint32(src))
		return nil

	case *[]string:
		*v = strings.Split(*base.B2S(src), ",")
		return nil

	case *time.Time:
		return v.UnmarshalBinary(src)

	case base.Binarier:
		return v.UnmarshalBinary(src)

	case base.Marshaler:
		return v.UnmarshalJSON(src)

	default:
		return errors.New("unsupported type: " + reflect.TypeOf(v).String())
	}
}

func (s *storeShard) getStatus() Status {
	s.Lock()
	defer s.Unlock()

	return s.status
}

func (s *storeShard) setStatus(status Status) {
	s.Lock()
	defer s.Unlock()

	s.status = status
}
