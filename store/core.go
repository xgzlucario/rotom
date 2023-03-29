package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

const (
	// 操作码
	// 从 `1` 开始, 以便于在文件中直接使用 `1` 表示 `OP_SET`
	// 数据行: 代表一条数据, 如 `1xgz|22#\n` 代表 `SET xgz 22`
	OP_SET byte = iota + 49
	OP_SET_WITH_TTL
	OP_REMOVE
	OP_PERSIST

	// 分隔符, 用于分隔数据行中的不同字段
	sprChar = byte(0)

	// 校验符, 用于校验数据行是否完整
	// 判断一条数据是否完整, 只需判断最后一个字符是否为 validChar 即可
	validChar = byte(0)

	// 换行符, 用于表示数据行的结尾
	endChar = '\n'
)

var (
	// 行尾, 起到换行及校验的作用
	// 不同数据行通过行尾分隔
	lineSpr = []byte{validChar, validChar, endChar}
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
	lines := bytes.Split(data, []byte{validChar, endChar})
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

	fs, err := os.OpenFile(s.storePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer fs.Close()

	// write file
	n, err := fs.Write(s.buf)
	if err != nil {
		return 0, err
	}

	s.buf = s.buf[0:0]
	return n, nil
}

// ReWriteBuffer
func (s *storeShard) ReWriteBuffer() (int, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.buf) == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(s.rwPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer fs.Close()

	// write file
	n, err := fs.Write(s.buf)
	if err != nil {
		return 0, err
	}

	s.buf = s.buf[0:0]
	return n, nil
}

// read line
func (s *storeShard) readLine(line []byte) {
	// line valid
	if !bytes.HasSuffix(line, []byte{validChar}) {
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

// encodeByte without asserting
func (s *storeShard) encodeBytes(v ...byte) {
	s.buf = append(s.buf, v...)
}

// EncodeValue encode value to store shard buffer
func (s *storeShard) EncodeValue(v any) error {
	switch v := v.(type) {
	case base.Marshaler:
		src, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)

	case base.Stringer:
		str := v.String()
		s.buf = append(s.buf, base.S2B(&str)...)

	case []byte:
		s.buf = append(s.buf, v...)

	case string:
		s.buf = append(s.buf, base.S2B(&v)...)

	case uint:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))

	case uint8:
		s.buf = append(s.buf, v)

	case uint16:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))

	case uint32:
		s.buf = binary.AppendUvarint(s.buf, uint64(v))

	case uint64:
		s.buf = binary.AppendUvarint(s.buf, v)

	case int:
		s.buf = binary.AppendVarint(s.buf, int64(v))

	case int8:
		s.buf = binary.AppendVarint(s.buf, int64(v))

	case int16:
		s.buf = binary.AppendVarint(s.buf, int64(v))

	case int32:
		s.buf = binary.AppendVarint(s.buf, int64(v))

	case int64:
		s.buf = binary.AppendVarint(s.buf, v)

	case float32:
		str := strconv.FormatFloat(float64(v), 'f', -1, 64)
		s.buf = append(s.buf, base.S2B(&str)...)

	case float64:
		str := strconv.FormatFloat(v, 'f', -1, 64)
		s.buf = append(s.buf, base.S2B(&str)...)

	case bool:
		if v {
			s.buf = append(s.buf, 'T')
		} else {
			s.buf = append(s.buf, 'F')
		}

	case []string:
		str := strings.Join(v, ",")
		s.buf = append(s.buf, base.S2B(&str)...)
	}

	return errors.New("unsupported type: " + reflect.TypeOf(v).String())
}

// DecodeValue
func (s *storeShard) DecodeValue(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case base.Marshaler:
		return v.UnmarshalJSON(src)

	case *string:
		*v = *base.B2S(src)

	case *[]byte:
		*v = src

	case *uint:
		num, _ := binary.Uvarint(src)
		*v = uint(num)

	case *uint8:
		*v = src[0]

	case *uint16:
		num, _ := binary.Uvarint(src)
		*v = uint16(num)

	case *uint32:
		num, _ := binary.Uvarint(src)
		*v = uint32(num)

	case *uint64:
		num, _ := binary.Uvarint(src)
		*v = num

	case *int:
		num, _ := binary.Varint(src)
		*v = int(num)

	case *int8:
		num, _ := binary.Varint(src)
		*v = int8(num)

	case *int16:
		num, _ := binary.Varint(src)
		*v = int16(num)

	case *int32:
		num, _ := binary.Varint(src)
		*v = int32(num)

	case *int64:
		*v, _ = binary.Varint(src)

	case *float32:
		num, err := strconv.ParseFloat(*base.B2S(src), 32)
		if err != nil {
			return err
		}
		*v = float32(num)

	case *float64:
		num, err := strconv.ParseFloat(*base.B2S(src), 64)
		if err != nil {
			return err
		}
		*v = num

	case *bool:
		val, err := strconv.ParseBool(*base.B2S(src))
		if err != nil {
			return err
		}
		*v = val

	case *[]string:
		*v = strings.Split(*base.B2S(src), ",")

	default:
		return errors.New("unsupported type: " + reflect.TypeOf(v).String())
	}
	return nil
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
