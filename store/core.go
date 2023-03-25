package store

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

const (
	// start with `1`
	OP_SET byte = iota + 49
	OP_SET_WITH_TTL
	OP_REMOVE
	OP_PERSIST

	spr     = '|'
	endChar = '#'

	// integer carry
	carry = 36
)

var (
	// seperate char
	lineSpr = []byte("#\n")
)

func (s *storeShard) load() {
	s.Lock()
	defer s.Unlock()

	data, err := os.ReadFile(s.storePath)
	if err != nil {
		return
	}

	// reset filter
	s.filter = structx.NewBloom()

	lines := bytes.Split(data, []byte{'\n'})
	// read line from tail
	for i := len(lines) - 1; i >= 0; i-- {
		s.readLine(lines[i])
	}

	// rewrite
	if s.rwBuffer.Len() > 0 {
		fs, err := os.OpenFile(s.rewritePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		defer fs.Close()

		s.rwBuffer.WriteTo(fs)

		// rename dat.rw to dat
		if err := os.Rename(s.storePath, s.storePath+".bak"); err != nil {
			panic(err)
		}
		if err := os.Rename(s.rewritePath, s.storePath); err != nil {
			panic(err)
		}
		if err := os.Remove(s.storePath + ".bak"); err != nil {
			panic(err)
		}
		os.Remove(s.rewritePath)
	}
}

// WriteBuffer
func (s *storeShard) WriteBuffer() (int64, error) {
	s.Lock()
	defer s.Unlock()

	if s.buffer.Len() == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(s.storePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		s.logger.Println("WriteBuffer", err)
		return -1, err
	}

	defer fs.Close()

	return s.buffer.WriteTo(fs)
}

// ReWriteBuffer
func (s *storeShard) ReWriteBuffer() (int64, error) {
	s.Lock()
	defer s.Unlock()

	if s.rwBuffer.Len() == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(s.rewritePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		s.logger.Println("ReWriteBuffer", err)
		return -1, err
	}

	defer fs.Close()

	return s.rwBuffer.WriteTo(fs)
}

// read line
func (s *storeShard) readLine(line []byte) {
	// line valid
	if len(line) == 0 {
		return
	}
	if line[len(line)-1] != endChar {
		return
	}
	line = line[:len(line)-1]

	switch line[0] {
	// SET: {op}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, spr)
		if i <= 0 {
			return
		}

		if !s.testAndAdd(line[:i], []byte{OP_SET_WITH_TTL, OP_REMOVE}) {
			return
		}

		line[0] = OP_SET
		s.rwBuffer.Write(line)
		s.rwBuffer.Write(lineSpr)

		s.Set(*base.B2S(line[1:i]), line[i+1:])

	// SET_WITH_TTL: {op}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		var sp1, sp2 int
		for i, c := range line {
			if c == spr {
				if sp1 == 0 {
					sp1 = i

				} else {
					sp2 = i
					break
				}
			}
		}

		if !s.testAndAdd(line[:sp1], []byte{OP_SET, OP_REMOVE}) {
			return
		}

		ttl, _ := strconv.ParseInt(*base.B2S(line[sp1+1 : sp2]), 10, 0)
		// not expired
		if ttl > globalTime {
			line[0] = OP_SET_WITH_TTL
			s.rwBuffer.Write(line)
			s.rwBuffer.Write(lineSpr)

			s.SetWithTTL(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), time.Duration(ttl))
		}

	// REMOVE: {op}{key}
	// 删除操作不需要重写，重写后的日志中应仅包含 Set, SetWithTTL, Persist 操作
	case OP_REMOVE:
		if !s.filter.TestAndAdd(line) {
			return
		}
		s.Remove(*base.B2S(line[1:]))

	// PERSIST: {op}{key}
	case OP_PERSIST:
		if !s.testAndAdd(line, []byte{OP_SET, OP_REMOVE}) {
			return
		}
		line[0] = OP_PERSIST
		s.rwBuffer.Write(line)
		s.rwBuffer.Write(lineSpr)

		s.Persist(*base.B2S(line[1:]))
	}
}

// testAndAdd check if a given line already exists in a bloom filter and if not, to add it to the filter.
// The method also checks if any of the ops can be applied to the line without causing a match in the filter.
func (s *storeShard) testAndAdd(line []byte, ops []byte) bool {
	if s.filter.TestAndAdd(line) {
		return false
	}
	for _, b := range ops {
		line[0] = b
		if s.filter.Test(line) {
			return false
		}
	}

	return true
}

// EncodeValue
func (s *storeShard) EncodeValue(v any) ([]byte, error) {
	switch v := v.(type) {
	case base.Marshaler:
		return v.MarshalJSON()

	case base.Stringer:
		str := v.String()
		return base.S2B(&str), nil

	case []byte:
		return v, nil

	case string:
		return base.S2B(&v), nil

	case uint:
		str := strconv.FormatUint(uint64(v), carry)
		return base.S2B(&str), nil

	case uint8:
		str := strconv.FormatUint(uint64(v), carry)
		return base.S2B(&str), nil

	case uint16:
		str := strconv.FormatUint(uint64(v), carry)
		return base.S2B(&str), nil

	case uint32:
		str := strconv.FormatUint(uint64(v), carry)
		return base.S2B(&str), nil

	case uint64:
		str := strconv.FormatUint(v, carry)
		return base.S2B(&str), nil

	case int:
		str := strconv.FormatInt(int64(v), carry)
		return base.S2B(&str), nil

	case int8:
		str := strconv.FormatInt(int64(v), carry)
		return base.S2B(&str), nil

	case int16:
		str := strconv.FormatInt(int64(v), carry)
		return base.S2B(&str), nil

	case int32:
		str := strconv.FormatInt(int64(v), carry)
		return base.S2B(&str), nil

	case int64:
		str := strconv.FormatInt(v, carry)
		return base.S2B(&str), nil

	case float32:
		str := strconv.FormatFloat(float64(v), 'f', -1, 64)
		return base.S2B(&str), nil

	case float64:
		str := strconv.FormatFloat(v, 'f', -1, 64)
		return base.S2B(&str), nil

	case bool:
		if v {
			return []byte{'T'}, nil
		}
		return []byte{'F'}, nil

	case []string:
		str := strings.Join(v, ",")
		return base.S2B(&str), nil

	default:
		return nil, errors.New("unsupported type: " + reflect.TypeOf(v).String())
	}
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
		num, err := strconv.ParseUint(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = uint(num)

	case *uint8:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 8)
		if err != nil {
			return err
		}
		*v = uint8(num)

	case *uint16:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 16)
		if err != nil {
			return err
		}
		*v = uint16(num)

	case *uint32:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 32)
		if err != nil {
			return err
		}
		*v = uint32(num)

	case *uint64:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = num

	case *int:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = int(num)

	case *int8:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 8)
		if err != nil {
			return err
		}
		*v = int8(num)

	case *int16:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 16)
		if err != nil {
			return err
		}
		*v = int16(num)

	case *int32:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 32)
		if err != nil {
			return err
		}
		*v = int32(num)

	case *int64:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 32)
		if err != nil {
			return err
		}
		*v = num

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
