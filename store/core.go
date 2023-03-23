package store

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

const (
	OP_SET          = '1'
	OP_SET_WITH_TTL = '2'
	OP_REMOVE       = '3'
	OP_PERSIST      = '4'

	spr = '|'
)

var (
	// seperate char
	lineSpr = []byte("\n")
	blkSpr  = []byte("[BLK]")
)

func (s *storeShard) load() {
	s.Lock()
	defer s.Unlock()

	// open file
	fs, err := os.ReadFile(s.storePath)
	if err != nil {
		return
	}

	// reset filter
	s.filter = structx.NewBloom()

	blks := bytes.Split(fs, blkSpr)

	// read block from tail
	for i := len(blks) - 1; i >= 0; i-- {
		// decompress
		// blks[i], err = base.ZstdDecode(blks[i])
		// if err != nil {
		// 	fmt.Println("=====================")
		// 	continue
		// }

		lines := bytes.Split(blks[i], lineSpr)

		// read line from tail
		for j := len(lines) - 1; j >= 0; j-- {
			s.readLine(lines[j])
		}
	}
}

func newWriter(path string) *os.File {
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return writer
}

// write buffer block
func (s *storeShard) writeBuffer() error {
	s.Lock()
	defer s.Unlock()

	if len(s.buffer) == 0 {
		return nil
	}

	// write
	s.buffer = append(s.buffer, blkSpr...)
	if _, err := s.rw.Write(s.buffer); err != nil {
		return err
	}

	// reset
	s.buffer = s.buffer[0:0]
	return nil
}

// read line
func (s *storeShard) readLine(line []byte) {
	if len(line) == 0 {
		return
	}

	switch line[0] {
	// SET: {op}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, spr)

		// test
		if !s.testAndAdd(line[:i], []byte{OP_SET_WITH_TTL, OP_REMOVE}) {
			return
		}
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
		if sp2 <= sp1 {
			panic(errors.New("sp2 < sp1"))
		}

		// test
		if !s.testAndAdd(line[:sp1], []byte{OP_SET, OP_REMOVE}) {
			return
		}

		ttl, _ := strconv.ParseInt(*base.B2S(line[sp1+1 : sp2]), 10, 0)
		// not expired
		if ttl > globalTime {
			s.SetWithTTL(*base.B2S(line[1:sp1]), *base.B2S(line[sp2+1:]), time.Duration(ttl))
		}

	// REMOVE: {op}{key}
	case OP_REMOVE:
		// test
		if !s.testAndAdd(line, []byte{OP_SET, OP_SET_WITH_TTL}) {
			return
		}
		s.Remove(*base.B2S(line[1:]))

	// PERSIST: {op}{key}
	case OP_PERSIST:
		// test
		if !s.testAndAdd(line, []byte{OP_SET, OP_REMOVE}) {
			return
		}
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

const carry = 36

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
		str := strconv.FormatBool(v)
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
		return nil

	case *[]byte:
		*v = src
		return nil

	case *uint:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = uint(num)
		return nil

	case *uint8:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 8)
		if err != nil {
			return err
		}
		*v = uint8(num)
		return nil

	case *uint16:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 16)
		if err != nil {
			return err
		}
		*v = uint16(num)
		return nil

	case *uint32:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 32)
		if err != nil {
			return err
		}
		*v = uint32(num)
		return nil

	case *uint64:
		num, err := strconv.ParseUint(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = num
		return nil

	case *int:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 64)
		if err != nil {
			return err
		}
		*v = int(num)
		return nil

	case *int8:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 8)
		if err != nil {
			return err
		}
		*v = int8(num)
		return nil

	case *int16:
		num, err := strconv.ParseInt(*base.B2S(src), carry, 16)
		if err != nil {
			return err
		}
		*v = int16(num)
		return nil

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

	default:
		return errors.New("unsupported type: " + reflect.TypeOf(v).String())
	}
	return nil
}
