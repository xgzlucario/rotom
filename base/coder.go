package base

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"time"
)

var order = binary.BigEndian

type Coder struct {
	buf []byte
}

// NewCoder
func NewCoder(buf []byte) *Coder {
	return &Coder{buf}
}

// EncBytes
func (s *Coder) EncBytes(v ...byte) *Coder {
	s.buf = append(s.buf, v...)
	return s
}

// EncInt64
func (s *Coder) EncInt64(v int64) *Coder {
	s.buf = binary.AppendVarint(s.buf, v)
	return s
}

// Encode
func (s *Coder) Encode(v any) error {
	switch v := v.(type) {
	case string:
		s.buf = append(s.buf, S2B(&v)...)
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
		s.buf = append(s.buf, S2B(&str)...)
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
	case Binarier:
		src, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
	case Marshaler:
		src, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		s.buf = append(s.buf, src...)
	default:
		return fmt.Errorf("%v: %v", ErrUnSupportType, reflect.TypeOf(v).String())
	}
	return nil
}

// Decode
func (s *Coder) Decode(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case *[]byte:
		*v = src
	case *string:
		*v = *B2S(src)
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
		*v = strings.Split(*B2S(src), ",")
	case *[]int:
		*v = make([]int, 0)
		for len(src) > 0 {
			num, n := binary.Varint(src)
			src = src[n:]
			*v = append(*v, int(num))
		}
	case *time.Time:
		return v.UnmarshalBinary(src)
	case Binarier:
		return v.UnmarshalBinary(src)
	case Marshaler:
		return v.UnmarshalJSON(src)
	default:
		return fmt.Errorf("%v: %v", ErrUnSupportType, reflect.TypeOf(v).String())
	}
	return nil
}

// FlushFile
func (s *Coder) FlushFile(path string) (int, error) {
	if len(s.buf) == 0 {
		return 0, nil
	}

	fs, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fs.Close()

	// write
	n, err := fs.Write(s.buf)
	if err != nil {
		return 0, err
	}

	// reset
	s.buf = s.buf[0:0]

	return n, nil
}

// ZstdEncode
func (s *Coder) ZstdEncode() {
	s.buf = encoder.EncodeAll(s.buf, nil)
}
