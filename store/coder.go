package store

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xgzlucario/rotom/base"
)

const (
	_base = 36
)

type Coder struct {
	buf []byte
	err error
}

func NewCoder(v Operation) *Coder {
	return &Coder{[]byte{byte(v)}, nil}
}

func (s *Coder) String(v string) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, ':')
	s.buf = append(s.buf, v...)
	s.End()
	return s
}

func (s *Coder) End() *Coder {
	s.buf = append(s.buf, recordSepChar)
	return s
}

func (s *Coder) int(v int) *Coder {
	str := strconv.FormatInt(int64(v), _base)
	s.buf = append(s.buf, str...)
	return s
}

func (s *Coder) format(v []byte) *Coder {
	s.int(len(v))
	s.buf = append(s.buf, ':')
	s.buf = append(s.buf, v...)
	s.End()
	return s
}

func (s *Coder) Int64(v int64) *Coder {
	str := strconv.FormatInt(v, _base)
	return s.format(base.S2B(&str))
}

func (s *Coder) Any(v any) *Coder {
	buf, err := s.encode(v)
	if err != nil {
		s.err = err
	}
	s.format(buf)
	return s
}

func (s *Coder) encode(v any) (buf []byte, err error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case int64:
		buf = strconv.AppendInt(buf, v, _base)
	case uint64:
		buf = strconv.AppendUint(buf, v, _base)
	case int:
		buf = strconv.AppendInt(buf, int64(v), _base)
	case uint:
		buf = strconv.AppendUint(buf, uint64(v), _base)
	case int32:
		buf = strconv.AppendInt(buf, int64(v), _base)
	case uint32:
		buf = strconv.AppendUint(buf, uint64(v), _base)
	case bool:
		buf = strconv.AppendBool(buf, v)
	case float64:
		n := strconv.FormatFloat(v, 'f', -1, 64)
		buf = append(buf, n...)
	case uint8:
		buf = append(buf, v)
	case int8:
		buf = strconv.AppendInt(buf, int64(v), _base)
	case uint16:
		buf = strconv.AppendUint(buf, uint64(v), _base)
	case int16:
		buf = strconv.AppendInt(buf, int64(v), _base)
	case float32:
		n := strconv.FormatFloat(float64(v), 'f', -1, 64)
		buf = append(buf, n...)
	case []string:
		str := strings.Join(v, ",")
		buf = append(buf, str...)
	case []int:
		length := len(v)
		for i, n := range v {
			buf = strconv.AppendInt(buf, int64(n), _base)
			if i < length {
				buf = append(buf, ',')
			}
		}
	case time.Time:
		return v.MarshalBinary()
	case base.Binarier:
		return v.MarshalBinary()
	case base.Marshaler:
		return v.MarshalJSON()
	default:
		return nil, fmt.Errorf("%v: %v", base.ErrUnSupportDataType, reflect.TypeOf(v).String())
	}
	return
}

func decode(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case *[]byte:
		*v = src
	case *string:
		*v = *base.B2S(src)
	case *int64:
		*v, _ = strconv.ParseInt(*base.B2S(src), _base, 64)
	case *uint64:
		*v, _ = strconv.ParseUint(*base.B2S(src), _base, 64)
	case *int32:
		num, _ := strconv.ParseInt(*base.B2S(src), _base, 64)
		*v = int32(num)
	case *uint32:
		num, _ := strconv.ParseUint(*base.B2S(src), _base, 64)
		*v = uint32(num)
	case *float64:
		*v, _ = strconv.ParseFloat(*base.B2S(src), 64)
	case *bool:
		*v, _ = strconv.ParseBool(*base.B2S(src))
	case *uint:
		num, _ := strconv.ParseUint(*base.B2S(src), _base, 64)
		*v = uint(num)
	case *int:
		num, _ := strconv.ParseInt(*base.B2S(src), _base, 64)
		*v = int(num)
	case *uint8:
		*v = src[0]
	case *int8:
		num, _ := strconv.ParseInt(*base.B2S(src), _base, 64)
		*v = int8(num)
	case *uint16:
		num, _ := strconv.ParseUint(*base.B2S(src), _base, 64)
		*v = uint16(num)
	case *int16:
		num, _ := strconv.ParseInt(*base.B2S(src), _base, 64)
		*v = int16(num)
	case *float32:
		num, _ := strconv.ParseFloat(*base.B2S(src), 64)
		*v = float32(num)
	case *[]string:
		*v = strings.Split(*base.B2S(src), ",")
	case *[]int:
		strs := strings.Split(*base.B2S(src), ",")
		for _, str := range strs {
			num, _ := strconv.ParseInt(str, _base, 64)
			*v = append(*v, int(num))
		}
	case *time.Time:
		return v.UnmarshalBinary(src)
	case base.Binarier:
		return v.UnmarshalBinary(src)
	case base.Marshaler:
		return v.UnmarshalJSON(src)
	default:
		return fmt.Errorf("%v: %v", base.ErrUnSupportDataType, reflect.TypeOf(v).String())
	}
	return nil
}
