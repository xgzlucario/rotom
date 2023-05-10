package base

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	base = 36
)

type Coder struct {
	buf []byte
}

// NewCoder
func NewCoder(buf []byte) *Coder {
	return &Coder{buf}
}

// Enc
func (s *Coder) Enc(v ...byte) *Coder {
	s.buf = append(s.buf, v...)
	return s
}

func (s *Coder) encInt(v int) *Coder {
	str := strconv.FormatInt(int64(v), base)
	s.buf = append(s.buf, S2B(&str)...)
	return s
}

func (s *Coder) format(v []byte, sep byte) *Coder {
	s.encInt(len(v)).Enc(':').Enc(v...).Enc(sep)
	return s
}

// EncodeInt64
func (s *Coder) EncodeInt64(sep byte, v int64) *Coder {
	str := strconv.FormatInt(v, base)
	return s.format(S2B(&str), sep)
}

// EncodeBytes
func (s *Coder) EncodeBytes(sep byte, v ...byte) *Coder {
	return s.format(v, sep)
}

// Encode
func (s *Coder) Encode(v any, sep byte) error {
	buf, err := s.encode(v)
	if err != nil {
		return err
	}
	s.format(buf, sep)
	return nil
}

func (s *Coder) encode(v any) (buf []byte, err error) {
	switch v := v.(type) {
	case string:
		buf = append(buf, v...)
	case []byte:
		buf = append(buf, v...)
	case int64:
		buf = strconv.AppendInt(buf, v, base)
	case uint64:
		buf = strconv.AppendUint(buf, v, base)
	case int:
		buf = strconv.AppendInt(buf, int64(v), base)
	case uint:
		buf = strconv.AppendUint(buf, uint64(v), base)
	case int32:
		buf = strconv.AppendInt(buf, int64(v), base)
	case uint32:
		buf = strconv.AppendUint(buf, uint64(v), base)
	case bool:
		buf = strconv.AppendBool(buf, v)
	case float64:
		n := strconv.FormatFloat(v, 'f', -1, 64)
		buf = append(buf, n...)
	case uint8:
		buf = append(buf, v)
	case int8:
		buf = strconv.AppendInt(buf, int64(v), base)
	case uint16:
		buf = strconv.AppendUint(buf, uint64(v), base)
	case int16:
		buf = strconv.AppendInt(buf, int64(v), base)
	case float32:
		n := strconv.FormatFloat(float64(v), 'f', -1, 64)
		buf = append(buf, n...)
	case []string:
		str := strings.Join(v, ",")
		buf = append(buf, str...)
	case []int:
		length := len(v)
		for i, n := range v {
			buf = strconv.AppendInt(buf, int64(n), base)
			if i < length {
				buf = append(buf, ',')
			}
		}
	case time.Time:
		return v.MarshalBinary()
	case Binarier:
		return v.MarshalBinary()
	case Marshaler:
		return v.MarshalJSON()
	default:
		return nil, fmt.Errorf("%v: %v", ErrUnSupportDataType, reflect.TypeOf(v).String())
	}
	return
}

// Decode
func (s *Coder) Decode(src []byte, vptr interface{}) error {
	switch v := vptr.(type) {
	case *[]byte:
		*v = src
	case *string:
		*v = *B2S(src)
	case *int64:
		*v, _ = strconv.ParseInt(*B2S(src), base, 64)
	case *uint64:
		*v, _ = strconv.ParseUint(*B2S(src), base, 64)
	case *int32:
		num, _ := strconv.ParseInt(*B2S(src), base, 64)
		*v = int32(num)
	case *uint32:
		num, _ := strconv.ParseUint(*B2S(src), base, 64)
		*v = uint32(num)
	case *float64:
		*v, _ = strconv.ParseFloat(*B2S(src), 64)
	case *bool:
		*v, _ = strconv.ParseBool(*B2S(src))
	case *uint:
		num, _ := strconv.ParseUint(*B2S(src), base, 64)
		*v = uint(num)
	case *int:
		num, _ := strconv.ParseInt(*B2S(src), base, 64)
		*v = int(num)
	case *uint8:
		*v = src[0]
	case *int8:
		num, _ := strconv.ParseInt(*B2S(src), base, 64)
		*v = int8(num)
	case *uint16:
		num, _ := strconv.ParseUint(*B2S(src), base, 64)
		*v = uint16(num)
	case *int16:
		num, _ := strconv.ParseInt(*B2S(src), base, 64)
		*v = int16(num)
	case *float32:
		num, _ := strconv.ParseFloat(*B2S(src), 64)
		*v = float32(num)
	case *[]string:
		*v = strings.Split(*B2S(src), ",")
	case *[]int:
		strs := strings.Split(*B2S(src), ",")
		for _, str := range strs {
			num, _ := strconv.ParseInt(str, base, 64)
			*v = append(*v, int(num))
		}
	case *time.Time:
		return v.UnmarshalBinary(src)
	case Binarier:
		return v.UnmarshalBinary(src)
	case Marshaler:
		return v.UnmarshalJSON(src)
	default:
		return fmt.Errorf("%v: %v", ErrUnSupportDataType, reflect.TypeOf(v).String())
	}
	return nil
}

// WriteTo
func (s *Coder) WriteTo(path string) (int, error) {
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
