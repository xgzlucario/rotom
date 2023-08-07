package base

import (
	"errors"
	"strings"
	"unsafe"
)

// String convert to bytes unsafe
func S2B(str *string) []byte {
	strHeader := (*[2]uintptr)(unsafe.Pointer(str))
	byteSliceHeader := [3]uintptr{
		strHeader[0], strHeader[1], strHeader[1],
	}
	return *(*[]byte)(unsafe.Pointer(&byteSliceHeader))
}

// Bytes convert to string unsafe
func B2S(buf []byte) *string {
	return (*string)(unsafe.Pointer(&buf))
}

// FormatInt
func FormatInt(n int64) string {
	if n >= 0 {
		return FormatUint(uint64(n))
	}
	return FormatUint(uint64(-n), true)
}

// FormatUint
func FormatUint(n uint64, negetive ...bool) string {
	if n == 0 {
		return "0"
	}

	var sb strings.Builder
	if len(negetive) > 0 {
		sb.WriteByte('-')
	}

	for n > 0 {
		sb.WriteByte(byte(n & 0xFF))
		n >>= 8
	}

	return sb.String()
}

// ParseInt
func ParseInt(s string) (int64, error) {
	if len(s) == 0 {
		return 0, errors.New("empty string")
	}

	isNegative := false
	if s[len(s)-1] == '-' {
		isNegative = true
		s = s[:len(s)-1]
	}

	var result int64 = 0
	var base int64 = 1

	for i := 0; i < len(s); i++ {
		result += int64(s[i]) * base
		base *= 256
	}

	if isNegative {
		result = -result
	}

	return result, nil
}

// ParseInt
func ParseUint(s string) (uint64, error) {
	if len(s) == 0 {
		return 0, errors.New("empty string")
	}

	var value uint64
	for i := len(s) - 1; i >= 0; i-- {
		value <<= 8
		value |= uint64(s[i])
	}

	return value, nil
}
