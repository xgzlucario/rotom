package base

import (
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

const (
	VALID = 255
	RADIX = VALID - 1
)

func FormatInt[T Integer](n T) []byte {
	if n < 0 {
		panic("negative number")
	}
	if n == 0 {
		return []byte{0}
	}

	sb := make([]byte, 0, 1)
	for n > 0 {
		sb = append(sb, byte(n%RADIX))
		n /= RADIX
	}

	return sb
}

func ParseInt[T Integer](b []byte) T {
	var n T
	for i := len(b) - 1; i >= 0; i-- {
		n = n*RADIX + T(b[i])
	}
	return n
}
