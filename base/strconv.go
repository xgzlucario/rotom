package base

import (
	"encoding/binary"

	"github.com/bytedance/sonic"
)

const (
	VALID = 255
	RADIX = VALID - 1
)

// FormatInt
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

// ParseInt
func ParseInt[T Integer](b []byte) T {
	var n T
	for i := len(b) - 1; i >= 0; i-- {
		n = n*RADIX + T(b[i])
	}
	return n
}

// FormatStrSlice
func FormatStrSlice(ss []string) []byte {
	src, _ := sonic.Marshal(ss)
	return src
}

// ParseStrSlice
func ParseStrSlice(b []byte) []string {
	var ss []string
	sonic.Unmarshal(b, &ss)
	return ss
}

// FormatU32Slice
func FormatU32Slice(ss []uint32) []byte {
	bytes := make([]byte, 0, len(ss)*4)
	for _, s := range ss {
		bytes = binary.NativeEndian.AppendUint32(bytes, s)
	}
	return bytes
}

// ParseU32Slice
func ParseU32Slice(b []byte) []uint32 {
	ss := make([]uint32, 0, len(b)/4)
	for i := 0; i < len(b); i += 4 {
		ss = append(ss, binary.NativeEndian.Uint32(b[i:]))
	}
	return ss
}
