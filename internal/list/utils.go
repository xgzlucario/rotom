package list

import (
	"encoding/binary"
	"math/bits"
	"slices"
)

func appendUvarint(b []byte, n int, reverse bool) []byte {
	if !reverse {
		return binary.AppendUvarint(b, uint64(n))
	}
	before := len(b)
	b = binary.AppendUvarint(b, uint64(n))
	after := len(b)
	if after-before > 1 {
		slices.Reverse(b[before:])
	}
	return b
}

// uvarintReverse is the reverse version from binary.Uvarint.
func uvarintReverse(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i := range buf {
		b := buf[len(buf)-1-i]
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

// SizeUvarint
// See https://go-review.googlesource.com/c/go/+/572196/1/src/encoding/binary/varint.go#174
func SizeUvarint(x uint64) int {
	return int(9*uint32(bits.Len64(x))+64) / 64
}
