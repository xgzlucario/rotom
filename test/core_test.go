package test

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/xgzlucario/rotom/base"
)

const (
	num    = uint64(1234567890)
	numStr = "1234567890"
)

var src = []byte{210, 133, 216, 204, 4} // binary data of 1234567890

func BenchmarkEncodeBinary(b *testing.B) {
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = binary.AppendUvarint(buf, num)
	}
}

func BenchmarkDecodeBinary(b *testing.B) {
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = binary.Uvarint(src)
	}
	if n > 0 {
		n = 0
	}
}

func BenchmarkEncodeString(b *testing.B) {
	var buf []byte
	for i := 0; i < b.N; i++ {
		str := strconv.FormatUint(num, 36)
		buf = append(buf, base.S2B(&str)...)
	}
	if len(buf) > 0 {
		buf = nil
	}
}

func BenchmarkDecodeString(b *testing.B) {
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = strconv.ParseUint(numStr, 10, 64)
	}
	if n > 0 {
		n = 0
	}
}
