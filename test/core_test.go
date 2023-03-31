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

func test1(src []byte) {
	if len(src) == 2 {
		src = nil
	}
}

func test2(src any) {
	switch src := src.(type) {
	case []byte:
		if len(src) == 2 {
			src = nil
		}
	case int:
	case int32:
	case int64:
	default:
	}
}

func test3(src any) {
	switch src := src.(type) {
	case int:
	case int32:
	case int64:
	case []byte:
		if len(src) == 2 {
			src = nil
		}
	default:
	}
}

func BenchmarkTest1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		test1(src)
	}
}

func BenchmarkTest2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		test2(src)
	}
}

func BenchmarkTest3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		test3(src)
	}
}
