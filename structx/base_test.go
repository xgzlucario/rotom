package structx

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/cespare/xxhash"
	"github.com/xgzlucario/rotom/base"
	"github.com/zeebo/xxh3"
)

var (
	str = gofakeit.Name()
	bt  = []byte(str)
)

func BenchmarkXXHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		xxhash.Sum64(bt)
	}
}

func BenchmarkXXH3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		xxh3.Hash(bt)
	}
}

func BenchmarkStr1(b *testing.B) {
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = []byte(str)
	}
	if len(buf) > 0 {
		buf = nil
	}
}

func BenchmarkStr2(b *testing.B) {
	var buf []byte
	for i := 0; i < b.N; i++ {
		base.S2B(&str)
	}
	if len(buf) > 0 {
		buf = nil
	}
}

func BenchmarkBytes1(b *testing.B) {
	var str string
	for i := 0; i < b.N; i++ {
		str = string(bt)
	}
	if len(str) > 0 {
		str = ""
	}
}

func BenchmarkBytes2(b *testing.B) {
	var str string
	for i := 0; i < b.N; i++ {
		str = *base.B2S(bt)
	}
	if len(str) > 0 {
		str = ""
	}
}
