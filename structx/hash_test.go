package structx

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/cespare/xxhash"
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
