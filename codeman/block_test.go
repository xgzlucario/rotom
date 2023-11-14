package codeman

import (
	"bytes"
	"testing"
)

func BenchmarkCompress(b *testing.B) {
	src := bytes.Repeat([]byte("hello world"), 1000)

	b.Run("zstd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.EncodeAll(src, nil)
		}
	})

	b.Run("zstd-alloc1", func(b *testing.B) {
		dst := make([]byte, 0, len(src))
		for i := 0; i < b.N; i++ {
			encoder.EncodeAll(src, dst)
		}
	})

	b.Run("zstd-alloc2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dst := make([]byte, 0, len(src))
			encoder.EncodeAll(src, dst)
		}
	})
}
