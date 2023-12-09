package codeman

import (
	"strings"
	"testing"
)

var (
	src = []byte(strings.Repeat("Hello World", 1000))
)

func BenchmarkCmpress(b *testing.B) {
	b.Run("compress", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Compress(src, make([]byte, 0))
		}
	})

	b.Run("compress1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Compress(src, make([]byte, 0, len(src)))
		}
	})

	b.Run("compress2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Compress(src, make([]byte, 0, len(src)/2))
		}
	})

	b.Run("compress3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Compress(src, make([]byte, 0, len(src)/4))
		}
	})
}
