package dict

import (
	"runtime"
	"testing"

	"github.com/cockroachdb/swiss"
)

const N = 10000

func BenchmarkSet(b *testing.B) {
	b.Run("stdmap", func(b *testing.B) {
		m := make(map[string]any, 8)
		for i := 0; i < b.N; i++ {
			k, v := genKV(i)
			m[k] = v
		}
	})
	b.Run("swiss", func(b *testing.B) {
		m := swiss.New[string, any](8)
		for i := 0; i < b.N; i++ {
			k, v := genKV(i)
			m.Put(k, v)
		}
	})
}

func BenchmarkGC(b *testing.B) {
	b.Run("swiss", func(b *testing.B) {
		m := swiss.New[string, any](N)
		for i := 0; i < N; i++ {
			k, v := genKV(i)
			m.Put(k, v)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
	})
}
