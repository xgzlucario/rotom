package dict

import (
	"runtime"
	"testing"

	"github.com/cockroachdb/swiss"
)

const N = 100 * 10000

func getStdmap(n int) map[string]int {
	m := make(map[string]int, n)
	for i := 0; i < n; i++ {
		k, _ := genKV(i)
		m[k] = i
	}
	return m
}

func getSwiss(n int) *swiss.Map[string, int] {
	m := swiss.New[string, int](n)
	for i := 0; i < n; i++ {
		k, _ := genKV(i)
		m.Put(k, i)
	}
	return m
}

func BenchmarkSet(b *testing.B) {
	b.Run("stdmap", func(b *testing.B) {
		m := make(map[string]int, 8)
		for i := 0; i < b.N; i++ {
			k, _ := genKV(i)
			m[k] = i
		}
	})
	b.Run("swiss", func(b *testing.B) {
		m := swiss.New[string, int](8)
		for i := 0; i < b.N; i++ {
			k, _ := genKV(i)
			m.Put(k, i)
		}
	})
}

func BenchmarkGet(b *testing.B) {
	b.Run("stdmap", func(b *testing.B) {
		m := getStdmap(N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k, _ := genKV(i)
			_ = m[k]
		}
	})
	b.Run("swiss", func(b *testing.B) {
		m := getSwiss(N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k, _ := genKV(i)
			m.Get(k)
		}
	})
}

func BenchmarkGC(b *testing.B) {
	b.Run("swiss1", func(b *testing.B) {
		m := swiss.New[string, int](N)
		data := make([]byte, 0)
		for i := 0; i < N; i++ {
			k, v := genKV(i)
			m.Put(k, i)
			data = append(data, v...)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
		m.Put("xgz", len(data))
	})

	b.Run("swiss2", func(b *testing.B) {
		m := swiss.New[string, []byte](N)
		for i := 0; i < N; i++ {
			k, v := genKV(i)
			m.Put(k, v)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
		m.Put("xgz", []byte("hello"))
	})
}
