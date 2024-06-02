package main

import (
	"testing"

	"github.com/cockroachdb/swiss"
)

func BenchmarkMap(b *testing.B) {
	const N = 100

	b.Run("stdmap/set", func(b *testing.B) {
		m := make(map[int]int, N)
		for i := 0; i < b.N; i++ {
			m[i%N] = i % N
		}
	})
	b.Run("swiss/set", func(b *testing.B) {
		m := swiss.New[int, int](N)
		for i := 0; i < b.N; i++ {
			m.Put(i%N, i%N)
		}
	})

	b.Run("stdmap/get", func(b *testing.B) {
		m := make(map[int]int, N)
		for i := 0; i < N; i++ {
			m[i] = i
		}
		for i := 0; i < b.N; i++ {
			_ = m[i%N]
		}
	})
	b.Run("swiss/get", func(b *testing.B) {
		m := swiss.New[int, int](N)
		for i := 0; i < N; i++ {
			m.Put(i, i)
		}
		for i := 0; i < b.N; i++ {
			m.Get(i % N)
		}
	})
}
