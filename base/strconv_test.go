package base

import (
	"strconv"
	"testing"
)

func BenchmarkConv(b *testing.B) {
	num := int64(123456789)

	b.Run("std/10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatInt(num, 10)
		}
	})
	b.Run("std/36", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatInt(num, 36)
		}
	})
	b.Run("formatNumber", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatNumber(num)
		}
	})
}

func FuzzConv(f *testing.F) {
	f.Fuzz(func(t *testing.T, n1 int, n2 int64, n3 uint, n4 uint64) {
		// not support negative number
		if n1 < 0 || n2 < 0 {
			return
		}

		if r1 := ParseNumber[int](FormatNumber(n1)); r1 != n1 {
			t.Errorf("Expected %d, got %d", n1, r1)
		}

		if r2 := ParseNumber[int64](FormatNumber(n2)); r2 != n2 {
			t.Errorf("Expected %d, got %d", n2, r2)
		}

		if r3 := ParseNumber[uint](FormatNumber(n3)); r3 != n3 {
			t.Errorf("Expected %d, got %d", n3, r3)
		}

		if r4 := ParseNumber[uint64](FormatNumber(n4)); r4 != n4 {
			t.Errorf("Expected %d, got %d", n4, r4)
		}
	})
}
