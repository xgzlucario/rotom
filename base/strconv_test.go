package base

import (
	"strconv"
	"testing"
	"time"
)

func TestStrconv(t *testing.T) {
	n := time.Now().UnixNano()
	un := uint64(n)

	s := FormatInt(n)
	if res, err := ParseInt(s); err != nil || res != n {
		t.Fatal("ParseInt failed")
	}

	s = FormatUint(un)
	if res, err := ParseUint(s); err != nil || res != un {
		t.Fatal("ParseUint failed")
	}
}

func BenchmarkStrconv(b *testing.B) {
	n := time.Now().Unix()
	un := uint64(n)

	b.Run("FormatInt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatInt(n)
		}
	})
	b.Run("std/FormatInt_10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatInt(n, 10)
		}
	})
	b.Run("std/FormatInt_36", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatInt(n, 36)
		}
	})
	b.Run("FormatUint", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatUint(un)
		}
	})
	b.Run("std/FormatUint_10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatUint(un, 10)
		}
	})
	b.Run("std/FormatUint_36", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			strconv.FormatUint(un, 36)
		}
	})
}
