package structx

import (
	"testing"
)

func TestZSet(t *testing.T) {
	t.Parallel()

	z := NewZSet[string, int, string]()

	// Test Set
	z.Set("a", "1")
	z.Set("b", "2")
	z.Set("c", "3")
	if z.Size() != 3 {
		t.Errorf("Size() = %d; want 3", z.Size())
	}

	// Test SetScore
	z.SetScore("d", 4)
	z.SetScore("e", 5)
	z.SetScore("f", 6)
	if z.Size() != 6 {
		t.Errorf("Size() = %d; want 6", z.Size())
	}

	// Test SetWithScore
	z.SetWithScore("g", 7, "7")
	z.SetWithScore("h", 8, "8")
	z.SetWithScore("i", 9, "9")
	if z.Size() != 9 {
		t.Errorf("Size() = %d; want 9", z.Size())
	}

	// Test Incr
	z.Incr("a", 1)
	z.Incr("b", 2)
	z.Incr("c", 3)
	if z.Size() != 9 {
		t.Errorf("Size() = %d; want 9", z.Size())
	}

	// Test Get
	v, s, ok := z.Get("a")
	if !ok || v != "1" || s != 1 {
		t.Errorf("Get() = (%v, %v, %v); want (%v, %v, %v)", v, s, ok, "1", 2, true)
	}

	// Test Delete
	v, ok = z.Delete("a")
	if !ok || v != "1" || z.Size() != 8 {
		t.Errorf("Delete() = (%v, %v); want (%v, %v)", v, ok, "1", true)
	}

	// Test Iter
	iter := z.Iter()
	for iter.Valid() {
		k := iter.Key()
		s := iter.Score()
		v, _, _ := z.Get(k)
		t.Logf("key=%v, score=%v, value=%v", k, s, v)
		iter.Next()
	}
}

func BenchmarkZSet(b *testing.B) {
	s := NewZSet[int, float64, any]()
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.Set(i, float64(i))
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.Get(i)
		}
	})

	s = NewZSet[int, float64, any]()
	b.Run("SetWithScore", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.SetWithScore(i, float64(i), "xgz")
		}
	})

	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.Delete(i)
		}
	})

	s = NewZSet[int, float64, any]()
	b.Run("Incr", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.Incr(i, float64(i))
		}
	})
}
