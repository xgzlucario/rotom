package test

import (
	"strconv"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkZSet(b *testing.B) {
	// initialize zset
	zset := structx.NewZSet[string, int64, int]()

	// insert data
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		score := int64(i)
		value := i
		zset.SetWithScore(key, score, value)
	}

	// benchmark Get
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			zset.Get(key)
		}
	})

	// benchmark Set
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			value := i
			zset.Set(key, value)
		}
	})

	// benchmark SetScore
	b.Run("SetScore", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			score := int64(i)
			zset.SetScore(key, score)
		}
	})

	// benchmark SetWithScore
	b.Run("SetWithScore", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			score := int64(i)
			value := i
			zset.SetWithScore(key, score, value)
		}
	})

	// benchmark Incr
	b.Run("Incr", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			score := int64(i)
			zset.Incr(key, score)
		}
	})

	// benchmark Delete
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			zset.Delete(key)
		}
	})

	// benchmark Iter
	b.Run("Iter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := zset.Iter()
			for iter.HasNext() {
				iter.Next()
			}
		}
	})
}

func BenchmarkRBTree(b *testing.B) {
	// initialize rbtree
	rbt := structx.NewRBTree[int64, string]()

	// insert data
	for i := 0; i < 1000; i++ {
		key := int64(i)
		value := strconv.Itoa(i)
		rbt.Insert(key, value)
	}

	// benchmark Get
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := int64(i % 1000)
			rbt.Find(key)
		}
	})

	// benchmark Insert
	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := int64(i % 1000)
			value := strconv.Itoa(i)
			rbt.Insert(key, value)
		}
	})

	// benchmark Delete
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := int64(i % 1000)
			rbt.Delete(key)
		}
	})

	// benchmark Iterator
	b.Run("Iterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := rbt.Iterator()
			for iter.Next() != nil {
				iter.Next()
			}
		}
	})
}

func BenchmarkMap(b *testing.B) {
	// initialize map
	m := make(map[string]int)

	// insert data
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		value := i
		m[key] = value
	}

	// benchmark Get
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			_, _ = m[key]
		}
	})

	// benchmark Set
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			value := i
			m[key] = value
		}
	})

	// benchmark Delete
	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := strconv.Itoa(i % 1000)
			delete(m, key)
		}
	})
}
