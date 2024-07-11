package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func genList(start, end int) *QuickList {
	lp := New()
	for i := start; i < end; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func TestList(t *testing.T) {
	const N = 1000
	assert := assert.New(t)
	SetMaxListPackSize(128)

	t.Run("rpush", func(t *testing.T) {
		ls := New()
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), i)
			ls.RPush(genKey(i))
		}
		for i := 0; i < N; i++ {
			v, ok := ls.Index(i)
			assert.Equal(genKey(i), v)
			assert.Equal(true, ok)
		}
		// check each node length
		for cur := ls.head; cur != nil; cur = cur.next {
			assert.LessOrEqual(len(cur.data), maxListPackSize)
		}
	})

	t.Run("lpush", func(t *testing.T) {
		ls := New()
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), i)
			ls.LPush(genKey(i))
		}
		for i := 0; i < N; i++ {
			v, ok := ls.Index(N - 1 - i)
			assert.Equal(genKey(i), v)
			assert.Equal(true, ok)
		}
		// check each node length
		for cur := ls.head; cur != nil; cur = cur.next {
			assert.LessOrEqual(len(cur.data), maxListPackSize)
		}
	})

	t.Run("lpop", func(t *testing.T) {
		ls := genList(0, N)
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), N-i)
			key, ok := ls.LPop()
			assert.Equal(key, genKey(i))
			assert.Equal(true, ok)
		}
		// pop empty list
		key, ok := ls.LPop()
		assert.Equal(key, "")
		assert.Equal(false, ok)
	})

	t.Run("rpop", func(t *testing.T) {
		ls := genList(0, N)
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), N-i)
			key, ok := ls.RPop()
			assert.Equal(key, genKey(N-i-1))
			assert.Equal(true, ok)
		}
		// pop empty list
		key, ok := ls.RPop()
		assert.Equal(key, "")
		assert.Equal(false, ok)
	})

	t.Run("len", func(t *testing.T) {
		ls := New()
		for i := 0; i < N; i++ {
			ls.RPush(genKey(i))
			assert.Equal(ls.Size(), i+1)
		}
	})

	t.Run("range", func(t *testing.T) {
		ls := New()
		ls.Range(1, 2, func(s []byte) bool {
			panic("should not call")
		})
		ls = genList(0, N)

		var count int
		ls.Range(0, -1, func(s []byte) bool {
			assert.Equal(string(s), genKey(count))
			count++
			return false
		})
		assert.Equal(count, N)

		ls.Range(1, 1, func(s []byte) bool {
			panic("should not call")
		})
		ls.Range(-1, -1, func(s []byte) bool {
			panic("should not call")
		})
	})

	t.Run("revrange", func(t *testing.T) {
		ls := New()
		ls.RevRange(1, 2, func(s []byte) bool {
			panic("should not call")
		})
		ls = genList(0, N)

		var count int
		ls.RevRange(0, -1, func(s []byte) bool {
			assert.Equal(string(s), genKey(N-count-1))
			count++
			return false
		})
		assert.Equal(count, N)

		ls.RevRange(1, 1, func(s []byte) bool {
			panic("should not call")
		})
		ls.RevRange(-1, -1, func(s []byte) bool {
			panic("should not call")
		})
	})
}
