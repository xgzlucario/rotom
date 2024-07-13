package list

import (
	"slices"
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

func list2slice(ls *QuickList) (res []string) {
	ls.Range(0, -1, func(data []byte) {
		res = append(res, string(data))
	})
	return
}

func TestList(t *testing.T) {
	const N = 10000
	assert := assert.New(t)

	t.Run("lpush", func(t *testing.T) {
		ls := New()
		ls2 := make([]string, 0, N)
		for i := 0; i < N; i++ {
			key := genKey(i)
			ls.LPush(key)
			ls2 = slices.Insert(ls2, 0, key)
		}
		assert.Equal(ls.Size(), len(ls2))
		assert.Equal(list2slice(ls), ls2)
	})

	t.Run("rpush", func(t *testing.T) {
		ls := New()
		ls2 := make([]string, 0, N)
		for i := 0; i < N; i++ {
			key := genKey(i)
			ls.RPush(key)
			ls2 = append(ls2, key)
		}
		assert.Equal(ls.Size(), len(ls2))
		assert.Equal(list2slice(ls), ls2)
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

	t.Run("range", func(t *testing.T) {
		ls := genList(0, N)
		i := 0
		ls.Range(0, -1, func(data []byte) {
			assert.Equal(string(data), genKey(i))
			i++
		})
		assert.Equal(i, N)

		for _, start := range []int{100, 1000, 5000} {
			i = 0
			ls.Range(start, start+100, func(data []byte) {
				assert.Equal(string(data), genKey(start+i))
				i++
			})
			assert.Equal(i, 100)
		}
	})

	t.Run("revrange", func(t *testing.T) {
		ls := genList(0, N)
		i := 0
		ls.RevRange(0, -1, func(data []byte) {
			assert.Equal(string(data), genKey(N-i-1))
			i++
		})
		assert.Equal(i, N)

		for _, start := range []int{100, 1000, 5000} {
			i = 0
			ls.RevRange(start, start+100, func(data []byte) {
				assert.Equal(string(data), genKey(N-start-i-1))
				i++
			})
			assert.Equal(i, 100)
		}
	})
}
