package list

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genList(start, stop int) *QuickList {
	lp := New()
	for i := start; i < stop; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func genListPack(start, stop int) *ListPack {
	lp := NewListPack()
	for i := start; i < stop; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func list2slice(ls *QuickList) (res []string) {
	ls.Range(0, ls.Size(), func(data []byte) {
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
		ls := New()
		// [0, 1, 2, 3, 4]
		for i := range 5 {
			ls.RPush(strconv.Itoa(i))
		}

		rangeFn := func(start, stop int) (res []string) {
			ls.Range(start, stop, func(data []byte) {
				res = append(res, string(data))
			})
			assert.Equal(len(res), ls.RangeCount(start, stop))
			return
		}

		assert.Equal(rangeFn(0, -1), []string{"0", "1", "2", "3", "4"})
		assert.Equal(rangeFn(-100, 100), []string{"0", "1", "2", "3", "4"})
		assert.Equal(rangeFn(1, 3), []string{"1", "2", "3"})
		assert.Equal(rangeFn(-3, -1), []string{"2", "3", "4"})
		assert.Equal(rangeFn(3, 3), []string{"3"})
		assert.Equal(rangeFn(-3, 2), []string{"2"})

		// empty
		var nilStrings []string
		assert.Equal(rangeFn(99, 100), nilStrings)
		assert.Equal(rangeFn(-100, -99), nilStrings)
		assert.Equal(rangeFn(-1, -3), nilStrings)
		assert.Equal(rangeFn(3, 2), nilStrings)
	})

	t.Run("range2", func(t *testing.T) {
		ls := genList(0, N)
		i := 0
		ls.Range(0, N, func(data []byte) {
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
			assert.Equal(i, 101)
		}
	})

	t.Run("marshal", func(t *testing.T) {
		ls := genList(0, N)
		buf, err := ls.Marshal()
		assert.Nil(err)

		ls2 := New()
		err = ls2.Unmarshal(buf)
		assert.Nil(err)

		i := 0
		ls.Range(0, N, func(data []byte) {
			assert.Equal(string(data), genKey(i))
			i++
		})
	})
}
