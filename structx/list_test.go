package structx

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	assert := assert.New(t)
	ls := NewList[int]()
	valid := make([]int, 0, 1024)

	// test empty pop
	_, ok := ls.LPop()
	assert.False(ok)

	_, ok = ls.RPop()
	assert.False(ok)

	// random insert
	for i := 0; i < 1000; i++ {
		elem := rand.Int()
		if i%2 == 0 {
			ls.RPush(elem)
			valid = append(valid, elem)

		} else {
			ls.LPush(elem)
			valid = append([]int{elem}, valid...)
		}
	}

	// test len
	assert.Equal(ls.Len(), 1000)

	// test range
	i := 0
	ls.Range(func(elem int) bool {
		assert.Equal(elem, valid[i])
		i++
		return i < 500
	})

	// test index
	for i := 0; i < 1000; i++ {
		index := rand.Intn(ls.Len())

		elem, ok := ls.Index(index)
		assert.True(ok)
		assert.Equal(elem, valid[index])
	}

	val, ok := ls.Index(-1)
	assert.False(ok)
	assert.Equal(val, 0)

	val, ok = ls.Index(math.MaxInt)
	assert.False(ok)
	assert.Equal(val, 0)

	// test pop
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			elem, ok := ls.LPop()
			assert.True(ok)
			assert.Equal(elem, valid[0])

			valid = valid[1:]

		} else {
			elem, ok := ls.RPop()
			assert.True(ok)
			assert.Equal(elem, valid[len(valid)-1])

			valid = valid[:len(valid)-1]
		}
	}
}

func TestListMarshal(t *testing.T) {
	ls := NewList[int]()
	assert := assert.New(t)

	src, err := ls.MarshalJSON()
	assert.Nil(src)
	assert.Nil(err)

	err = ls.UnmarshalJSON([]byte("T"))
	assert.Nil(err)
}

func BenchmarkList(b *testing.B) {
	b.Run("ziplist/RPush", func(b *testing.B) {
		ls := NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.RPush(i)
		}
	})

	b.Run("ziplist/LPush", func(b *testing.B) {
		ls := NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.LPush(i)
		}
	})

	getList := func() *List[int] {
		ls := NewList[int]()
		for i := 0; i < 1000*10000; i++ {
			ls.RPush(i)
		}
		return ls
	}

	b.Run("ziplist/RPop", func(b *testing.B) {
		ls := getList()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.RPop()
		}
	})

	b.Run("ziplist/LPop", func(b *testing.B) {
		ls := getList()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ls.LPop()
		}
	})
}
