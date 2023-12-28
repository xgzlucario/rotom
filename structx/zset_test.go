package structx

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZSet(t *testing.T) {
	assert := assert.New(t)
	z := NewZSet[string, int, string]()

	// Test Set
	for i := 0; i < 100; i++ {
		z.Set("key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}
	assert.Equal(z.Len(), 100)

	// Test SetScore
	for i := 0; i < 100; i++ {
		z.SetScore("key"+strconv.Itoa(i), i)
	}

	// Test SetWithScore
	for i := 100; i < 200; i++ {
		z.SetWithScore("key"+strconv.Itoa(i), i, "value"+strconv.Itoa(i))
	}

	// Test Get
	for i := 0; i < 100; i++ {
		v, s, ok := z.Get("key" + strconv.Itoa(i))
		assert.True(ok)
		assert.Equal(s, i)
		assert.Equal(v, "value"+strconv.Itoa(i))
	}

	v, s, ok := z.Get("no-exist")
	assert.False(ok)
	assert.Equal(s, 0)
	assert.Equal(v, "")

	// Test Iter
	z.Iter(func(k string, s int, v string) bool {
		test := strconv.Itoa(s)
		assert.Equal(k, "key"+test)
		assert.Equal(v, "value"+test)
		return s > 90
	})

	// Set update
	z.Set("key1", "hahaha")
	v, s, ok = z.Get("key1")
	assert.True(ok)
	assert.Equal(s, 1)
	assert.Equal(v, "hahaha")

	z.SetWithScore("key1", 999, "hahaha")
	v, s, ok = z.Get("key1")
	assert.True(ok)
	assert.Equal(s, 999)
	assert.Equal(v, "hahaha")

	// Set score not exist
	z.SetScore("key654", 100)
	v, s, ok = z.Get("key654")
	assert.True(ok)
	assert.Equal(s, 100)
	assert.Equal(v, "")

	// Incr
	z.Incr("key1", 1)
	v, s, ok = z.Get("key1")
	assert.True(ok)
	assert.Equal(s, 1000)
	assert.Equal(v, "hahaha")

	// Delete
	v, ok = z.Delete("key1")
	assert.True(ok)
	assert.Equal(v, "hahaha")

	v, ok = z.Delete("not-exist-delete")
	assert.False(ok)
	assert.Equal(v, "")

	// Incr not exist
	s = z.Incr("not-incr", 5)
	assert.Equal(s, 5)
	v, s, ok = z.Get("not-incr")
	assert.True(ok)
	assert.Equal(s, 5)
	assert.Equal(v, "")
}

func TestZSetMarshal(t *testing.T) {
	z := NewZSet[string, int, string]()
	for i := 0; i < 10*10000; i++ {
		z.SetWithScore("key"+strconv.Itoa(i), i, "value"+strconv.Itoa(i))
	}

	src, err := z.MarshalJSON()
	assert.Nil(t, err)

	z2 := NewZSet[string, int, string]()
	err = z2.UnmarshalJSON(src)
	assert.Nil(t, err)

	// valid
	for i := 0; i < 10*10000; i++ {
		v, s, ok := z2.Get("key" + strconv.Itoa(i))
		assert.True(t, ok)
		assert.Equal(t, s, i)
		assert.Equal(t, v, "value"+strconv.Itoa(i))
	}

	// unmarsal error
	err = z2.UnmarshalJSON([]byte("invalid json"))
	assert.NotNil(t, err)
}

func BenchmarkZSet(b *testing.B) {
	b.Run("Set", func(b *testing.B) {
		s := NewZSet[string, float64, any]()
		for i := 0; i < b.N; i++ {
			s.Set(strconv.Itoa(i), float64(i))
		}
	})
}
