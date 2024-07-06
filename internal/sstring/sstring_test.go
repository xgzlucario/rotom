package sstring

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShared(t *testing.T) {
	assert := assert.New(t)

	for i := -1; i < 10; i++ {
		var str string
		func() {
			defer func() {
				assert.True(recover().(string) == "string not found")
			}()
			str = Load(i)
		}()
		assert.Equal(str, "")
	}
	assert.Equal(Store("hello"), 0)
	assert.Equal(Store(""), 1)
	assert.Equal(Store("jello"), 2)
	assert.Equal(Store("hello"), 0)
	assert.Equal(Store(""), 1)
	assert.Equal(Store("jello"), 2)
	assert.Equal(Load(0), "hello")
	assert.Equal(Load(1), "")
	assert.Equal(Load(2), "jello")
	assert.Equal(Len(), 3)

}

func randStr(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	for i := 0; i < n; i++ {
		b[i] = 'a' + b[i]%26
	}
	return string(b)
}

func BenchmarkStore(b *testing.B) {
	wmap := make(map[string]bool, b.N)
	for len(wmap) < b.N {
		wmap[randStr(10)] = true
	}
	words := make([]string, 0, b.N)
	for word := range wmap {
		words = append(words, word)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Store(words[i])
	}
}

func BenchmarkLoad(b *testing.B) {
	wmap := make(map[string]bool, b.N)
	for len(wmap) < b.N {
		wmap[randStr(10)] = true
	}
	words := make([]string, 0, b.N)
	for word := range wmap {
		words = append(words, word)
	}
	var nums []int
	for i := 0; i < b.N; i++ {
		nums = append(nums, Store(words[i]))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Load(nums[i])
	}
}
