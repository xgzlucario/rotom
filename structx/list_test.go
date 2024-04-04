package structx

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	assert := assert.New(t)
	const N = 1000
	SetEachNodeMaxSize(128)

	t.Run("rpush", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), i)
			ls.RPush(fmt.Sprintf("%08d", i))
		}
		for i := 0; i < N; i++ {
			v, ok := ls.Index(i)
			assert.Equal(fmt.Sprintf("%08d", i), v)
			assert.True(ok)
		}
	})

	t.Run("lpush", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), i)
			ls.LPush(fmt.Sprintf("%08d", i))
		}
		for i := 0; i < N; i++ {
			v, ok := ls.Index(N - 1 - i)
			assert.Equal(fmt.Sprintf("%08d", i), v)
			assert.True(ok)
		}
	})

	t.Run("lpop", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			ls.RPush(fmt.Sprintf("%08d", i))
		}
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), N-i)
			key, ok := ls.LPop()
			assert.Equal(key, fmt.Sprintf("%08d", i))
			assert.True(ok)
		}
		// pop empty list
		for i := 0; i < N; i++ {
			key, ok := ls.LPop()
			assert.Equal(key, "")
			assert.False(ok)
		}
	})

	t.Run("rpop", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			ls.RPush(fmt.Sprintf("%08d", i))
		}
		for i := 0; i < N; i++ {
			assert.Equal(ls.Size(), N-i)
			key, ok := ls.RPop()
			assert.Equal(key, fmt.Sprintf("%08d", N-i-1))
			assert.True(ok)
		}
		// pop empty list
		for i := 0; i < N; i++ {
			key, ok := ls.RPop()
			assert.Equal(key, "")
			assert.False(ok)
		}
	})

	t.Run("len", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			ls.RPush(fmt.Sprintf("%08d", i))
			assert.Equal(ls.Size(), i+1)
			assert.Equal(len(ls.Keys()), i+1)
		}
	})

	t.Run("marshal", func(t *testing.T) {
		ls := NewList()
		for i := 0; i < N; i++ {
			ls.RPush(fmt.Sprintf("%08d", i))
		}
		data := ls.Marshal()

		ls2 := NewList()
		err := ls2.Unmarshal(data)
		assert.Nil(err)

		for i := 0; i < N; i++ {
			v, ok := ls.Index(i)
			assert.Equal(fmt.Sprintf("%08d", i), v)
			assert.True(ok)
		}
	})

	t.Run("range", func(t *testing.T) {
		ls := NewList()
		ls.Range(1, 2, func(s string) (stop bool) {
			panic("should not call")
		})
		for i := 0; i < N; i++ {
			ls.RPush(fmt.Sprintf("%08d", i))
		}
		ls.Range(-1, -1, func(s string) (stop bool) {
			panic("should not call")
		})
	})
}

func FuzzList(f *testing.F) {
	ls := NewList()
	vls := make([]string, 0, 4096)

	f.Fuzz(func(t *testing.T, key string) {
		assert := assert.New(t)

		switch rand.IntN(13) {
		// RPush
		case 0, 1, 2:
			k := strconv.Itoa(rand.Int())
			ls.RPush(k)
			vls = append(vls, k)

		// LPush
		case 3, 4, 5:
			k := strconv.Itoa(rand.Int())
			ls.LPush(k)
			vls = append([]string{k}, vls...)

		// LPop
		case 6, 7:
			val, ok := ls.LPop()
			if len(vls) > 0 {
				valVls := vls[0]
				vls = vls[1:]
				assert.Equal(val, valVls)
				assert.True(ok)
			} else {
				assert.Equal(val, "")
				assert.False(ok)
			}

		// RPop
		case 8, 9:
			val, ok := ls.RPop()
			if len(vls) > 0 {
				valVls := vls[len(vls)-1]
				vls = vls[:len(vls)-1]
				assert.Equal(val, valVls)
				assert.True(ok)
			} else {
				assert.Equal(val, "")
				assert.False(ok)
			}

		// Index
		case 10:
			if len(vls) > 0 {
				index := rand.IntN(len(vls))
				val, ok := ls.Index(index)
				vlsVal := vls[index]
				assert.Equal(val, vlsVal)
				assert.True(ok)
			}

		// Range
		case 11:
			if len(vls) > 2 {
				start := rand.IntN(len(vls) / 2)
				end := len(vls)/2 + rand.IntN(len(vls)/2)

				keys := make([]string, 0, end-start)
				ls.Range(start, end, func(s string) (stop bool) {
					keys = append(keys, s)
					return false
				})
				assert.Equal(keys, vls[start:end], fmt.Sprintf("start: %v, end: %v", start, end))
			}

		// Marshal
		case 12:
			data := ls.Marshal()
			nls := NewList()
			err := nls.Unmarshal(data)
			assert.Nil(err)

			assert.Equal(len(vls), nls.Size())
			if len(vls) == 0 {
				assert.Equal(vls, []string{})
			} else {
				assert.Equal(vls, nls.Keys())
			}
		}
	})
}
