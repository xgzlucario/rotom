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

	t.Run("hybird", func(t *testing.T) {
		ls := NewList()
		vls := make([]string, 0, N)

		for i := 0; i < N; i++ {
			switch rand.IntN(7) {
			// RPush
			case 0, 1:
				k := strconv.Itoa(int(rand.Uint32()))
				ls.RPush(k)
				vls = append(vls, k)

			// LPush
			case 2, 3:
				k := strconv.Itoa(int(rand.Uint32()))
				ls.LPush(k)
				vls = append([]string{k}, vls...)

			// LPop
			case 4:
				if len(vls) > 0 {
					val, ok := ls.LPop()
					valVls := vls[0]
					vls = vls[1:]
					assert.Equal(val, valVls)
					assert.True(ok)
				}

			// RPop
			case 5:
				if len(vls) > 0 {
					val, ok := ls.RPop()
					valVls := vls[len(vls)-1]
					vls = vls[:len(vls)-1]
					assert.Equal(val, valVls)
					assert.True(ok)
				}

			// Len
			case 6:
				assert.Equal(len(vls), ls.Size())
				assert.Equal(vls, ls.Keys())
			}
		}
	})
}
