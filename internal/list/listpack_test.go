package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genKey(i int) string {
	return fmt.Sprintf("%08x", i)
}

func lp2list(lp *ListPack) (res []string) {
	it := lp.Iterator()
	for !it.IsLast() {
		res = append(res, string(it.Next()))
	}
	return
}

func TestListpack(t *testing.T) {
	assert := assert.New(t)

	t.Run("rpush", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")
		assert.Equal(lp.Size(), 3)
		assert.Equal(lp2list(lp), []string{"A", "B", "C"})
	})

	t.Run("lpush", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("A", "B", "C")
		assert.Equal(lp.Size(), 3)
		assert.Equal(lp2list(lp), []string{"C", "B", "A"})
	})

	t.Run("lpop", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")

		val, ok := lp.LPop()
		assert.Equal(val, "A")
		assert.True(ok)

		val, ok = lp.LPop()
		assert.Equal(val, "B")
		assert.True(ok)

		val, ok = lp.LPop()
		assert.Equal(val, "C")
		assert.True(ok)

		// empty
		val, ok = lp.LPop()
		assert.Equal(val, "")
		assert.False(ok)
	})

	t.Run("rpop", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")

		val, ok := lp.RPop()
		assert.Equal(val, "C")
		assert.True(ok)

		val, ok = lp.RPop()
		assert.Equal(val, "B")
		assert.True(ok)

		val, ok = lp.RPop()
		assert.Equal(val, "A")
		assert.True(ok)

		// empty
		val, ok = lp.RPop()
		assert.Equal(val, "")
		assert.False(ok)
	})

	t.Run("removeNexts", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("aa", "bb", "cc", "dd", "ee")

		lp.Iterator().RemoveNexts(1, func(b []byte) {
			assert.Equal(string(b), "aa")
		})

		var index int
		lp.Iterator().RemoveNexts(5, func(b []byte) {
			switch index {
			case 0:
				assert.Equal(string(b), "bb")
			case 1:
				assert.Equal(string(b), "cc")
			case 2:
				assert.Equal(string(b), "dd")
			case 3:
				assert.Equal(string(b), "ee")
			}
			index++
		})

		lp.Iterator().RemoveNexts(1, func([]byte) {
			panic("should not call")
		})

		lp.Iterator().SeekLast().RemoveNexts(1, func([]byte) {
			panic("should not call")
		})
	})

	t.Run("replaceNext", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("TEST1", "TEST2", "TEST3")

		it := lp.Iterator()
		it.ReplaceNext("TEST4")
		assert.Equal(lp2list(lp), []string{"TEST4", "TEST2", "TEST3"})

		it.ReplaceNext("ABC")
		assert.Equal(lp2list(lp), []string{"ABC", "TEST2", "TEST3"})

		it.ReplaceNext("TTTTTT")
		assert.Equal(lp2list(lp), []string{"TTTTTT", "TEST2", "TEST3"})

		it.SeekLast().ReplaceNext("a")
		assert.Equal(lp2list(lp), []string{"TTTTTT", "TEST2", "TEST3"})
	})

	t.Run("compress", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")
		lp.Compress()
		lp.Compress()
		lp.Decompress()
		lp.Decompress()
		assert.Equal(lp2list(lp), []string{"A", "B", "C"})
	})
}
