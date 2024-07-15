package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genKey(i int) string {
	return fmt.Sprintf("%06x", i)
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
		lp.RPush("A")
		lp.RPush("B", "C")
		assert.Equal(lp.Size(), 3)
		assert.Equal(lp2list(lp), []string{"A", "B", "C"})
	})

	t.Run("rpush", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("A")
		lp.LPush("B", "C")
		assert.Equal(lp.Size(), 3)
		assert.Equal(lp2list(lp), []string{"B", "C", "A"})
	})

	t.Run("lpop", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("A", "B", "C")

		it := lp.Iterator()
		// bound check
		it.Prev()

		val, ok := it.RemoveNext()
		assert.Equal(val, "A")
		assert.True(ok)

		val, ok = it.RemoveNext()
		assert.Equal(val, "B")
		assert.True(ok)

		val, ok = it.RemoveNext()
		assert.Equal(val, "C")
		assert.True(ok)

		// empty
		val, ok = it.RemoveNext()
		assert.Equal(val, "")
		assert.False(ok)
	})

	t.Run("rpop", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("A", "B", "C")

		it := lp.Iterator().SeekLast()
		// bound check
		// it.Next()

		it.Prev()
		val, ok := it.RemoveNext()
		assert.Equal(val, "C")
		assert.True(ok)

		it.Prev()
		val, ok = it.RemoveNext()
		assert.Equal(val, "B")
		assert.True(ok)

		it.Prev()
		val, ok = it.RemoveNext()
		assert.Equal(val, "A")
		assert.True(ok)

		// empty
		it.Prev()
		val, ok = it.RemoveNext()
		assert.Equal(val, "")
		assert.False(ok)
	})

	t.Run("removeNexts", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("aa", "bb", "cc", "dd", "ee")

		str, ok := lp.Iterator().RemoveNext()
		assert.Equal(str, "aa")
		assert.True(ok)

		res := lp.Iterator().RemoveNexts(2)
		assert.Equal(res, []string{"bb", "cc"})

		res = lp.Iterator().RemoveNexts(3)
		assert.Equal(res, []string{"dd", "ee"})

		str, ok = lp.Iterator().RemoveNext()
		assert.Equal(str, "")
		assert.False(ok)
	})

	t.Run("replaceNext", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("TEST1", "TEST2", "TEST3")

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
		lp.LPush("A", "B", "C", "D", "E")
		lp.Compress()
		lp.Compress()
		lp.Decompress()
		lp.Decompress()
		assert.Equal(lp2list(lp), []string{"A", "B", "C", "D", "E"})
	})
}
