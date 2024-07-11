package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genListPack(start, end int) *ListPack {
	lp := NewListPack()
	for i := start; i < end; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func genKey(i int) string {
	return fmt.Sprintf("%06x", i)
}

func lp2list(lp *ListPack) (res []string) {
	it := lp.NewIterator()
	for !it.IsEnd() {
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
		lp.LPush("A", "B", "C")

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
}
