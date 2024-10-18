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
	ast := assert.New(t)

	t.Run("rpush", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")
		ast.Equal(lp.Size(), 3)
		ast.Equal(lp2list(lp), []string{"A", "B", "C"})
	})

	t.Run("lpush", func(t *testing.T) {
		lp := NewListPack()
		lp.LPush("A", "B", "C")
		ast.Equal(lp.Size(), 3)
		ast.Equal(lp2list(lp), []string{"C", "B", "A"})
	})

	t.Run("lpop", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")

		val, ok := lp.LPop()
		ast.Equal(val, "A")
		ast.True(ok)

		val, ok = lp.LPop()
		ast.Equal(val, "B")
		ast.True(ok)

		val, ok = lp.LPop()
		ast.Equal(val, "C")
		ast.True(ok)

		// empty
		val, ok = lp.LPop()
		ast.Equal(val, "")
		ast.False(ok)
	})

	t.Run("rpop", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("A", "B", "C")

		val, ok := lp.RPop()
		ast.Equal(val, "C")
		ast.True(ok)

		val, ok = lp.RPop()
		ast.Equal(val, "B")
		ast.True(ok)

		val, ok = lp.RPop()
		ast.Equal(val, "A")
		ast.True(ok)

		// empty
		val, ok = lp.RPop()
		ast.Equal(val, "")
		ast.False(ok)
	})

	t.Run("removeNexts", func(t *testing.T) {
		lp := NewListPack()
		lp.RPush("aa", "bb", "cc", "dd", "ee")

		lp.Iterator().RemoveNexts(1, func(b []byte) {
			ast.Equal(string(b), "aa")
		})

		var index int
		lp.Iterator().RemoveNexts(5, func(b []byte) {
			switch index {
			case 0:
				ast.Equal(string(b), "bb")
			case 1:
				ast.Equal(string(b), "cc")
			case 2:
				ast.Equal(string(b), "dd")
			case 3:
				ast.Equal(string(b), "ee")
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
		ast.Equal(lp2list(lp), []string{"TEST4", "TEST2", "TEST3"})

		it.ReplaceNext("ABC")
		ast.Equal(lp2list(lp), []string{"ABC", "TEST2", "TEST3"})

		it.ReplaceNext("TTTTTT")
		ast.Equal(lp2list(lp), []string{"TTTTTT", "TEST2", "TEST3"})

		it.SeekLast().ReplaceNext("a")
		ast.Equal(lp2list(lp), []string{"TTTTTT", "TEST2", "TEST3"})
	})
}
