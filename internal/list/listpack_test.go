package list

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

const MAX = 10000

func FuzzTestList(f *testing.F) {
	slice := make([]string, 0, MAX)
	lp := NewListPack()
	ls := New()

	f.Fuzz(func(t *testing.T, op int, key string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1: // LPush
			slice = append([]string{key}, slice...)
			lp.LPush(key)
			ls.LPush(key)

		case 2, 3: // RPush
			slice = append(slice, key)
			lp.RPush(key)
			ls.RPush(key)

		case 4: // LPop
			var key1 string
			var ok1 bool
			if len(slice) > 0 {
				key1 = slice[0]
				ok1 = true
				slice = slice[1:]
			}
			key2, ok2 := lp.LPop()
			key3, ok3 := ls.LPop()

			ast.Equal(key1, key2)
			ast.Equal(key1, key3)
			ast.Equal(ok1, ok2)
			ast.Equal(ok1, ok3)

		case 5: // RPop
			var key1 string
			var ok1 bool
			if len(slice) > 0 {
				key1 = slice[len(slice)-1]
				ok1 = true
				slice = slice[:len(slice)-1]
			}
			key2, ok2 := lp.RPop()
			key3, ok3 := ls.RPop()

			ast.Equal(key1, key2)
			ast.Equal(key1, key3)
			ast.Equal(ok1, ok2)
			ast.Equal(ok1, ok3)

		case 8: // LRange
			if len(slice) == 0 {
				break
			}
			var keys2, keys3 []string

			start := rand.Int() % len(slice)
			count := rand.IntN(len(slice) + 1)

			keys1 := slice[start:min(start+count, len(slice))]

			// listpack range
			it := lp.Iterator()
			for range start {
				if !it.IsLast() {
					it.Next()
				}
			}
			for range count {
				if !it.IsLast() {
					keys2 = append(keys2, string(it.Next()))
				}
			}

			// list range
			ls.Range(start, start+count, func(k []byte) {
				keys3 = append(keys3, string(k))
			})
			ast.ElementsMatch(keys1, keys2)
			ast.ElementsMatch(keys1, keys3)

		case 9: // Marshal
		}
	})
}

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
