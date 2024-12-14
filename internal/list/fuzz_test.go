package list

import (
	"github.com/xgzlucario/rotom/internal/iface"
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

		case 6: // LRange
			if len(slice) == 0 {
				break
			}
			var keys1, keys2, keys3 []string
			start := rand.Int() % len(slice)
			count := rand.IntN(len(slice) + 1)

			// slice range
			keys1 = slice[start:min(start+count, len(slice))]

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
			i := 0
			ls.Range(start, func(k []byte) (stop bool) {
				if i == count {
					return true
				}
				i++
				keys3 = append(keys3, string(k))
				return false
			})

			ast.ElementsMatch(keys1, keys2)
			ast.ElementsMatch(keys1, keys3)

		case 7: // Encode
			{
				w := iface.NewWriter(nil)
				lp.WriteTo(w)
				lp = NewListPack()
				lp.ReadFrom(iface.NewReaderFrom(w))
			}
			{
				w := iface.NewWriter(nil)
				ls.WriteTo(w)
				ls = New()
				ls.ReadFrom(iface.NewReaderFrom(w))
			}
			ast.Equal(lp.Len(), ls.Len())
		}
	})
}
