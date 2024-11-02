package hash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func FuzzTestMap(f *testing.F) {
	const N = 10000
	stdmap := make(map[string][]byte, N)
	hashmap := NewMap()
	zipmap := NewZipMap()

	f.Fuzz(func(t *testing.T, op int, key, val string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2:
			if len(stdmap) > N {
				break
			}
			_, ok := stdmap[key]
			stdmap[key] = []byte(val)
			ast.Equal(!ok, hashmap.Set(key, []byte(val)))
			ast.Equal(!ok, zipmap.Set(key, []byte(val)))

		case 3, 4, 5:
			val1, ok1 := stdmap[key]
			val2, ok2 := hashmap.Get(key)
			val3, ok3 := zipmap.Get(key)

			ast.Equal(string(val1), string(val2))
			ast.Equal(string(val1), string(val3))
			ast.Equal(ok1, ok2)
			ast.Equal(ok1, ok3)

		case 6, 7:
			_, ok := stdmap[key]
			delete(stdmap, key)
			ast.Equal(ok, hashmap.Remove(key))
			ast.Equal(ok, zipmap.Remove(key))

		case 8:
			len1 := len(stdmap)
			ast.Equal(len1, hashmap.Len())
			ast.Equal(len1, zipmap.Len())

		case 9:
			n := len(stdmap)
			keys1 := make([]string, 0, n)
			keys2 := make([]string, 0, n)
			keys3 := make([]string, 0, n)
			vals1 := make([]string, 0, n)
			vals2 := make([]string, 0, n)
			vals3 := make([]string, 0, n)
			for k, v := range stdmap {
				keys1 = append(keys1, k)
				vals1 = append(vals1, string(v))
			}
			hashmap.Scan(func(k string, v []byte) {
				keys2 = append(keys2, k)
				vals2 = append(vals2, string(v))
			})
			// toMap
			newMap := zipmap.ToMap()
			newMap.Scan(func(k string, v []byte) {
				keys3 = append(keys3, k)
				vals3 = append(vals3, string(v))
			})
			ast.ElementsMatch(keys1, keys2)
			ast.ElementsMatch(keys1, keys3)
			ast.ElementsMatch(vals1, vals2)
			ast.ElementsMatch(vals1, vals3)
		}
	})
}
