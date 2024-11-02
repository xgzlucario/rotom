package hash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func FuzzTestMap(f *testing.F) {
	stdmap := make(map[string][]byte)
	hashmap := NewMap()
	zipmap := NewZipMap()

	f.Fuzz(func(t *testing.T, op int, key, val string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2: // Set
			_, ok := stdmap[key]
			stdmap[key] = []byte(val)
			ast.Equal(!ok, hashmap.Set(key, []byte(val)))
			ast.Equal(!ok, zipmap.Set(key, []byte(val)))

		case 3, 4, 5: // Get
			val1, ok1 := stdmap[key]
			val2, ok2 := hashmap.Get(key)
			val3, ok3 := zipmap.Get(key)

			ast.Equal(string(val1), string(val2))
			ast.Equal(string(val1), string(val3))
			ast.Equal(ok1, ok2)
			ast.Equal(ok1, ok3)

		case 6, 7: // Delete
			_, ok := stdmap[key]
			delete(stdmap, key)
			ast.Equal(ok, hashmap.Remove(key))
			ast.Equal(ok, zipmap.Remove(key))

		case 8: // Len
			n := len(stdmap)
			ast.Equal(n, hashmap.Len())
			ast.Equal(n, zipmap.Len())

		case 9: // Scan
			n := len(stdmap)
			kv1 := make([]string, 0, n)
			kv2 := make([]string, 0, n)
			kv3 := make([]string, 0, n)
			for k, v := range stdmap {
				kv1 = append(kv1, fmt.Sprintf("%s->%s", k, v))
			}
			hashmap.Scan(func(k string, v []byte) {
				kv2 = append(kv2, fmt.Sprintf("%s->%s", k, v))
			})
			// toMap
			newMap := zipmap.ToMap()
			newMap.Scan(func(k string, v []byte) {
				kv3 = append(kv3, fmt.Sprintf("%s->%s", k, v))
			})
			ast.ElementsMatch(kv1, kv2)
			ast.ElementsMatch(kv1, kv3)
		}
	})
}

func FuzzTestSet(f *testing.F) {
	stdset := make(map[string]struct{})
	hashset := NewSet()
	zipset := NewZipSet()

	f.Fuzz(func(t *testing.T, op int, key string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2: // Add
			_, ok := stdset[key]
			stdset[key] = struct{}{}
			ast.Equal(!ok, hashset.Add(key))
			ast.Equal(!ok, zipset.Add(key))

		case 3, 4, 5: // Exist
			_, ok1 := stdset[key]
			ok2 := hashset.Exist(key)
			ok3 := zipset.Exist(key)

			ast.Equal(ok1, ok2)
			ast.Equal(ok1, ok3)

		case 6, 7: // Remove
			_, ok := stdset[key]
			delete(stdset, key)
			ast.Equal(ok, hashset.Remove(key))
			ast.Equal(ok, zipset.Remove(key))

		case 8: // Len
			n := len(stdset)
			ast.Equal(n, hashset.Len())
			ast.Equal(n, zipset.Len())

		case 9: // Scan
			n := len(stdset)
			keys1 := make([]string, 0, n)
			keys2 := make([]string, 0, n)
			keys3 := make([]string, 0, n)
			for k := range stdset {
				keys1 = append(keys1, k)
			}
			hashset.Scan(func(k string) {
				keys2 = append(keys2, k)
			})
			// toSet
			newSet := zipset.ToSet()
			newSet.Scan(func(k string) {
				keys3 = append(keys3, k)
			})
			ast.ElementsMatch(keys1, keys2)
			ast.ElementsMatch(keys1, keys3)
		}
	})
}
