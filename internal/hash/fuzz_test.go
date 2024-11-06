package hash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/resp"
	"golang.org/x/exp/maps"
	"testing"
)

const MAX = 10000

func FuzzTestMap(f *testing.F) {
	stdmap := make(map[string][]byte, MAX)
	hashmap := NewMap()
	zipmap := NewZipMap()

	f.Fuzz(func(t *testing.T, op int, key, val string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2: // Set
			if len(stdmap) > MAX {
				break
			}
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

		case 8: // Scan
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

		case 9: // Encode
			w := resp.NewWriter(0)

			ast.Nil(hashmap.Encode(w))
			hashmap = NewMap()
			ast.Nil(hashmap.Decode(resp.NewReader(w.Bytes())))

			w.Reset()
			ast.Nil(zipmap.Encode(w))
			zipmap = NewZipMap()
			ast.Nil(zipmap.Decode(resp.NewReader(w.Bytes())))

			n := len(stdmap)
			ast.Equal(n, hashmap.Len())
			ast.Equal(n, zipmap.Len())
		}
	})
}

func FuzzTestSet(f *testing.F) {
	stdset := make(map[string]struct{}, MAX)
	hashset := NewSet()
	zipset := NewZipSet()

	f.Fuzz(func(t *testing.T, op int, key string) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2: // Add
			if len(stdset) > MAX {
				break
			}
			_, ok := stdset[key]
			stdset[key] = struct{}{}
			ast.Equal(!ok, hashset.Add(key))
			ast.Equal(!ok, zipset.Add(key))

		case 3, 4, 5: // Exist
			_, ok1 := stdset[key]
			ast.Equal(ok1, hashset.Exist(key))
			ast.Equal(ok1, zipset.Exist(key))

		case 6, 7: // Remove
			_, ok := stdset[key]
			delete(stdset, key)
			ast.Equal(ok, hashset.Remove(key))
			ast.Equal(ok, zipset.Remove(key))

		case 8: // Scan
			n := len(stdset)
			keys1 := maps.Keys(stdset)
			keys2 := make([]string, 0, n)
			keys3 := make([]string, 0, n)
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

		case 9: // Encode
			w := resp.NewWriter(0)

			ast.Nil(hashset.Encode(w))
			hashset = NewSet()
			ast.Nil(hashset.Decode(resp.NewReader(w.Bytes())))

			w.Reset()
			ast.Nil(zipset.Encode(w))
			zipset = NewZipSet()
			ast.Nil(zipset.Decode(resp.NewReader(w.Bytes())))

			n := len(stdset)
			ast.Equal(n, hashset.Len())
			ast.Equal(n, zipset.Len())
		}
	})
}
