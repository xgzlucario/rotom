package zset

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

const MAX = 10000

func FuzzTestZSet(f *testing.F) {
	zs := New()
	zzs := NewZipZSet()

	f.Fuzz(func(t *testing.T, op int, key string, score float64) {
		ast := assert.New(t)
		switch op % 10 {
		case 0, 1, 2: // Set
			if zs.Len() > MAX {
				return
			}
			ast.Equal(zs.Set(key, score), zzs.Set(key, score))

		case 3, 4: // Get
			sc1, ok1 := zs.Get(key)
			sc2, ok2 := zzs.Get(key)
			ast.Equal(sc1, sc2)
			ast.Equal(ok1, ok2)

		case 5: // Rank
			ast.Equal(zs.Rank(key), zzs.Rank(key))

		case 6: // PopMin
			k1, s1 := zs.PopMin()
			k2, s2 := zzs.PopMin()
			ast.Equal(k1, k2)
			ast.Equal(s1, s2)

		case 7: // Delete
			ast.Equal(zs.Remove(key), zzs.Remove(key))

		case 8: // Scan
			kv1 := make([]string, 0)
			kv2 := make([]string, 0)
			zs.Scan(func(k string, v float64) {
				kv1 = append(kv1, fmt.Sprintf("%s->%v", k, v))
			})
			zzs.Scan(func(k string, v float64) {
				kv2 = append(kv2, fmt.Sprintf("%s->%v", k, v))
			})
			ast.ElementsMatch(kv1, kv2)

		case 9: // Encode
		}
	})
}
