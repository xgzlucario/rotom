package zset

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/resp"
	"testing"
	"time"
)

func FuzzTestZSet(f *testing.F) {
	zs := New()
	zzs := NewZipZSet()

	f.Fuzz(func(t *testing.T, key string, score float64) {
		ast := assert.New(t)
		ts := time.Now().Second()
		switch ts % 10 {
		case 0, 1: // Set
			ast.Equal(zs.Set(key, score), zzs.Set(key, score))
			ast.Equal(zs.Len(), zzs.Len())

		case 2, 3: // Get
			sc1, ok1 := zs.Get(key)
			sc2, ok2 := zzs.Get(key)
			ast.Equal(sc1, sc2)
			ast.Equal(ok1, ok2)
			ast.Equal(zs.Rank(key), zzs.Rank(key))

		case 4, 5: // PopMin
			k1, s1 := zs.PopMin()
			k2, s2 := zzs.PopMin()
			ast.Equal(k1, k2)
			ast.Equal(s1, s2)

		case 6, 7: // Remove
			ast.Equal(zs.Remove(key), zzs.Remove(key))
			ast.Equal(zs.Len(), zzs.Len())

		case 8: // Scan
			kv1 := make([]string, 0, zs.Len())
			kv2 := make([]string, 0, zs.Len())
			zs.Scan(func(k string, v float64) {
				kv1 = append(kv1, fmt.Sprintf("%s->%v", k, v))
			})
			zzs.Scan(func(k string, v float64) {
				kv2 = append(kv2, fmt.Sprintf("%s->%v", k, v))
			})
			ast.Equal(kv1, kv2)

		case 9: // Encode
			writer := resp.NewWriter(1024)

			// zset
			ast.Nil(zs.Encode(writer))
			zs = New()
			ast.Nil(zs.Decode(resp.NewReader(writer.Bytes())))
			writer.Reset()

			// zipzset
			ast.Nil(zzs.Encode(writer))
			zzs = NewZipZSet()
			ast.Nil(zzs.Decode(resp.NewReader(writer.Bytes())))
		}
	})
}
