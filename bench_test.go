package rotom

import (
	"strconv"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkList(b *testing.B) {
	b.Run("slice", func(b *testing.B) {
		s := make([]string, 0)
		for i := 0; i < b.N; i++ {
			k := strconv.Itoa(i)
			s = append(s, k)
		}
	})
	b.Run("structx/r", func(b *testing.B) {
		s := structx.NewList()
		for i := 0; i < b.N; i++ {
			k := strconv.Itoa(i)
			s.RPush(k)
		}
	})
	b.Run("structx/l", func(b *testing.B) {
		s := structx.NewList()
		for i := 0; i < b.N; i++ {
			k := strconv.Itoa(i)
			s.LPush(k)
		}
	})
}
