package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func BenchmarkList(b *testing.B) {
	b.Run("ziplist/RPush", func(b *testing.B) {
		ls := structx.NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.RPush(i)
		}
	})

	b.Run("ziplist/LPush", func(b *testing.B) {
		ls := structx.NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.LPush(i)
		}
	})
}
