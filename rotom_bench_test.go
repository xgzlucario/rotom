package rotom

import "testing"

func BenchmarkDefer(b *testing.B) {
	b.Run("no-defer", func(b *testing.B) {
		var a int
		f := func() {
			a = a + 1
		}
		for i := 0; i < b.N; i++ {
			f()
		}
	})

	b.Run("defer", func(b *testing.B) {
		var a int
		f := func() {
			defer func() {
				a = a + 1
			}()
		}
		for i := 0; i < b.N; i++ {
			f()
		}
	})
}
