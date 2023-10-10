package test

import (
	"sync"
	"testing"

	"github.com/xgzlucario/rotom/base"
)

func BenchmarkLocker(b *testing.B) {
	b.Run("mutex-single", func(b *testing.B) {
		var m sync.Mutex
		var a int

		for i := 0; i < b.N; i++ {
			m.Lock()
			a = a + 1
			m.Unlock()
		}
	})

	b.Run("mutex-multi", func(b *testing.B) {
		var m sync.Mutex
		var a int

		f := func() {
			m.Lock()
			a = a + 1
			m.Unlock()
		}

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < b.N; i++ {
					f()
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			f()
		}
	})

	b.Run("lockfree-single", func(b *testing.B) {
		m := base.NewLfLocker()
		var a int

		for i := 0; i < b.N; i++ {
			m.Lock()
			a = a + 1
			m.Unlock()
		}
	})

	b.Run("lockfree-multi", func(b *testing.B) {
		m := base.NewLfLocker()
		var a int

		f := func() {
			m.Lock()
			a = a + 1
			m.Unlock()
		}

		for i := 0; i < 10; i++ {
			go func() {
				for i := 0; i < b.N; i++ {
					f()
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			f()
		}
	})
}
