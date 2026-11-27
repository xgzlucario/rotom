package set

import (
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/codec"
)

type set interface {
	Add(string) bool
	Contains(string) bool
	Remove(string) bool
	Pop() (string, bool)
	Len() int
	All() iter.Seq[string]
	codec.Encoder
}

// TestModel runs the same operations on both encodings and a map.
func TestModel(t *testing.T) {
	for name, newFn := range map[string]func() set{
		"set":    func() set { return New() },
		"zipset": func() set { return NewZipSet() },
	} {
		t.Run(name, func(t *testing.T) {
			ast := assert.New(t)
			s := newFn()
			model := map[string]bool{}
			for range 5000 {
				m := fmt.Sprint(rand.IntN(300))
				switch rand.IntN(5) {
				case 0, 1:
					ast.Equal(!model[m], s.Add(m))
					model[m] = true
				case 2:
					ast.Equal(model[m], s.Contains(m))
				case 3:
					ast.Equal(model[m], s.Remove(m))
					delete(model, m)
				case 4:
					got, ok := s.Pop()
					ast.Equal(len(model) > 0, ok)
					ast.True(!ok || model[got])
					delete(model, got)
				}
				ast.Equal(len(model), s.Len())
			}
			want := slices.Sorted(maps.Keys(model))
			ast.Equal(want, slices.Sorted(s.All()))

			w := codec.NewWriter(nil)
			s.Encode(w)
			s2 := newFn()
			s2.Decode(codec.NewReader(w.Bytes()))
			ast.Equal(want, slices.Sorted(s2.All()))

			if zs, ok := s.(*ZipSet); ok {
				ast.Equal(want, slices.Sorted(zs.ToSet().All()))
			}
		})
	}
}
