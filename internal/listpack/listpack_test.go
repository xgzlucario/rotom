package listpack

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/codec"
)

func collect(seq func(func([]byte) bool)) []string {
	var res []string
	for v := range seq {
		res = append(res, string(v))
	}
	return res
}

// TestModel checks ListPack against a plain slice.
func TestModel(t *testing.T) {
	ast := assert.New(t)
	lp := New()
	var model []string
	for i := range 20000 {
		// lengths around varint boundaries (127/128) and empty strings
		v := strings.Repeat("x", []int{0, 1, 127, 128, 300}[rand.IntN(5)]) + fmt.Sprint(i)
		switch rand.IntN(6) {
		case 0, 1:
			lp.PushFront(v)
			model = slices.Insert(model, 0, v)
		case 2, 3:
			lp.PushBack(v)
			model = append(model, v)
		case 4:
			got, ok := lp.PopFront()
			ast.Equal(len(model) > 0, ok)
			if ok {
				ast.Equal(model[0], got)
				model = model[1:]
			}
		case 5:
			got, ok := lp.PopBack()
			ast.Equal(len(model) > 0, ok)
			if ok {
				ast.Equal(model[len(model)-1], got)
				model = model[:len(model)-1]
			}
		}
		ast.Equal(len(model), lp.Len())
	}
	ast.Equal(model, collect(lp.All()))
	rev := slices.Clone(model)
	slices.Reverse(rev)
	ast.Equal(rev, collect(lp.Backward()))

	w := codec.NewWriter(nil)
	lp.Encode(w)
	lp2 := New()
	lp2.Decode(codec.NewReader(w.Bytes()))
	ast.Equal(model, collect(lp2.All()))
}

func TestCursor(t *testing.T) {
	ast := assert.New(t)
	lp := New()
	lp.PushBack("a")
	lp.PushBack("c")

	// insert in the middle
	c := lp.Front()
	c.Next()
	c.Insert("b")
	ast.Equal([]string{"a", "b", "c"}, collect(lp.All()))

	// remove the entry just read: step back, then remove
	c = lp.Front()
	c.Next()
	c.Next()
	c.Prev()
	ast.Equal("b", c.Remove())
	ast.Equal([]string{"a", "c"}, collect(lp.All()))
	ast.Equal(2, lp.Len())
}

func TestDecodeCorrupted(t *testing.T) {
	w := codec.NewWriter(nil)
	w.WriteUvarint(1)
	w.WriteBytes([]byte{5, 'a'}) // entry claims 5 bytes
	assert.PanicsWithValue(t, codec.ErrCorrupted, func() {
		New().Decode(codec.NewReader(w.Bytes()))
	})
}

func BenchmarkListPack(b *testing.B) {
	lp := New()
	for i := range 100 {
		lp.PushBack(fmt.Sprintf("%08x", i))
	}
	b.Run("all", func(b *testing.B) {
		for b.Loop() {
			for range lp.All() {
			}
		}
	})
	b.Run("backward", func(b *testing.B) {
		for b.Loop() {
			for range lp.Backward() {
			}
		}
	})
}
