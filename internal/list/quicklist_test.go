package list

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/codec"
)

func collect(seq func(func([]byte) bool)) []string {
	res := []string{}
	for v := range seq {
		res = append(res, string(v))
	}
	return res
}

func checkLinks(t *testing.T, ql *QuickList) {
	t.Helper()
	total := 0
	var prev *node
	for n := ql.head; n != nil; prev, n = n, n.next {
		if n.prev != prev || n.lp.Len() == 0 {
			t.Fatal("broken links or empty node")
		}
		total += n.lp.Len()
	}
	if ql.tail != prev || total != ql.length {
		t.Fatal("tail/length mismatch")
	}
}

// TestModel checks QuickList against a plain slice; values are large enough
// to spread the list over many nodes.
func TestModel(t *testing.T) {
	ast := assert.New(t)
	ql := New()
	var model []string
	for i := range 30000 {
		v := fmt.Sprintf("%0100d", i)
		switch rand.IntN(5) {
		case 0, 1:
			ql.LPush(v)
			model = slices.Insert(model, 0, v)
		case 2, 3:
			ql.RPush(v)
			model = append(model, v)
		case 4:
			if rand.IntN(2) == 0 {
				got, ok := ql.LPop()
				if ast.Equal(len(model) > 0, ok) && ok {
					ast.Equal(model[0], got)
					model = model[1:]
				}
			} else {
				got, ok := ql.RPop()
				if ast.Equal(len(model) > 0, ok) && ok {
					ast.Equal(model[len(model)-1], got)
					model = model[:len(model)-1]
				}
			}
		}
		if i%1000 == 0 && len(model) > 0 {
			start := rand.IntN(len(model))
			stop := start + rand.IntN(len(model)-start)
			ast.Equal(model[start:stop+1], collect(ql.Range(start, stop)))
		}
	}
	checkLinks(t, ql)
	ast.Equal(len(model), ql.Len())
	ast.Equal(model, collect(ql.All()))

	w := codec.NewWriter(nil)
	ql.Encode(w)
	ql2 := New()
	ql2.Decode(codec.NewReader(w.Bytes()))
	checkLinks(t, ql2)
	ast.Equal(model, collect(ql2.All()))

	for ql.Len() > 0 {
		ql.RPop()
	}
	ast.Nil(ql.head)
	ast.Nil(ql.tail)
	_, ok := ql.LPop()
	ast.False(ok)
	ast.Empty(collect(ql.All()))
}

func BenchmarkQuickList(b *testing.B) {
	b.Run("rpush-lpop", func(b *testing.B) {
		ql := New()
		for b.Loop() {
			ql.RPush("value-0000000001")
			if ql.Len() > 1000 {
				ql.LPop()
			}
		}
	})
	b.Run("range-tail", func(b *testing.B) {
		ql := New()
		for i := range 100000 {
			ql.RPush(fmt.Sprintf("%016d", i))
		}
		for b.Loop() {
			for range ql.Range(99990, 99999) {
			}
		}
	})
}
