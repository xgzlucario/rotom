package zset

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// checkInvariants verifies ordering, backward links and that every span
// equals the number of level-0 steps it skips.
func checkInvariants(t *testing.T, sl *skiplist) {
	t.Helper()
	pos := map[*skiplistNode]int{sl.head: 0}
	var prev *skiplistNode
	i := 0
	for x := sl.first(); x != nil; x = x.level[0].forward {
		i++
		pos[x] = i
		if x.backward != prev {
			t.Fatalf("bad backward link at %d", i)
		}
		if prev != nil && !prev.less(x.score, x.member) {
			t.Fatalf("not sorted at %d", i)
		}
		prev = x
	}
	if i != sl.length || sl.tail != prev {
		t.Fatalf("length/tail mismatch")
	}
	for x := sl.head; x != nil; x = x.level[0].forward {
		for lv := 0; lv < len(x.level) && lv < sl.level; lv++ {
			f := x.level[lv].forward
			want := sl.length - pos[x] // to the end
			if f != nil {
				want = pos[f] - pos[x]
			}
			if f != nil && x.level[lv].span != want {
				t.Fatalf("bad span at pos %d level %d: %d != %d", pos[x], lv, x.level[lv].span, want)
			}
		}
	}
}

func TestSkiplistRank(t *testing.T) {
	ast := assert.New(t)
	z := New()
	ref := map[string]float64{}
	for i := range 5000 {
		key := fmt.Sprint(rand.IntN(2000))
		switch i % 3 {
		case 0, 1:
			score := float64(rand.IntN(100)) // many equal scores: ties sort by member
			z.Set(key, score)
			ref[key] = score
		case 2:
			z.Remove(key)
			delete(ref, key)
		}
	}
	checkInvariants(t, z.skl)

	type kv struct {
		k string
		s float64
	}
	var sorted []kv
	for k, s := range ref {
		sorted = append(sorted, kv{k, s})
	}
	slices.SortFunc(sorted, func(a, b kv) int {
		if a.s != b.s {
			if a.s < b.s {
				return -1
			}
			return 1
		}
		if a.k < b.k {
			return -1
		}
		if a.k > b.k {
			return 1
		}
		return 0
	})
	ast.Equal(len(sorted), z.Len())
	for i, e := range sorted {
		ast.Equal(i, z.Rank(e.k))
	}
	ast.Equal(-1, z.Rank("missing"))

	var got []string
	z.Range(10, 20, func(k string, _ float64) { got = append(got, k) })
	var want []string
	for _, e := range sorted[10:21] {
		want = append(want, e.k)
	}
	ast.Equal(want, got)
}
