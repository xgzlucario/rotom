package zset

import (
	"fmt"
	"iter"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/codec"
)

type sortedSet interface {
	Get(string) (float64, bool)
	Set(string, float64) bool
	Remove(string) bool
	PopMin() (string, float64)
	Rank(string) int
	Len() int
	All() iter.Seq2[string, float64]
	Range(int, int) iter.Seq2[string, float64]
	codec.Encoder
}

func pairs(seq iter.Seq2[string, float64]) []string {
	var res []string
	for m, s := range seq {
		res = append(res, fmt.Sprintf("%s:%v", m, s))
	}
	return res
}

// TestEncodingsAgree runs the same random operations on both encodings and
// requires identical results, including ordering of equal scores.
func TestEncodingsAgree(t *testing.T) {
	ast := assert.New(t)
	var a, b sortedSet = New(), NewZipZSet()
	for i := range 20000 {
		m := fmt.Sprint(rand.IntN(200))
		score := float64(rand.IntN(20)) // many ties
		switch rand.IntN(7) {
		case 0, 1, 2:
			ast.Equal(a.Set(m, score), b.Set(m, score))
		case 3:
			ast.Equal(a.Remove(m), b.Remove(m))
		case 4:
			ast.Equal(a.Rank(m), b.Rank(m))
			s1, ok1 := a.Get(m)
			s2, ok2 := b.Get(m)
			ast.Equal([]any{s1, ok1}, []any{s2, ok2})
		case 5:
			if a.Len() > 0 {
				m1, s1 := a.PopMin()
				m2, s2 := b.PopMin()
				ast.Equal(m1+fmt.Sprint(s1), m2+fmt.Sprint(s2))
			}
		case 6:
			if n := a.Len(); n > 0 {
				start := rand.IntN(n)
				stop := start + rand.IntN(n-start)
				ast.Equal(pairs(a.Range(start, stop)), pairs(b.Range(start, stop)))
			}
		}
		ast.Equal(a.Len(), b.Len())
		if i%2000 == 0 {
			ast.Equal(pairs(a.All()), pairs(b.All()))
		}
	}
	ast.Equal(pairs(a.All()), pairs(b.(*ZipZSet).ToZSet().All()))

	for _, z := range []sortedSet{a, b} {
		w := codec.NewWriter(nil)
		z.Encode(w)
		var z2 sortedSet = New()
		if _, ok := z.(*ZipZSet); ok {
			z2 = NewZipZSet()
		}
		z2.Decode(codec.NewReader(w.Bytes()))
		ast.Equal(pairs(z.All()), pairs(z2.All()))
	}
}

func BenchmarkZSet(b *testing.B) {
	z := New()
	for i := range 100000 {
		z.Set(fmt.Sprintf("%08x", i), float64(i))
	}
	b.Run("rank", func(b *testing.B) {
		for b.Loop() {
			z.Rank("0000ffff")
		}
	})
	b.Run("range-10", func(b *testing.B) {
		for b.Loop() {
			for range z.Range(50000, 50009) {
			}
		}
	})
}

// BenchmarkZAddUpdate mirrors valkey-benchmark's ZADD test: one large zset
// whose random members get random new scores, so nearly every call moves a
// node to another position.
func BenchmarkZAddUpdate(b *testing.B) {
	const n = 100000
	z := New()
	members := make([]string, n)
	for i := range n {
		members[i] = fmt.Sprintf("element:%012d", i)
		z.Set(members[i], float64(rand.IntN(n)))
	}
	r := rand.New(rand.NewPCG(1, 2))
	b.ReportAllocs()
	for b.Loop() {
		z.Set(members[r.IntN(n)], float64(r.IntN(n)))
	}
}
