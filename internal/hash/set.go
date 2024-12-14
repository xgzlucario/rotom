package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/xgzlucario/rotom/internal/iface"
)

const (
	defaultSetSize = 512
)

var _ iface.SetI = (*Set)(nil)

type Set struct {
	mapset.Set[string]
}

func NewSet() *Set {
	return &Set{mapset.NewThreadUnsafeSetWithSize[string](defaultSetSize)}
}

func (s Set) Remove(key string) bool {
	if !s.Exist(key) {
		return false
	}
	s.Set.Remove(key)
	return true
}

func (s Set) Scan(fn func(string)) {
	s.Set.Each(func(s string) bool {
		fn(s)
		return false
	})
}

func (s Set) Exist(key string) bool { return s.Set.ContainsOne(key) }

func (s Set) Len() int { return s.Cardinality() }

func (s Set) ReadFrom(rd *iface.Reader) {
	n := rd.ReadUint64()
	for range n {
		s.Add(rd.ReadString())
	}
}

// WriteTo encode set to [klen, key, ...].
func (s Set) WriteTo(w *iface.Writer) {
	w.WriteUint64(uint64(s.Len()))
	s.Scan(func(key string) {
		w.WriteString(key)
	})
}
