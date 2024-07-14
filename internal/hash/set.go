package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type SetI interface {
	Add(key string) (ok bool)
	Remove(key string) (ok bool)
	Pop() (key string, ok bool)
	Scan(fn func(string))
	Len() int
}

var _ SetI = (*Set)(nil)

type Set struct {
	mapset.Set[string]
}

func NewSet() *Set {
	return &Set{mapset.NewThreadUnsafeSetWithSize[string](64)}
}

func (s Set) Remove(key string) bool {
	if !s.ContainsOne(key) {
		return false
	}
	s.Set.Remove(key)
	return true
}

func (s Set) Len() int {
	return s.Cardinality()
}

func (s Set) Scan(fn func(string)) {
	s.Set.Each(func(s string) bool {
		fn(s)
		return false
	})
}
