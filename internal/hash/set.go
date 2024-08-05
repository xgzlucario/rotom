package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type SetI interface {
	Add(key string) bool
	Exist(key string) bool
	Remove(key string) bool
	Pop() (key string, ok bool)
	Scan(fn func(key string))
	Len() int
}

var _ SetI = (*Set)(nil)

type Set struct {
	mapset.Set[string]
}

func NewSet() *Set {
	return &Set{mapset.NewThreadUnsafeSetWithSize[string](512)}
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
