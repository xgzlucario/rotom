package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type SetI interface {
	Add(key string) (ok bool)
	Remove(key string) (ok bool)
	Pop() (key string, ok bool)
	Len() int
}

var _ SetI = (*Set)(nil)

type Set struct {
	mapset.Set[string]
}

func NewSet() *Set {
	return &Set{mapset.NewThreadUnsafeSet[string]()}
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
