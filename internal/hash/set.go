package hash

import (
	mapset "github.com/deckarep/golang-set/v2"
)

const (
	defaultSetSize = 512
)

type SetI interface {
	Add(key string) bool
	Exist(key string) bool
	Remove(key string) bool
	Pop() (key string, ok bool)
	Scan(fn func(key string))
	Len() int
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

var _ SetI = (*Set)(nil)

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

func (s Set) Marshal() ([]byte, error) {
	return s.MarshalJSON()
}

func (s Set) Unmarshal(src []byte) error {
	return s.UnmarshalJSON(src)
}
