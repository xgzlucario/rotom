package structx

import (
	mapset "github.com/deckarep/golang-set/v2"
)

// Set
type Set struct {
	mapset.Set[string]
}

// NewSet
func NewSet() *Set {
	return &Set{mapset.NewSet[string]()}
}

// Clone
func (s *Set) Clone() *Set {
	return &Set{s.Set.Clone()}
}

// Union
func (s *Set) Union(other *Set) {
	s.Set = s.Set.Union(other.Set)
}

// Intersect
func (s *Set) Intersect(other *Set) {
	s.Set = s.Set.Intersect(other.Set)
}

// Difference
func (s *Set) Difference(other *Set) {
	s.Set = s.Set.SymmetricDifference(other.Set)
}
