package structx

import (
	mapset "github.com/deckarep/golang-set/v2"
)

// Set
type Set[T comparable] struct {
	mapset.Set[T]
}

// NewSet
func NewSet[T comparable](val ...T) *Set[T] {
	return &Set[T]{mapset.NewSet[T](val...)}
}

// Clone
func (s *Set[T]) Clone() *Set[T] {
	return &Set[T]{s.Set.Clone()}
}

// Union
func (s *Set[T]) Union(other *Set[T]) {
	s.Set = s.Set.Union(other.Set)
}

// Intersect
func (s *Set[T]) Intersect(other *Set[T]) {
	s.Set = s.Set.Intersect(other.Set)
}

// Difference
func (s *Set[T]) Difference(other *Set[T]) {
	s.Set = s.Set.SymmetricDifference(other.Set)
}
