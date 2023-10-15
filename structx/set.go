package structx

import (
	"sync"

	"github.com/dolthub/swiss"
)

type Set[T comparable] struct {
	sync.RWMutex
	m *swiss.Map[T, struct{}]
}

// NewSet
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{sync.RWMutex{}, swiss.NewMap[T, struct{}](8)}
}

// Add
func (s *Set[T]) Add(v T) bool {
	s.Lock()
	defer s.Unlock()

	if s.m.Has(v) {
		return false
	}
	s.m.Put(v, struct{}{})

	return true
}

// Remove
func (s *Set[T]) Remove(v T) bool {
	s.Lock()
	defer s.Unlock()

	if !s.m.Has(v) {
		return false
	}
	s.m.Delete(v)

	return true
}

// Contains
func (s *Set[T]) Contains(v T) bool {
	s.RLock()
	ok := s.m.Has(v)
	s.RUnlock()
	return ok
}

// Cardinality
func (s *Set[T]) Cardinality() int {
	s.RLock()
	n := s.m.Count()
	s.RUnlock()
	return n
}

// Clone
func (s *Set[T]) Clone() *Set[T] {
	s.RLock()
	defer s.RUnlock()

	cl := NewSet[T]()
	s.m.Iter(func(k T, _ struct{}) bool {
		cl.Add(k)
		return false
	})

	return cl
}

// Union
func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	s.Lock()
	other.m.Iter(func(k T, _ struct{}) bool {
		s.Add(k)
		return false
	})
	s.Unlock()

	return s
}

// Intersect
func (s *Set[T]) Intersect(other *Set[T]) *Set[T] {
	s.Lock()
	other.m.Iter(func(k T, _ struct{}) bool {
		if !s.m.Has(k) {
			s.Remove(k)
		}
		return false
	})
	s.Unlock()

	return s
}

// Difference
func (s *Set[T]) Difference(other *Set[T]) *Set[T] {
	s.Lock()
	other.m.Iter(func(k T, _ struct{}) bool {
		s.Remove(k)
		return false
	})
	s.Unlock()

	return s
}
