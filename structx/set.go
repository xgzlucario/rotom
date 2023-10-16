package structx

import (
	"encoding/json"
	"sync"

	"github.com/dolthub/swiss"
)

type Set[T comparable] struct {
	sync.RWMutex
	m *swiss.Map[T, struct{}]
}

// NewSet
func NewSet[T comparable](args ...T) *Set[T] {
	defaultCap := 8
	if len(args) > defaultCap {
		defaultCap = len(args)
	}

	s := &Set[T]{m: swiss.NewMap[T, struct{}](uint32(defaultCap))}
	for _, v := range args {
		s.m.Put(v, struct{}{})
	}
	return s
}

func (s *Set[T]) add(v T) bool {
	if s.m.Has(v) {
		return false
	}
	s.m.Put(v, struct{}{})

	return true
}

// Add
func (s *Set[T]) Add(v T) bool {
	s.Lock()
	ok := s.add(v)
	s.Unlock()
	return ok
}

// Remove
func (s *Set[T]) Remove(v T) bool {
	s.Lock()
	ok := s.m.Delete(v)
	s.Unlock()
	return ok
}

// Has
func (s *Set[T]) Has(v T) bool {
	s.RLock()
	ok := s.m.Has(v)
	s.RUnlock()
	return ok
}

// Len
func (s *Set[T]) Len() int {
	s.RLock()
	n := s.m.Count()
	s.RUnlock()
	return n
}

// ToSlice
func (s *Set[T]) ToSlice() []T {
	s.RLock()
	defer s.RUnlock()

	sl := make([]T, 0, s.m.Count())
	s.m.Iter(func(k T, _ struct{}) bool {
		sl = append(sl, k)
		return false
	})

	return sl
}

// Clone
func (s *Set[T]) Clone() *Set[T] {
	s.RLock()
	defer s.RUnlock()

	m := swiss.NewMap[T, struct{}](uint32(s.m.Count()))
	s.m.Iter(func(k T, _ struct{}) bool {
		m.Put(k, struct{}{})
		return false
	})

	return &Set[T]{m: m}
}

// Union
func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	s.Lock()
	other.m.Iter(func(k T, _ struct{}) bool {
		s.m.Put(k, struct{}{})
		return false
	})
	s.Unlock()

	return s
}

// Intersect
func (s *Set[T]) Intersect(other *Set[T]) *Set[T] {
	s.Lock()
	other.RLock()

	s.m.Iter(func(k T, _ struct{}) bool {
		if !other.m.Has(k) {
			s.m.Delete(k)
		}
		return false
	})

	other.RUnlock()
	s.Unlock()

	return s
}

// Difference
func (s *Set[T]) Difference(other *Set[T]) *Set[T] {
	s.Lock()
	other.RLock()

	other.m.Iter(func(k T, _ struct{}) bool {
		if s.m.Has(k) {
			s.m.Delete(k)
		} else {
			s.m.Put(k, struct{}{})
		}
		return false
	})

	other.RUnlock()
	s.Unlock()

	return s
}

// MarshalJSON
func (s *Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.ToSlice())
}

// UnmarshalJSON
func (s *Set[T]) UnmarshalJSON(src []byte) error {
	var tmp []T
	if err := json.Unmarshal(src, &tmp); err != nil {
		return err
	}

	*s = *NewSet(tmp...)
	return nil
}
