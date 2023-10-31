package structx

import (
	"encoding/json"
	"sync"
)

type Set[T comparable] struct {
	sync.RWMutex
	m Map[T, struct{}]
}

// NewSet
func NewSet[T comparable](args ...T) *Set[T] {
	defaultCap := 8
	if len(args) > defaultCap {
		defaultCap = len(args)
	}

	s := &Set[T]{m: NewMap[T, struct{}](defaultCap)}
	for _, v := range args {
		s.m.Put(v, struct{}{})
	}
	return s
}

// Add
func (s *Set[T]) Add(v T) {
	s.Lock()
	s.m.Put(v, struct{}{})
	s.Unlock()
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

	return &Set[T]{m: s.m.Clone()}
}

// Union
func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	if s == other {
		return s
	}

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
	if s == other {
		return s
	}

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
	if s == other {
		s.m.Clear()
		return s
	}

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
