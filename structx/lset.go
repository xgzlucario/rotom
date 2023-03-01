package structx

import (
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

/*
LSet (ListSet): map + list structure
LSet has richer api and faster Intersect, Union, Range operations than mapset
*/
type LSet[T comparable] struct {
	m Map[T, struct{}]
	*List[T]
}

// NewLSet: Create a new LSet
func NewLSet[T comparable](values ...T) *LSet[T] {
	ls := &LSet[T]{
		m:    NewMap[T, struct{}](),
		List: NewList[T](),
	}
	for _, v := range values {
		ls.Add(v)
	}
	return ls
}

// Add
func (s *LSet[T]) Add(key T) bool {
	if !s.Exist(key) {
		s.add(key)
		return true
	}
	return false
}

func (s *LSet[T]) add(key T) {
	s.m[key] = struct{}{}
	s.RPush(key)
}

// Remove
func (s *LSet[T]) Remove(key T) bool {
	if s.Exist(key) {
		s.remove(key)
		return true
	}
	return false
}

func (s *LSet[T]) remove(key T) {
	delete(s.m, key)
	s.RemoveFirst(key)
}

// Exist
func (s *LSet[T]) Exist(key T) bool {
	_, ok := s.m[key]
	return ok
}

// Copy
func (s *LSet[T]) Copy() *LSet[T] {
	return &LSet[T]{
		m:    maps.Clone(s.m),
		List: &List[T]{slices.Clone(s.array)},
	}
}

// Equal: Compare members between two lsets is equal
func (s *LSet[T]) Equal(t *LSet[T]) bool {
	return maps.Equal(s.m, t.m)
}

// Union: Return the union of two sets
func (s *LSet[T]) Union(t *LSet[T]) *LSet[T] {
	min, max := s.compareLength(t)
	// should copy max object
	max = max.Copy()

	for _, k := range min.array {
		max.Add(k)
	}
	return max
}

// Intersect
func (s *LSet[T]) Intersect(t *LSet[T]) *LSet[T] {
	min, max := s.compareLength(t)
	// should copy min object
	min = min.Copy()

	for _, k := range min.array {
		if !max.Exist(k) {
			min.remove(k)
		}
	}
	return min
}

// Difference
func (s *LSet[T]) Difference(t *LSet[T]) *LSet[T] {
	newS := NewLSet[T]()

	for _, key := range s.array {
		if !t.Exist(key) {
			newS.add(key)
		}
	}
	for _, key := range t.array {
		if !s.Exist(key) {
			newS.add(key)
		}
	}
	return newS
}

// IsSubSet
func (s *LSet[T]) IsSubSet(t *LSet[T]) bool {
	if t.Len() > s.Len() {
		return false
	}
	for _, v := range t.array {
		if !s.Exist(v) {
			return false
		}
	}
	return true
}

// LPop: Pop a elem from left
func (s *LSet[T]) LPop() (key T, ok bool) {
	key, ok = s.List.LPop()
	if !ok {
		return
	}
	delete(s.m, key)
	return key, true
}

// RPop: Pop a elem from right
func (s *LSet[T]) RPop() (key T, ok bool) {
	key, ok = s.List.RPop()
	if !ok {
		return
	}
	delete(s.m, key)
	return key, true
}

// RandomPop: Pop a random elem
func (s *LSet[T]) RandomPop() (key T, ok bool) {
	if len(s.m) == 0 {
		return
	}
	for k := range s.m {
		key = k
		break
	}
	delete(s.m, key)
	s.RemoveFirst(key)
	return key, true
}

// Compare two lset length and return (*min, *max)
func (s1 *LSet[T]) compareLength(s2 *LSet[T]) (*LSet[T], *LSet[T]) {
	if s1.Len() < s2.Len() {
		return s1, s2
	}
	return s2, s1
}

func (s *LSet[T]) UnmarshalJSON(src []byte) error {
	if err := base.UnmarshalJSON(src, &s); err != nil {
		return err
	}
	*s = *NewLSet(s.array...)
	return nil
}
