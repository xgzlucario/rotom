package structx

import (
	"unsafe"

	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/slices"
)

type List[T comparable] []T

// NewList
func NewList[T comparable](values ...T) *List[T] {
	s := make(List[T], 0, len(values))
	copy(s, values)
	return &s
}

func (s *List[T]) Swap(i, j int) {
	(*s)[i], (*s)[j] = (*s)[j], (*s)[i]
}

func (s *List[T]) Len() int {
	return len(*s)
}

func (s *List[T]) ByteSize() int {
	var a T
	return int(unsafe.Sizeof(a)) * len(*s)
}

// Top: move value to the top
func (s *List[T]) Top(i int) {
	for j := i; j > 0; j-- {
		s.Swap(j, j-1)
	}
}

// Bottom: move value to the bottom
func (s *List[T]) Bottom(i int) {
	for j := i; j < s.Len()-1; j++ {
		s.Swap(j, j+1)
	}
}

// Index: return the element of index
func (s *List[T]) Index(i int) T {
	return (*s)[i]
}

// Find: return the index of element
func (s *List[T]) Find(elem T) int {
	return slices.Index(*s, elem)
}

// LShift: Shift all elements of the array left
// exp: [1, 2, 3] => [2, 3, 1]
func (s *List[T]) LShift() {
	s.Bottom(0)
}

// RShift: Shift all elements of the array right
// exp: [1, 2, 3] => [3, 1, 2]
func (s *List[T]) RShift() {
	s.Top(s.Len() - 1)
}

// Reverse
func (s *List[T]) Reverse() {
	l, r := 0, s.Len()-1
	for l < r {
		s.Swap(l, r)
		l++
		r--
	}
}

// Copy
func (s *List[T]) Copy() *List[T] {
	news := make(List[T], 0, len(*s))
	copy(*s, news)
	return &news
}

// Range
func (s *List[T]) Range(f func(T) bool) {
	for _, v := range *s {
		if f(v) {
			return
		}
	}
}

// LPush
func (s *List[T]) LPush(values ...T) {
	*s = append(values, *s...)
}

// RPush
func (s *List[T]) RPush(values ...T) {
	*s = append(*s, values...)
}

// Insert
func (s *List[T]) Insert(i int, values ...T) {
	*s = slices.Insert(*s, i, values...)
}

// AddToSet
func (s *List[T]) AddToSet(value T) bool {
	if r := s.Find(value); r < 0 {
		s.RPush(value)
		return true
	}
	return false
}

// LPop
func (s *List[T]) LPop() (val T, ok bool) {
	if len(*s) == 0 {
		return
	}
	val = (*s)[0]
	(*s) = (*s)[1:]
	return val, true
}

// RPop
func (s *List[T]) RPop() (val T, ok bool) {
	n := len(*s)
	if n == 0 {
		return
	}
	val = (*s)[n-1]
	(*s) = (*s)[:n-1]
	return val, true
}

// RemoveFirst remove elem
func (s *List[T]) RemoveFirst(elem T) bool {
	for i, v := range *s {
		if v == elem {
			s.remove(i)
			return true
		}
	}
	return false
}

// RemoveIndex remove elem by index
func (s *List[T]) RemoveIndex(i int) bool {
	if i > 0 && i < len(*s) {
		s.remove(i)
		return true
	}
	return false
}

// remove with zero memory allocation
func (s *List[T]) remove(i int) {
	if i > len(*s)/2 {
		s.Bottom(i)
		s.RPop()

	} else {
		s.Top(i)
		s.LPop()
	}
}

// Max
func (s *List[T]) Max(less func(T, T) bool) T {
	max := (*s)[0]
	for _, v := range *s {
		if less(max, v) {
			max = v
		}
	}
	return max
}

// Min
func (s *List[T]) Min(less func(T, T) bool) T {
	min := (*s)[0]
	for _, v := range *s {
		if less(v, min) {
			min = v
		}
	}
	return min
}

// Sort
func (s *List[T]) Sort(less func(T, T) bool) *List[T] {
	slices.SortFunc(*s, less)
	return s
}

// Values
func (s *List[T]) Values() []T {
	return *s
}

// IsSorted
func (s *List[T]) IsSorted(less func(T, T) bool) bool {
	return slices.IsSortedFunc(*s, less)
}

func (s *List[T]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(*s)
}

func (s *List[T]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, s)
}
