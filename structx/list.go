package structx

import (
	"slices"

	"github.com/bytedance/sonic"
)

// List
type List[T comparable] struct {
	array[T]
}

// NewList
func NewList[T comparable](values ...T) *List[T] {
	return &List[T]{slices.Clone(values)}
}

// LPush
func (ls *List[T]) LPush(values ...T) {
	ls.array = append(values, ls.array...)
}

// RPush
func (ls *List[T]) RPush(values ...T) {
	ls.array = append(ls.array, values...)
}

// Insert
func (ls *List[T]) Insert(i int, values ...T) {
	ls.array = slices.Insert(ls.array, i, values...)
}

// LPop
func (ls *List[T]) LPop() (val T, ok bool) {
	if len(ls.array) == 0 {
		return
	}
	val = ls.array[0]
	ls.array = ls.array[1:]
	return val, true
}

// RPop
func (ls *List[T]) RPop() (val T, ok bool) {
	n := len(ls.array)
	if n == 0 {
		return
	}
	val = ls.array[n-1]
	ls.array = ls.array[:n-1]
	return val, true
}

// RemoveFirst remove elem
func (ls *List[T]) RemoveFirst(elem T) bool {
	for i, v := range ls.array {
		if v == elem {
			ls.remove(i)
			return true
		}
	}
	return false
}

// RemoveIndex remove elem by index
func (ls *List[T]) RemoveIndex(i int) bool {
	if i > 0 && i < len(ls.array) {
		ls.remove(i)
		return true
	}
	return false
}

// remove with zero memory allocation
func (ls *List[T]) remove(i int) {
	if i > len(ls.array)/2 {
		ls.Bottom(i)
		ls.RPop()

	} else {
		ls.Top(i)
		ls.LPop()
	}
}

// Max return max with less return t1 < t2
func (ls *List[T]) Max(less func(t1, t2 T) bool) T {
	max := ls.array[0]
	for _, v := range ls.array {
		if less(max, v) {
			max = v
		}
	}
	return max
}

// Min return min with less return t1 < t2
func (ls *List[T]) Min(less func(t1, t2 T) bool) T {
	min := ls.array[0]
	for _, v := range ls.array {
		if less(v, min) {
			min = v
		}
	}
	return min
}

// Sort
func (ls *List[T]) Sort(f func(T, T) int) *List[T] {
	slices.SortFunc(ls.array, f)
	return ls
}

// IsSorted
func (ls *List[T]) IsSorted(f func(T, T) int) bool {
	return slices.IsSortedFunc(ls.array, f)
}

// array
type array[T comparable] []T

// Swap
func (s array[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Len
func (s array[T]) Len() int {
	return len(s)
}

// Capacity
func (s array[T]) Capacity() int {
	return cap(s)
}

// Top move value to the top
func (s array[T]) Top(i int) {
	for j := i; j > 0; j-- {
		s.Swap(j, j-1)
	}
}

// Bottom move value to the bottom
func (s array[T]) Bottom(i int) {
	for j := i; j < s.Len()-1; j++ {
		s.Swap(j, j+1)
	}
}

// Index return the element of index
func (s array[T]) Index(i int) T {
	return s[i]
}

// Find return the index of element
func (s array[T]) Find(elem T) int {
	return slices.Index(s, elem)
}

// LShift Shift all elements of the array left
// exp: [1, 2, 3] => [2, 3, 1]
func (s array[T]) LShift() {
	s.Bottom(0)
}

// RShift Shift all elements of the array right
// exp: [1, 2, 3] => [3, 1, 2]
func (s array[T]) RShift() {
	s.Top(s.Len() - 1)
}

// Reverse
func (s array[T]) Reverse() {
	l, r := 0, s.Len()-1
	for l < r {
		s.Swap(l, r)
		l++
		r--
	}
}

// Copy
func (s array[T]) Copy() array[T] {
	return slices.Clone(s)
}

// Range
func (s array[T]) Range(f func(T) bool) {
	for _, v := range s {
		if f(v) {
			return
		}
	}
}

// Values return slice of array
func (s array[T]) Values() []T {
	return s
}

func (s *List[T]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(s.array)
}

func (s *List[T]) UnmarshalJSON(src []byte) error {
	return sonic.Unmarshal(src, &s.array)
}
