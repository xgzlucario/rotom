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
func (ls *List[T]) LPush(values ...T) {
	*ls = append(values, *ls...)
}

// RPush
func (ls *List[T]) RPush(values ...T) {
	*ls = append(*ls, values...)
}

// Insert
func (ls *List[T]) Insert(i int, values ...T) {
	*ls = slices.Insert(*ls, i, values...)
}

// AddToSet
func (ls *List[T]) AddToSet(value T) bool {
	if r := ls.Find(value); r < 0 {
		ls.RPush(value)
		return true
	}
	return false
}

// LPop
func (ls *List[T]) LPop() (val T, ok bool) {
	if len(*ls) == 0 {
		return
	}
	val = (*ls)[0]
	(*ls) = (*ls)[1:]
	return val, true
}

// RPop
func (ls *List[T]) RPop() (val T, ok bool) {
	n := len(*ls)
	if n == 0 {
		return
	}
	val = (*ls)[n-1]
	(*ls) = (*ls)[:n-1]
	return val, true
}

// RemoveFirst remove elem
func (ls *List[T]) RemoveFirst(elem T) bool {
	for i, v := range *ls {
		if v == elem {
			ls.remove(i)
			return true
		}
	}
	return false
}

// RemoveIndex remove elem by index
func (ls *List[T]) RemoveIndex(i int) bool {
	if i > 0 && i < len(*ls) {
		ls.remove(i)
		return true
	}
	return false
}

// remove with zero memory allocation
func (ls *List[T]) remove(i int) {
	if i > len(*ls)/2 {
		ls.Bottom(i)
		ls.RPop()

	} else {
		ls.Top(i)
		ls.LPop()
	}
}

// Max
func (ls *List[T]) Max(less func(T, T) bool) T {
	max := (*ls)[0]
	for _, v := range *ls {
		if less(max, v) {
			max = v
		}
	}
	return max
}

// Min
func (ls *List[T]) Min(less func(T, T) bool) T {
	min := (*ls)[0]
	for _, v := range *ls {
		if less(v, min) {
			min = v
		}
	}
	return min
}

// Sort
func (ls *List[T]) Sort(less func(T, T) bool) *List[T] {
	slices.SortFunc(*ls, less)
	return ls
}

// Values
func (s *List[T]) Values() []T {
	return *s
}

// IsSorted
func (ls *List[T]) IsSorted(less func(T, T) bool) bool {
	return slices.IsSortedFunc(*ls, less)
}

func (s *List[T]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(*s)
}

func (s *List[T]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, s)
}
