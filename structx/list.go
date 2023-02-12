package structx

import (
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/slices"
)

type List[T comparable] struct {
	array[T]
}

// NewList: return new List
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
	if len(ls.array) == 0 {
		return
	}
	val = ls.array[ls.Len()-1]
	ls.array = ls.array[:len(ls.array)-1]
	return val, true
}

// RemoveFirst
func (ls *List[T]) RemoveFirst(elem T) bool {
	for i, v := range ls.array {
		if v == elem {
			ls.remove(i)
			return true
		}
	}
	return false
}

// RemoveIndex
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

// Max: Input param is Less function
func (ls *List[T]) Max(less func(T, T) bool) T {
	max := ls.array[0]
	for _, v := range ls.array {
		if less(max, v) {
			max = v
		}
	}
	return max
}

// Min: Input param is Less function
func (ls *List[T]) Min(less func(T, T) bool) T {
	min := ls.array[0]
	for _, v := range ls.array {
		if less(v, min) {
			min = v
		}
	}
	return min
}

// Sum
func (ls *List[T]) Sum(f func(T) float64) float64 {
	var sum float64
	for _, v := range ls.array {
		sum += f(v)
	}
	return sum
}

// Mean
func (ls *List[T]) Mean(f func(T) float64) float64 {
	return ls.Sum(f) / float64(ls.Len())
}

// Sort: Input param is Order function
func (ls *List[T]) Sort(f func(T, T) bool) *List[T] {
	slices.SortFunc(ls.array, f)
	return ls
}

// IsSorted: Input param is Order function
func (ls *List[T]) IsSorted(f func(T, T) bool) bool {
	return slices.IsSortedFunc(ls.array, f)
}

// Filter
func (ls *List[T]) Filter(f func(T) bool) *List[T] {
	newLs := NewList[T]()
	for _, v := range ls.array {
		if f(v) {
			newLs.RPush(v)
		}
	}
	return newLs
}

// Compact replaces consecutive runs of equal elements with a single copy.
func (s *List[T]) Compact() {
	s.array = slices.Compact(s.array)
}

// Clip removes unused capacity from the slice.
func (s *List[T]) Clip() {
	s.array = slices.Clip(s.array)
}

func (s *List[T]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(s.array)
}

func (s *List[T]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, &s.array)
}
