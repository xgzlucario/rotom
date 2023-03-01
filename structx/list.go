package structx

import (
	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/slices"
)

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

// Max
func (ls *List[T]) Max(less func(T, T) bool) T {
	max := ls.array[0]
	for _, v := range ls.array {
		if less(max, v) {
			max = v
		}
	}
	return max
}

// Min
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

// Sort
func (ls *List[T]) Sort(less func(T, T) bool) *List[T] {
	slices.SortFunc(ls.array, less)
	return ls
}

// IsSorted
func (ls *List[T]) IsSorted(less func(T, T) bool) bool {
	return slices.IsSortedFunc(ls.array, less)
}

// Filter
func (ls *List[T]) Filter(filter func(T) bool) *List[T] {
	nls := NewList[T]()
	for _, v := range ls.array {
		if filter(v) {
			nls.RPush(v)
		}
	}
	return nls
}

func (s *List[T]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(s.array)
}

func (s *List[T]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, &s.array)
}
