package structx

import (
	"fmt"

	"golang.org/x/exp/slices"
)

type array[T comparable] []T

func (s array[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s array[T]) Len() int {
	return len(s)
}

func (s array[T]) Capacity() int {
	return cap(s)
}

// Top: move value to the top
func (s array[T]) Top(i int) {
	for j := i; j > 0; j-- {
		s.Swap(j, j-1)
	}
}

// Bottom: move value to the bottom
func (s array[T]) Bottom(i int) {
	for j := i; j < s.Len()-1; j++ {
		s.Swap(j, j+1)
	}
}

// Index: return the element of index
func (s array[T]) Index(i int) T {
	return s[i]
}

// Find: return the index of element
func (s array[T]) Find(elem T) int {
	return slices.Index(s, elem)
}

// LShift: Shift all elements of the array left
// exp: [1, 2, 3] => [2, 3, 1]
func (s array[T]) LShift() {
	s.Bottom(0)
}

// RShift: Shift all elements of the array right
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

// Values
func (s array[T]) Values() []T {
	return s
}

// Print
func (s array[T]) Print() {
	fmt.Println(s)
}
