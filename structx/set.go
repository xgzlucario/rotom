package structx

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type Set[T comparable] struct {
	mapset.Set[T]
}

// NewSet
func NewSet[T comparable](vals ...T) Set[T] {
	return Set[T]{mapset.NewSet(vals...)}
}
