package structx

import (
	"github.com/bytedance/sonic"
	"github.com/tidwall/hashmap"
)

// Map
type Map[K comparable, V any] struct {
	*hashmap.Map[K, V]
}

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{&hashmap.Map[K, V]{}}
}

// NewMapWithCap
func NewMapWithCap[K comparable, V any](cap int) Map[K, V] {
	return Map[K, V]{hashmap.New[K, V](cap)}
}

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return sonic.Unmarshal(src, m)
}

// Set
type Set[K comparable] struct {
	*hashmap.Set[K]
}

// NewSet
func NewSet[K comparable]() Set[K] {
	return Set[K]{&hashmap.Set[K]{}}
}
