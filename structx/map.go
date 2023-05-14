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

type entry[K comparable, V any] struct {
	K []K
	V []V
}

// MarshalJSON
func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	e := entry[K, V]{
		K: make([]K, 0, m.Len()),
		V: make([]V, 0, m.Len()),
	}
	m.Scan(func(key K, value V) bool {
		e.K = append(e.K, key)
		e.V = append(e.V, value)
		return true
	})

	return sonic.Marshal(e)
}

// UnmarshalJSON
func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	var e entry[K, V]
	if err := sonic.Unmarshal(src, &e); err != nil {
		return err
	}

	for i, k := range e.K {
		m.Set(k, e.V[i])
	}
	return nil
}

// Set
type Set[K comparable] struct {
	*hashmap.Set[K]
}

// NewSet
func NewSet[K comparable]() Set[K] {
	return Set[K]{&hashmap.Set[K]{}}
}
