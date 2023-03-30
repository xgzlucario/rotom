package structx

import (
	"github.com/bytedance/sonic"
	"golang.org/x/exp/maps"
)

type Map[K comparable, V any] map[K]V

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return make(Map[K, V])
}

// Get
func (m Map[K, V]) Get(key K) (V, bool) {
	v, ok := m[key]
	return v, ok
}

// Set
func (m Map[K, V]) Set(key K, val V) {
	m[key] = val
}

// Remove
func (m Map[K, V]) Remove(key K) (V, bool) {
	v, ok := m[key]
	if ok {
		delete(m, key)
	}
	return v, ok
}

// Keys
func (m Map[K, V]) Keys() []K {
	return maps.Keys(m)
}

// Values
func (m Map[K, V]) Values() []V {
	return maps.Values(m)
}

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return sonic.Unmarshal(src, m)
}
