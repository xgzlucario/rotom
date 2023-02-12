package structx

import (
	"unsafe"

	"github.com/xgzlucario/rotom/base"
	"golang.org/x/exp/maps"
)

type Map[K comparable, V any] map[K]V

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{}
}

// Get
func (m Map[K, V]) Get(key K) (V, bool) {
	v, ok := m[key]
	return v, ok
}

// Set
func (m Map[K, V]) Set(key K, value V) {
	m[key] = value
}

// Exist
func (m Map[K, V]) Exist(key K) bool {
	_, ok := m[key]
	return ok
}

// Delete
func (m Map[K, V]) Delete(key K) error {
	_, ok := m[key]
	if ok {
		delete(m, key)
		return nil
	}
	return base.ErrKeyNotFound(key)
}

// Range
func (m Map[K, V]) Range(f func(K, V) bool) {
	for k, v := range m {
		if f(k, v) {
			return
		}
	}
}

// Clear
func (m Map[K, V]) Clear() {
	maps.Clear(m)
}

// Copy
func (m Map[K, V]) Copy() Map[K, V] {
	return maps.Clone(m)
}

// Keys
func (m Map[K, V]) Keys() []K {
	return maps.Keys(m)
}

// Values
func (m Map[K, V]) Values() []V {
	return maps.Values(m)
}

// Len
func (m Map[K, V]) Len() int {
	return len(m)
}

// ByteSize
func (m Map[K, V]) ByteSize() int {
	var k K
	var v V
	return int(unsafe.Sizeof(k)+unsafe.Sizeof(v)) * len(m)
}

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, m)
}
