package structx

import (
	"sync"

	"github.com/xgzlucario/rotom/base"
)

// SyncMap
type SyncMap[K comparable, V any] struct {
	m  Map[K, V]
	mu sync.RWMutex
}

// NewSyncMap
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		m: Map[K, V]{},
	}
}

// Get
func (m *SyncMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.m[key]
	return val, ok
}

// Set
func (m *SyncMap[K, V]) Set(key K, val V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[key] = val
}

// Remove
func (m *SyncMap[K, V]) Remove(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.m[key]
	if ok {
		delete(m.m, key)
	}
	return ok
}

// Keys
func (m *SyncMap[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]K, 0, len(m.m))
	for k := range m.m {
		keys = append(keys, k)
	}
	return keys
}

// Count
func (m *SyncMap[K, V]) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.m)
}

// Clear
func (m *SyncMap[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m = Map[K, V]{}
}

// MarshalJSON
func (m *SyncMap[K, V]) MarshalJSON() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return base.MarshalJSON(m.m)
}

// UnmarshalJSON
func (m *SyncMap[K, V]) UnmarshalJSON(src []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return base.UnmarshalJSON(src, &m.m)
}
