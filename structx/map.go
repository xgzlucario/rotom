package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/tidwall/hashmap"
)

// ==================== Map ====================
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
func (m *Map[K, V]) MarshalJSON() ([]byte, error) {
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
func (m *Map[K, V]) UnmarshalJSON(src []byte) error {
	var e entry[K, V]
	if err := sonic.Unmarshal(src, &e); err != nil {
		return err
	}

	for i, k := range e.K {
		m.Set(k, e.V[i])
	}
	return nil
}

// ================== SyncMap ==================
type SyncMap[K comparable, V any] struct {
	m Map[K, V]
	sync.RWMutex
}

// NewSyncMap
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{NewMap[K, V](), sync.RWMutex{}}
}

// Get
func (m *SyncMap[K, V]) Get(key K) (V, bool) {
	m.RLock()
	defer m.RUnlock()
	return m.m.Get(key)
}

// Set
func (m *SyncMap[K, V]) Set(key K, value V) {
	m.Lock()
	defer m.Unlock()
	m.m.Set(key, value)
}

// Delete
func (m *SyncMap[K, V]) Delete(key K) {
	m.Lock()
	defer m.Unlock()
	m.m.Delete(key)
}

// Keys
func (m *SyncMap[K, V]) Keys() []K {
	m.RLock()
	defer m.RUnlock()
	return m.m.Keys()
}

// MarshalJSON
func (m *SyncMap[K, V]) MarshalJSON() ([]byte, error) {
	m.RLock()
	defer m.RUnlock()
	return m.m.MarshalJSON()
}

// UnmarshalJSON
func (m *SyncMap[K, V]) UnmarshalJSON(src []byte) error {
	m.Lock()
	defer m.Unlock()
	return m.m.UnmarshalJSON(src)
}

// ==================== Set ====================
type Set[K comparable] struct {
	*hashmap.Set[K]
}

// NewSet
func NewSet[K comparable]() Set[K] {
	return Set[K]{&hashmap.Set[K]{}}
}
