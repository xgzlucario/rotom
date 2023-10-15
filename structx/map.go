package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/dolthub/swiss"
)

// Map
type Map[K comparable, V any] struct {
	*swiss.Map[K, V]
}

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{swiss.NewMap[K, V](8)}
}

type entry[K comparable, V any] struct {
	K []K
	V []V
}

// MarshalJSON
func (m *Map[K, V]) MarshalJSON() ([]byte, error) {
	e := entry[K, V]{
		K: make([]K, 0, m.Count()),
		V: make([]V, 0, m.Count()),
	}
	m.Iter(func(k K, v V) bool {
		e.K = append(e.K, k)
		e.V = append(e.V, v)
		return false
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
		m.Put(k, e.V[i])
	}
	return nil
}

// SyncMap
type SyncMap[K comparable, V any] struct {
	m Map[K, V]
	sync.RWMutex
}

// NewSyncMap
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{NewMap[K, V](), sync.RWMutex{}}
}

// Get
func (m *SyncMap[K, V]) Get(key K) (v V, ok bool) {
	m.RLock()
	v, ok = m.m.Get(key)
	m.RUnlock()
	return
}

// Set
func (m *SyncMap[K, V]) Set(key K, value V) {
	m.Lock()
	m.m.Put(key, value)
	m.Unlock()
}

// Delete
func (m *SyncMap[K, V]) Delete(key K) {
	m.Lock()
	m.m.Delete(key)
	m.Unlock()
}

// Keys
func (m *SyncMap[K, V]) Keys() (keys []K) {
	m.RLock()
	keys = make([]K, 0, m.m.Count())
	m.m.Iter(func(k K, _ V) bool {
		keys = append(keys, k)
		return false
	})
	m.RUnlock()
	return
}

// Len
func (m *SyncMap[K, V]) Len() (n int) {
	m.RLock()
	n = m.m.Count()
	m.RUnlock()
	return
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
