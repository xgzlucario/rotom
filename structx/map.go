package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/swiss"
)

// Map
type Map[K comparable, V any] struct {
	*swiss.Map[K, V]
}

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{swiss.New[K, V](32)}
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
	m.All(func(k K, v V) bool {
		e.K = append(e.K, k)
		e.V = append(e.V, v)
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
		m.Put(k, e.V[i])
	}
	return nil
}

// SyncMap
type SyncMap struct {
	m Map[string, []byte]
	sync.RWMutex
}

// NewSyncMap
func NewSyncMap() *SyncMap {
	return &SyncMap{m: NewMap[string, []byte]()}
}

// Get
func (m *SyncMap) Get(key string) (v []byte, ok bool) {
	m.RLock()
	v, ok = m.m.Get(key)
	m.RUnlock()
	return
}

// Set
func (m *SyncMap) Set(key string, value []byte) {
	m.Lock()
	m.m.Put(key, value)
	m.Unlock()
}

// Delete
func (m *SyncMap) Delete(key string) bool {
	m.Lock()
	m.m.Delete(key)
	m.Unlock()
	return true
}

// Keys
func (m *SyncMap) Keys() (keys []string) {
	m.RLock()
	keys = make([]string, 0, m.m.Len())
	m.m.All(func(k string, _ []byte) bool {
		keys = append(keys, k)
		return true
	})
	m.RUnlock()
	return
}

// Len
func (m *SyncMap) Len() (n int) {
	m.RLock()
	n = m.m.Len()
	m.RUnlock()
	return
}

// MarshalJSON
func (m *SyncMap) MarshalJSON() ([]byte, error) {
	m.RLock()
	defer m.RUnlock()
	return m.m.MarshalJSON()
}

// UnmarshalJSON
func (m *SyncMap) UnmarshalJSON(src []byte) error {
	m.Lock()
	defer m.Unlock()
	return m.m.UnmarshalJSON(src)
}
