package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/dolthub/swiss"
	bproto "github.com/xgzlucario/rotom/proto"
	"google.golang.org/protobuf/proto"
)

// Map
type Map[K comparable, V any] struct {
	*swiss.Map[K, V]
}

// NewMap
func NewMap[K comparable, V any](size ...int) Map[K, V] {
	defaultCap := 8
	if len(size) > 0 {
		defaultCap = size[0]
	}
	return Map[K, V]{swiss.NewMap[K, V](uint32(defaultCap))}
}

type entry[K comparable, V any] struct {
	K []K
	V []V
}

// Clone
func (m *Map[K, V]) Clone() Map[K, V] {
	m2 := NewMap[K, V](m.Count())
	m.Iter(func(k K, v V) bool {
		m2.Put(k, v)
		return false
	})
	return m2
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
type SyncMap struct {
	m Map[string, []byte]
	sync.RWMutex
}

// NewSyncMap
func NewSyncMap(size ...int) *SyncMap {
	return &SyncMap{m: NewMap[string, []byte](size...)}
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
	ok := m.m.Delete(key)
	m.Unlock()
	return ok
}

// Keys
func (m *SyncMap) Keys() (keys []string) {
	m.RLock()
	keys = make([]string, 0, m.m.Count())
	m.m.Iter(func(k string, _ []byte) bool {
		keys = append(keys, k)
		return false
	})
	m.RUnlock()
	return
}

// Len
func (m *SyncMap) Len() (n int) {
	m.RLock()
	n = m.m.Count()
	m.RUnlock()
	return
}

// Clone
func (m *SyncMap) Clone() *SyncMap {
	m.RLock()
	m2 := &SyncMap{m: m.m.Clone()}
	m.RUnlock()
	return m2
}

// MarshalJSON
func (m *SyncMap) MarshalJSON() ([]byte, error) {
	m.RLock()
	defer m.RUnlock()

	e := &bproto.SyncMapEntries{
		K: make([]string, 0, m.m.Count()),
		V: make([][]byte, 0, m.m.Count()),
	}
	m.m.Iter(func(k string, v []byte) bool {
		e.K = append(e.K, k)
		e.V = append(e.V, v)
		return false
	})

	return proto.Marshal(e)
}

// UnmarshalJSON
func (m *SyncMap) UnmarshalJSON(src []byte) error {
	m.Lock()
	defer m.Unlock()

	var entries bproto.SyncMapEntries
	if err := proto.Unmarshal(src, &entries); err != nil {
		return err
	}

	for i, k := range entries.K {
		m.m.Put(k, entries.V[i])
	}

	return nil
}
