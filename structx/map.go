package structx

import (
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/swiss"
	cache "github.com/xgzlucario/GigaCache"
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

func syncMapOptions() cache.Options {
	options := cache.DefaultOptions
	options.ShardCount = 32
	options.IndexSize = 32
	options.BufferSize = 1024
	options.DisableEvict = true
	return options
}

// SyncMap
type SyncMap struct {
	m *cache.GigaCache
}

// NewSyncMap
func NewSyncMap() (s *SyncMap) {
	return &SyncMap{m: cache.New(syncMapOptions())}
}

// Get
func (m *SyncMap) Get(key string) ([]byte, bool) {
	val, _, ok := m.m.Get(key)
	return val, ok
}

// Set
func (m *SyncMap) Set(key string, val []byte) {
	m.m.Set(key, val)
}

// Remove
func (m *SyncMap) Remove(key string) bool {
	return m.m.Remove(key)
}

// Keys
func (m *SyncMap) Keys() (keys []string) {
	keys = make([]string, 0, m.m.Stat().Len)
	m.m.Scan(func(key, val []byte, _ int64) (next bool) {
		keys = append(keys, string(key))
		return true
	})
	return
}

// Len
func (m *SyncMap) Len() (n int) {
	return m.m.Stat().Len
}

// MarshalJSON
func (m *SyncMap) MarshalJSON() ([]byte, error) {
	n := m.m.Stat().Len
	entry := entry[string, string]{
		K: make([]string, 0, n),
		V: make([]string, 0, n),
	}
	m.m.Scan(func(key, val []byte, _ int64) (next bool) {
		entry.K = append(entry.K, b2s(key))
		entry.V = append(entry.V, b2s(val))
		return true
	})
	return sonic.Marshal(entry)
}

// UnmarshalJSON
func (m *SyncMap) UnmarshalJSON(src []byte) error {
	m.m = cache.New(syncMapOptions())

	var entry entry[string, string]
	if err := sonic.Unmarshal(src, &entry); err != nil {
		return err
	}

	for i, k := range entry.K {
		m.m.Set(k, s2b(&entry.V[i]))
	}
	return nil
}

func s2b(str *string) []byte {
	strHeader := (*[2]uintptr)(unsafe.Pointer(str))
	byteSliceHeader := [3]uintptr{
		strHeader[0], strHeader[1], strHeader[1],
	}
	return *(*[]byte)(unsafe.Pointer(&byteSliceHeader))
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
