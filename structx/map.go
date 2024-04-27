package structx

import (
	"unsafe"

	"github.com/bytedance/sonic"
	cache "github.com/xgzlucario/GigaCache"
)

type MapAPI interface {
	Set(string, []byte)
	Get(string) ([]byte, bool)
	Remove(string) bool
	Len() int
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	Keys() []string
	GetType() Type
}

type Type byte

const (
	TypeZipmap Type = iota + 1
	TypeGigaCache
)

type entry[K comparable, V any] struct {
	K []K
	V []V
}

func syncMapOptions() cache.Options {
	options := cache.DefaultOptions
	options.ShardCount = 32
	options.IndexSize = 8
	options.BufferSize = 512
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

func (m *SyncMap) GetType() Type {
	return TypeGigaCache
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
