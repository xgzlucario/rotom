package structx

import (
	"unsafe"

	"github.com/bytedance/sonic"
	cache "github.com/xgzlucario/GigaCache"
)

// Map
type Map struct {
	m *cache.GigaCache
}

func defaultOptions() cache.Options {
	options := cache.DefaultOptions
	options.ShardCount = 4
	options.IndexSize = 8
	options.BufferSize = 32
	return options
}

// NewMap
func NewMap() (s *Map) {
	return &Map{m: cache.New(defaultOptions())}
}

// Get
func (m *Map) Get(key string) ([]byte, int64, bool) {
	return m.m.Get(key)
}

// Set
func (m *Map) Set(key string, val []byte, ts int64) {
	m.m.SetTx(key, val, ts)
}

// Remove
func (m *Map) Remove(key string) bool {
	return m.m.Remove(key)
}

// Keys
func (m *Map) Keys() (keys []string) {
	keys = make([]string, 0, m.m.Stat().Len)
	m.m.Scan(func(key, val []byte, _ int64) (next bool) {
		keys = append(keys, string(key))
		return true
	})
	return
}

// Len
func (m *Map) Len() (n int) {
	return m.m.Stat().Len
}

type mentry struct {
	K []string
	V [][]byte
	T []int64
}

// MarshalJSON
func (m *Map) MarshalJSON() ([]byte, error) {
	n := m.m.Stat().Len
	entry := mentry{
		K: make([]string, 0, n),
		V: make([][]byte, 0, n),
		T: make([]int64, 0, n),
	}
	m.m.Scan(func(key, val []byte, ts int64) (next bool) {
		entry.K = append(entry.K, b2s(key))
		entry.V = append(entry.V, val)
		entry.T = append(entry.T, ts)
		return true
	})
	return sonic.Marshal(entry)
}

// UnmarshalJSON
func (m *Map) UnmarshalJSON(src []byte) error {
	m.m = cache.New(defaultOptions())

	var entry mentry
	if err := sonic.Unmarshal(src, &entry); err != nil {
		return err
	}

	for i, k := range entry.K {
		m.m.SetTx(k, entry.V[i], entry.T[i])
	}
	return nil
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
