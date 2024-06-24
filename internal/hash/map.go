package hash

import (
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/pkg"
)

var (
	mapAllocator = pkg.NewAllocator[string, []byte]()
)

type Map struct {
	m *swiss.Map[string, []byte]
}

func NewMap() *Map {
	return &Map{m: swiss.New(8, swiss.WithAllocator(mapAllocator))}
}

func (m *Map) Get(key string) ([]byte, bool) {
	return m.m.Get(key)
}

func (m *Map) Set(key string, val []byte) bool {
	_, ok := m.m.Get(key)
	m.m.Put(key, val)
	return !ok
}

func (m *Map) Remove(key string) bool {
	_, ok := m.m.Get(key)
	m.m.Delete(key)
	return ok
}

func (m *Map) Len() int {
	return m.m.Len()
}

func (m *Map) Scan(fn func(key string, value []byte)) {
	m.m.All(func(key string, val []byte) (next bool) {
		fn(key, val)
		return true
	})
}

func (m *Map) Free() {
	m.m.Close()
}
