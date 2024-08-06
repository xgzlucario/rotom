package hash

import (
	"github.com/dolthub/swiss"
)

type MapI interface {
	Set(key string, val []byte) bool
	Get(key string) ([]byte, bool)
	Remove(key string) bool
	Len() int
	Scan(fn func(key string, val []byte))
}

var _ MapI = (*Map)(nil)

type Map struct {
	data *swiss.Map[string, []byte]
}

func NewMap() *Map {
	return &Map{swiss.NewMap[string, []byte](256)}
}

func (m *Map) Get(key string) ([]byte, bool) {
	return m.data.Get(key)
}

func (m *Map) Set(key string, val []byte) bool {
	_, ok := m.data.Get(key)
	m.data.Put(key, val)
	return !ok
}

func (m *Map) Remove(key string) bool {
	return m.data.Delete(key)
}

func (m *Map) Len() int {
	return m.data.Count()
}

func (m *Map) Scan(fn func(key string, val []byte)) {
	m.data.Iter(func(key string, val []byte) (stop bool) {
		fn(key, val)
		return false
	})
}
