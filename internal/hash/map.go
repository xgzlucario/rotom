package hash

import (
	"github.com/cockroachdb/swiss"
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
	m *swiss.Map[string, []byte]
}

func NewMap() *Map {
	return &Map{m: swiss.New[string, []byte](256)}
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

func (m *Map) Scan(fn func(key string, val []byte)) {
	m.m.All(func(key string, val []byte) (next bool) {
		fn(key, val)
		return true
	})
}
