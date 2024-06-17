package structx

import (
	"github.com/cockroachdb/swiss"
)

type Map struct {
	m *swiss.Map[string, []byte]
}

func NewMap() *Map {
	return &Map{m: swiss.New[string, []byte](8)}
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

func (m *Map) Scan(fn func(key string, value []byte)) {
	m.m.All(func(key string, val []byte) (next bool) {
		fn(key, val)
		return true
	})
}
