package hash

import (
	"github.com/bytedance/sonic"
	"io"
)

type MapI interface {
	Set(key string, val []byte) bool
	Get(key string) ([]byte, bool)
	Remove(key string) bool
	Len() int
	Scan(fn func(key string, val []byte))
	Encode(writer io.Writer) error
	Decode([]byte) error
}

var _ MapI = (*Map)(nil)

type Map struct {
	data map[string][]byte
}

func NewMap() *Map {
	return &Map{make(map[string][]byte, 256)}
}

func (m *Map) Get(key string) ([]byte, bool) {
	val, ok := m.data[key]
	return val, ok
}

func (m *Map) Set(key string, val []byte) bool {
	_, ok := m.data[key]
	m.data[key] = val
	return !ok
}

func (m *Map) Remove(key string) bool {
	_, ok := m.data[key]
	delete(m.data, key)
	return ok
}

func (m *Map) Len() int {
	return len(m.data)
}

func (m *Map) Scan(fn func(key string, val []byte)) {
	for key, val := range m.data {
		fn(key, val)
	}
}

func (m *Map) Encode(writer io.Writer) error {
	return sonic.ConfigDefault.NewEncoder(writer).Encode(m.data)
}

func (m *Map) Decode(src []byte) error {
	return sonic.Unmarshal(src, &m.data)
}
