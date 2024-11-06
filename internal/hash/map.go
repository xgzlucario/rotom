package hash

import (
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
)

type MapI interface {
	iface.Encoder
	Set(key string, val []byte) bool
	Get(key string) ([]byte, bool)
	Remove(key string) bool
	Len() int
	Scan(fn func(key string, val []byte))
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

func (m *Map) Encode(writer *resp.Writer) error {
	writer.WriteArrayHead(len(m.data))
	for k, v := range m.data {
		writer.WriteBulkString(k)
		writer.WriteBulk(v)
	}
	return nil
}

func (m *Map) Decode(reader *resp.Reader) error {
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	m.data = make(map[string][]byte, n*2)
	for range n {
		key, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		val, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		m.data[string(key)] = val
	}
	return nil
}
