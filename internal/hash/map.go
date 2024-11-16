package hash

import (
	"bytes"
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
)

var _ iface.MapI = (*Map)(nil)

type Map struct {
	data *swiss.Map[string, []byte]
}

func NewMap() *Map {
	return &Map{
		data: swiss.New[string, []byte](256),
	}
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
	_, ok := m.data.Get(key)
	m.data.Delete(key)
	return ok
}

func (m *Map) Len() int {
	return m.data.Len()
}

func (m *Map) Scan(fn func(string, []byte)) {
	m.data.All(func(key string, val []byte) bool {
		fn(key, val)
		return true
	})
}

func (m *Map) Encode(writer *resp.Writer) error {
	writer.WriteArrayHead(m.Len())
	m.Scan(func(k string, v []byte) {
		writer.WriteBulkString(k)
		writer.WriteBulk(v)
	})
	return nil
}

func (m *Map) Decode(reader *resp.Reader) error {
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	m.data = swiss.New[string, []byte](n * 2)
	for range n {
		key, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		val, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		m.data.Put(string(key), bytes.Clone(val))
	}
	return nil
}
