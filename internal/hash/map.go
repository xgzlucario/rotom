package hash

type MapI interface {
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
