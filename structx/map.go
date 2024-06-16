package structx

import (
	"github.com/xgzlucario/rotom/dict"
)

type Map struct {
	m *dict.Dict
}

func defaultOptions() dict.Options {
	options := dict.DefaultOptions
	options.ShardCount = 1
	options.IndexSize = 8
	options.BufferSize = 32
	return options
}

func NewMap() (s *Map) {
	return &Map{m: dict.New(defaultOptions())}
}

func (m *Map) Get(key string) ([]byte, int64, bool) {
	return m.m.Get(key)
}

func (m *Map) Set(key string, val []byte) (newField bool) {
	return m.m.Set(key, val)
}

func (m *Map) Remove(key string) bool {
	return m.m.Remove(key)
}

func (m *Map) Scan(fn func(key string, value []byte)) {
	m.m.Scan(func(key string, val []byte, _ int64) (next bool) {
		fn(key, val)
		return true
	})
}

func (m *Map) Len() (n int) {
	return m.m.GetStats().Len
}
