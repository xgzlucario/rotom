package structx

import (
	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/base"
)

type MMap struct {
	m Map[string, any]
}

// NewMMap
func NewMMap() MMap {
	return MMap{m: NewMap[string, any]()}
}

// Get
func (m MMap) Get(keys ...string) (val any, ok bool) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		return m.m.Get(keys[0])
	}

	val, _ = m.m.Get(keys[0])
	mm, ok := val.(MMap)
	if !ok {
		return nil, false
	}
	return mm.Get(keys[1:]...)
}

// Set
func (m MMap) Set(value any, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		m.m.Set(keys[0], value)
		return
	}

	val, _ := m.m.Get(keys[0])
	mm, ok := val.(MMap)
	if !ok {
		mm = NewMMap()
		m.m.Set(keys[0], mm)
	}

	mm.Set(value, keys[1:]...)
}

// Remove
func (m MMap) Remove(keys ...string) (val any, ok bool) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		return m.m.Delete(keys[0])
	}

	val, _ = m.m.Get(keys[0])
	mm, ok := val.(MMap)
	if !ok {
		return
	}
	return mm.Remove(keys[1:]...)
}

// Print
func (m MMap) Print() {
	base.PrettyPrint(m)
}

// MarshalJSON
func (m MMap) Marshal() ([]byte, error) {
	return sonic.Marshal(m.m)
}

// UnmarshalJSON
func (m MMap) Unmarshal(src []byte) error {
	return sonic.Unmarshal(src, &m.m)
}
