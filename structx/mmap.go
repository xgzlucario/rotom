package structx

import (
	"github.com/bytedance/sonic"
	"github.com/xgzlucario/rotom/base"
)

type MMap map[string]any

// NewMMap
func NewMMap() MMap {
	return MMap{}
}

// Get
func (m MMap) Get(keys ...string) (val any, ok bool) {
	if len(keys) == 0 {
		return
	}

	val, ok = m[keys[0]]
	if !ok {
		return nil, false
	}
	// last key
	if len(keys) == 1 {
		return
	}

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

	// last key
	if len(keys) == 1 {
		m[keys[0]] = value
		return
	}

	mm, ok := m[keys[0]].(MMap)
	if !ok {
		mm = NewMMap()
		m[keys[0]] = mm
	}
	mm.Set(value, keys[1:]...)
}

// Remove
func (m MMap) Remove(keys ...string) {
	if len(keys) == 0 {
		return
	}

	// last key
	if len(keys) == 1 {
		delete(m, keys[0])
		return
	}

	mm, ok := m[keys[0]].(MMap)
	if !ok {
		return
	}
	mm.Remove(keys[1:]...)
}

// Print
func (m MMap) Print() {
	base.PrettyPrint(m)
}

// MarshalJSON
func (m MMap) Marshal() ([]byte, error) {
	return sonic.Marshal(m)
}

// UnmarshalJSON
func (m MMap) Unmarshal(src []byte) error {
	return sonic.Unmarshal(src, &m)
}
