package structx

import (
	"github.com/xgzlucario/rotom/base"
)

type HMap struct {
	Map[string, any]
}

// NewHMap
func NewHMap() HMap {
	return HMap{NewMap[string, any]()}
}

// Get
func (m HMap) HGet(keys ...string) (val any, ok bool) {
	if len(keys) == 0 {
		return
	}

	val, ok = m.Get(keys[0])
	if len(keys) == 1 {
		return
	}

	mm, ok := val.(HMap)
	if !ok {
		return nil, false
	}
	return mm.HGet(keys[1:]...)
}

// Set
func (m HMap) HSet(value any, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		m.Set(keys[0], value)
		return
	}

	val, _ := m.Get(keys[0])
	mm, ok := val.(HMap)
	if !ok {
		mm = NewHMap()
		m.Set(keys[0], mm)
	}

	mm.HSet(value, keys[1:]...)
}

// Remove
func (m HMap) HRemove(keys ...string) (val any, ok bool) {
	if len(keys) == 0 {
		return
	}
	if len(keys) == 1 {
		return m.Delete(keys[0])
	}

	val, _ = m.Get(keys[0])
	mm, ok := val.(HMap)
	if !ok {
		return
	}
	return mm.HRemove(keys[1:]...)
}

// Print
func (m HMap) Print() {
	base.PrettyPrint(m)
}
