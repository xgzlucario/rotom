package structx

// HMap
type HMap[V any] map[string]map[string]V

// NewHMap
func NewHMap[V any]() HMap[V] {
	return map[string]map[string]V{}
}

// HSet
func (m HMap[V]) HSet(key, field string, value V) {
	if s, ok := m[key]; ok {
		s[field] = value
	} else {
		m[key] = map[string]V{field: value}
	}
}

// HGet
func (m HMap[V]) HGet(key, field string) (V, bool) {
	if s, ok := m[key]; ok {
		v, ok := s[field]
		return v, ok

	} else {
		var v V
		return v, false
	}
}

// Len
func (m HMap[V]) Len() int {
	var length int
	for _, v := range m {
		length += len(v)
	}
	return length
}
