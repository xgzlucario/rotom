package structx

import (
	"github.com/bytedance/sonic"
	"github.com/tidwall/hashmap"
)

type Map[K comparable, V any] struct {
	*hashmap.Map[K, V]
}

// NewMap
func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{&hashmap.Map[K, V]{}}
}

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return sonic.Unmarshal(src, m)
}
