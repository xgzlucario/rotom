package structx

import (
	"github.com/bytedance/sonic"
	"golang.org/x/exp/maps"
)

type Map[K comparable, V any] map[K]V

func (m Map[K, V]) Keys() []K {
	return maps.Keys(m)
}

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return sonic.Unmarshal(src, m)
}
