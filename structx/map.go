package structx

import (
	"github.com/xgzlucario/rotom/base"
)

type Map[K comparable, V any] map[K]V

func (m Map[K, V]) MarshalJSON() ([]byte, error) {
	return base.MarshalJSON(m)
}

func (m Map[K, V]) UnmarshalJSON(src []byte) error {
	return base.UnmarshalJSON(src, m)
}
