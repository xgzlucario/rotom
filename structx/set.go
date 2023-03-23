package structx

import (
	"github.com/bytedance/sonic"
	"github.com/zyedidia/generic"
	"github.com/zyedidia/generic/set"
)

type Set[K comparable] struct {
	set.Set[K]
}

// NewMapSet
func NewMapSet[K comparable](keys ...K) Set[K] {
	return Set[K]{set.NewMapset(keys...)}
}

// NewHashSet
func NewHashSet[K comparable](cap uint64, equals generic.EqualsFn[K], hash generic.HashFn[K], in ...K) Set[K] {
	return Set[K]{set.NewHashset(cap, equals, hash, in...)}
}

// MarshalJSON
func (s Set[K]) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(s.Set.Keys())
}

// UnmarshalJSON
func (s Set[K]) UnmarshalJSON(src []byte) error {
	var keys []K
	if err := sonic.Unmarshal(src, &keys); err != nil {
		return err
	}
	for _, k := range keys {
		s.Put(k)
	}

	return nil
}
