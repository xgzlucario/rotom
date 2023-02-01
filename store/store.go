package store

import (
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// DB
func DB(i int) *Store {
	return dbs[i]
}

// Set
func (s *Store) Set(key string, value base.Marshaler) {
	s.m.Set(key, value)
	s.marshal()
}

// Exist
func (s *Store) Exist(key string) bool {
	return s.m.Has(key)
}

// Remove
func (s *Store) Remove(key string) bool {
	if s.Exist(key) {
		s.m.Remove(key)
		return true
	}
	return false
}

// Save
func (s *Store) Save() {
	s.marshalForce()
}

// getGenericValue return generic data from store
func getGenericValue[T base.Marshaler](s *Store, key string, data T) (T, error) {
	val, ok := s.m.Get(key)
	if !ok {
		return data, base.ErrKeyNotFound(key)
	}

	// type assertion
	obj, ok := val.(T)
	if ok {
		return obj, nil
	}

	// marshal from JSON
	src, _ := base.MarshalJSON(val)
	if err := data.UnmarshalJSON(src); err != nil {
		return data, base.ErrType(data)
	}

	return data, nil
}

// getValue return base data from store
func getValue[T base.Bases](s *Store, key string, data T) (T, error) {
	val, ok := s.m.Get(key)
	if !ok {
		return data, base.ErrKeyNotFound(key)
	}

	// type assertion
	obj, ok := val.(T)
	if ok {
		return obj, nil
	}
	return data, base.ErrType(obj)
}

// GetString
func GetString(s *Store, key string) (val string, err error) {
	return getValue(s, key, val)
}

// GetInt
func GetInt(s *Store, key string) (val int, eerr error) {
	return getValue(s, key, val)
}

// GetFloat64
func GetFloat64(s *Store, key string) (val float64, err error) {
	return getValue(s, key, val)
}

// GetBool
func GetBool(s *Store, key string) (val bool, err error) {
	return getValue(s, key, val)
}

// GetList
// func GetList[T comparable](s *Store, key string) (*structx.List[T], error) {
// 	return getStoreValue[*structx.List[T]](s, key)
// }

// GetLSet
// func GetLSet[T comparable](s *Store, key string) (*structx.LSet[T], error) {
// 	return getStoreValue[*structx.LSet[T]](s, key)
// }

// GetMap
// func GetMap[K comparable, V any](s *Store, key string) (*structx.Map[K, V], error) {
// 	return getStoreValue[*structx.Map[K, V]](s, key)
// }

// GetSyncMap
// func GetSyncMap[K comparable, V any](s *Store, key string) (*structx.SyncMap[K, V], error) {
// 	return getStoreValue[*structx.SyncMap[K, V]](s, key)
// }

// GetTrie
func GetTrie[T any](s *Store, key string) (*structx.Trie[T], error) {
	t := structx.NewTrie[T]()
	return getGenericValue(s, key, t)
}

// GetBitMap
// func (s *Store) GetBitMap(key string) (*structx.BitMap, error) {
// 	return getStoreValue[*structx.BitMap](s, key)
// }

// GetSignIn
// func (s *Store) GetSignIn(key string) (*app.SignIn, error) {
// 	return getStoreValue[*app.SignIn](s, key)
// }
