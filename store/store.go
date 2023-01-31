package store

import (
	"github.com/xgzlucario/rotom/app"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// DB
func DB(i int) *Store {
	return dbs[i]
}

// Get
func (s *Store) Get(key string) (any, bool) {
	val, ok := s.m.Get(key)
	return val, ok
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

func getStoreValue[T any](s *Store, key string) (t T, err error) {
	val, ok := s.Get(key)
	if !ok {
		return t, base.ErrKeyNotFound(key)
	}

	obj, ok := val.(T)
	if !ok {
		return t, base.ErrType(t)
	}
	return obj, nil
}

// GetString
func GetString(s *Store, key string) (string, error) {
	return getStoreValue[string](s, key)
}

// GetInt
func GetInt(s *Store, key string) (int, error) {
	return getStoreValue[int](s, key)
}

// GetFloat64
func GetFloat64(s *Store, key string) (float64, error) {
	return getStoreValue[float64](s, key)
}

// GetBool
func GetBool(s *Store, key string) (bool, error) {
	return getStoreValue[bool](s, key)
}

// GetList
func GetList[T comparable](s *Store, key string) (*structx.List[T], error) {
	return getStoreValue[*structx.List[T]](s, key)
}

// GetLSet
func GetLSet[T comparable](s *Store, key string) (*structx.LSet[T], error) {
	return getStoreValue[*structx.LSet[T]](s, key)
}

// GetMap
func GetMap[K comparable, V any](s *Store, key string) (*structx.Map[K, V], error) {
	return getStoreValue[*structx.Map[K, V]](s, key)
}

// GetSyncMap
func GetSyncMap[K comparable, V any](s *Store, key string) (*structx.SyncMap[K, V], error) {
	return getStoreValue[*structx.SyncMap[K, V]](s, key)
}

// GetTrie
func GetTrie[T any](s *Store, key string) (*structx.Trie[T], error) {
	return getStoreValue[*structx.Trie[T]](s, key)
}

// GetBitMap
func (s *Store) GetBitMap(key string) (*structx.BitMap, error) {
	return getStoreValue[*structx.BitMap](s, key)
}

// GetSignIn
func (s *Store) GetSignIn(key string) (*app.SignIn, error) {
	return getStoreValue[*app.SignIn](s, key)
}
