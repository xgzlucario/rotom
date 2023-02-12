package store

import (
	"time"

	"github.com/xgzlucario/rotom/app"
	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// DB
func DB(i int) *Store {
	return dbs[i]
}

// Set
func (s *Store) Set(key string, value any) {
	s.m.Set(key, value)
}

// SetWithTTL
func (s *Store) SetWithTTL(key string, value any, ttl time.Duration) {
	s.m.SetWithTTL(key, value, ttl)
}

// Remove
func (s *Store) Remove(key string) bool {
	return s.m.Remove(key)
}

// Count
func (s *Store) Count() int {
	return s.m.Count()
}

// Keys
func (s *Store) Keys() []string {
	return s.m.Keys()
}

// Save
func (s *Store) Save() {
	s.marshal()
}

// Flush
func (s *Store) Flush() {
	s.m.Clear()
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

	// marshal
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

// Incr
func (s *Store) Incr(key string, increment float64) (val float64, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil && err.Error() != base.ErrKeyNotFound(key).Error() {
		return -1, err
	}

	val = tmp + increment
	s.Set(key, val)
	return val, nil
}

// GetString
func (s *Store) GetString(key string) (val string, err error) {
	return getValue(s, key, val)
}

// GetInt
func (s *Store) GetInt(key string) (val int, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return int(tmp), nil
}

// GetInt32
func (s *Store) GetInt32(key string) (val int32, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return int32(tmp), nil
}

// GetInt64
func (s *Store) GetInt64(key string) (val int64, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return int64(tmp), nil
}

// GetUint
func (s *Store) GetUint(key string) (val uint, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return uint(tmp), nil
}

// GetUint32
func (s *Store) GetUint32(key string) (val uint32, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return uint32(tmp), nil
}

// GetUint64
func (s *Store) GetUint64(key string) (val uint64, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return uint64(tmp), nil
}

// GetFloat32
func (s *Store) GetFloat32(key string) (val float32, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return float32(tmp), nil
}

// GetFloat64
func (s *Store) GetFloat64(key string) (val float64, err error) {
	return getValue(s, key, val)
}

// GetBool
func (s *Store) GetBool(key string) (val bool, err error) {
	return getValue(s, key, val)
}

// GetTime
func (s *Store) GetTime(key string) (val time.Time, err error) {
	var str string
	str, err = getValue(s, key, str)
	if err != nil {
		return val, err
	}
	return time.Parse(time.RFC3339, str)
}

// GetDuration
func (s *Store) GetDuration(key string) (val time.Duration, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return 0, err
	}
	return time.Duration(tmp), nil
}

// GetList
func GetList[T comparable](s *Store, key string) (*structx.List[T], error) {
	return getGenericValue(s, key, structx.NewList[T]())
}

// GetLSet
func GetLSet[T comparable](s *Store, key string) (*structx.LSet[T], error) {
	return getGenericValue(s, key, structx.NewLSet[T]())
}

// GetMap
func GetMap[K comparable, V any](s *Store, key string) (structx.Map[K, V], error) {
	return getGenericValue(s, key, structx.NewMap[K, V]())
}

// GetSyncMap
func GetSyncMap[V any](s *Store, key string) (*structx.SyncMap[string, V], error) {
	return getGenericValue(s, key, structx.NewSyncMap[V]())
}

// GetTrie
func GetTrie[T any](s *Store, key string) (*structx.Trie[T], error) {
	return getGenericValue(s, key, structx.NewTrie[T]())
}

// GetBtree
func GetBtree[K base.Ordered, V any](s *Store, key string) (*structx.Btree[K, V], error) {
	return getGenericValue(s, key, structx.NewBtree[K, V]())
}

// GetZset
func GetZset[K, V base.Ordered](s *Store, key string) (*structx.ZSet[K, V], error) {
	return getGenericValue(s, key, structx.NewZSet[K, V]())
}

// GetBitMap
func (s *Store) GetBitMap(key string) (*structx.BitMap, error) {
	return getGenericValue(s, key, structx.NewBitMap())
}

// GetSignIn
func (s *Store) GetSignIn(key string) (*app.SignIn, error) {
	return getGenericValue(s, key, app.NewSignIn())
}

// GetCustomStruct
func GetCustomStruct[T base.Marshaler](s *Store, key string, data T) (T, error) {
	return getGenericValue(s, key, data)
}
