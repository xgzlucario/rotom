package store

import (
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// DB
func DB() *store {
	return db
}

// Set
func (s *store) Set(key string, value any) {
	shard := s.getShard(key)

	_, ok := value.(base.Marshaler)
	if ok {
		src, _ := value.(base.Marshaler).MarshalJSON()
		shard.logger.Printf("%s|%s|%s\n", OP_SET, key, src)

	} else {
		shard.logger.Printf("%s|%s|%v\n", OP_SET, key, value)
	}

	shard.Set(key, value)
}

// SetWithTTL
func (s *store) SetWithTTL(key string, value any, ttl time.Duration) {
	shard := s.getShard(key)

	_, ok := value.(base.Marshaler)
	if ok {
		src, _ := value.(base.Marshaler).MarshalJSON()
		shard.logger.Printf("%s|%s|%s|%d\n", OP_SET_WITH_TTL, key, src, ttl)

	} else {
		shard.logger.Printf("%s|%s|%v|%d\n", OP_SET_WITH_TTL, key, value, ttl)
	}

	shard.SetWithTTL(key, value, ttl)
}

// Remove
func (s *store) Remove(key string) bool {
	shard := s.getShard(key)
	shard.logger.Printf("%s|%s\n", OP_REMOVE, key)
	return shard.Remove(key)
}

// Count
func (s *store) Count() int {
	var sum int
	for _, s := range s.shards {
		sum += s.Count()
	}
	return sum
}

// GetShard
func (s *store) getShard(key string) *storeShard {
	return s.shards[fnv32(key)%DB_SHARD_COUNT]
}

// KeysWithPrefix
func (s *store) KeysWithPrefix(prefix string) []string {
	keys := make([]string, 0)
	for _, shard := range s.shards {
		keys = append(keys, shard.KeysWithPrefix(prefix)...)
	}

	return keys
}

// WithExpired
func (s *store) WithExpired(f func(string, any)) *store {
	for _, s := range s.shards {
		s.WithExpired(f)
	}
	return s
}

// Keys
func (s *store) Keys() []string {
	arr := make([]string, 0, s.Count())
	for _, s := range s.shards {
		arr = append(arr, s.Keys()...)
	}
	return arr
}

// getGenericValue return generic data from store
func getGenericValue[T base.Marshaler](key string, data T) (T, error) {
	shard := db.getShard(key)
	val, ok := shard.Get(key)
	if !ok {
		return data, base.ErrKeyNotFound(key)
	}

	// type assertion
	obj, ok := val.(T)
	if ok {
		return obj, nil
	}

	// unmarshal
	str := val.(string)
	if err := data.UnmarshalJSON([]byte(str)); err != nil {
		return data, base.ErrType(data)
	}

	shard.Set(key, data)

	return data, nil
}

// getValue return base data from store
func getValue[T base.Bases](key string, data T) (T, error) {
	val, ok := db.getShard(key).Get(key)
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
func (s *store) Incr(key string, increment float64) (val float64, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil && err.Error() != base.ErrKeyNotFound(key).Error() {
		return -1, err
	}

	val = tmp + increment
	s.Set(key, val)
	return val, nil
}

// GetString
func (s *store) GetString(key string) (val string, err error) {
	return getValue(key, val)
}

// GetFloat64
func (s *store) GetFloat64(key string) (val float64, err error) {
	return getValue(key, val)
}

// GetBool
func (s *store) GetBool(key string) (val bool, err error) {
	return getValue(key, val)
}

// GetList
func GetList[T comparable](key string) (*structx.List[T], error) {
	return getGenericValue(key, structx.NewList[T]())
}

// GetLSet
func GetLSet[T comparable](s *store, key string) (*structx.LSet[T], error) {
	return getGenericValue(key, structx.NewLSet[T]())
}

// GetMap
func GetMap[K comparable, V any](key string) (structx.Map[K, V], error) {
	return getGenericValue(key, structx.Map[K, V]{})
}

// GetSyncMap
func GetSyncMap[T any](key string) (*structx.SyncMap[string, T], error) {
	return getGenericValue(key, structx.NewSyncMap[string, T]())
}

// GetTrie
func GetTrie[T any](key string) (*structx.Trie[T], error) {
	return getGenericValue(key, structx.NewTrie[T]())
}

// GetZset
func GetZset[K, V base.Ordered](key string) (*structx.ZSet[K, V], error) {
	return getGenericValue(key, structx.NewZSet[K, V]())
}

// GetBitMap
func (s *store) GetBitMap(key string) (*structx.BitMap, error) {
	return getGenericValue(key, structx.NewBitMap())
}

// GetSignIn
func (s *store) GetSignIn(key string) (*structx.SignIn, error) {
	return getGenericValue(key, structx.NewSignIn())
}

// GetCustomStruct
func GetCustomStruct[T base.Marshaler](key string, data T) (T, error) {
	return getGenericValue(key, data)
}
