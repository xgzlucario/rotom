package store

import (
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
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
		src, err := value.(base.Marshaler).MarshalJSON()
		if err != nil {
			panic(err)
		}
		shard.write("1%s|%s\n", key, src)

	} else {
		shard.write("1%s|%v\n", key, value)
	}

	shard.Set(key, value)
}

// SetWithTTL
func (s *store) SetWithTTL(key string, value any, ttl time.Duration) {
	shard := s.getShard(key)

	_, ok := value.(base.Marshaler)
	if ok {
		src, err := value.(base.Marshaler).MarshalJSON()
		if err != nil {
			panic(err)
		}
		shard.write("2%s|%d|%s\n", key, ttl, src)

	} else {
		shard.write("2%s|%d|%v\n", key, ttl, value)
	}

	shard.SetWithTTL(key, value, ttl)
}

// Remove
func (s *store) Remove(key string) bool {
	shard := s.getShard(key)
	shard.write("3%s\n", key)
	return shard.Remove(key)
}

// Persist
func (s *store) Persist(key string) bool {
	shard := s.getShard(key)
	shard.write("4%s\n", key)
	return shard.Persist(key)
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
	return s.shards[xxh3.HashString(key)&(ShardCount-1)]
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

func getValue[T any](key string, data T) (T, error) {
	shard := db.getShard(key)
	// get
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
	buf := val.([]byte)
	if err := base.UnmarshalJSON(buf, &data); err != nil {
		return data, err
	}
	shard.Set(key, data)

	return data, nil
}

// Incr
func (s *store) Incr(key string, increment float64) (val float64, err error) {
	tmp, err := s.GetFloat64(key)
	if err != nil {
		return -1, err
	}

	val = tmp + increment
	s.Set(key, val)
	return val, nil
}

// GetString
func (s *store) GetString(key string) (val string, err error) { return getValue(key, val) }

// GetInt
func (s *store) GetInt(key string) (val int, err error) { return getValue(key, val) }

// GetFloat64
func (s *store) GetFloat64(key string) (val float64, err error) { return getValue(key, val) }

// GetBool
func (s *store) GetBool(key string) (val bool, err error) { return getValue(key, val) }

// GetList
func GetList[T comparable](key string) (*structx.List[T], error) {
	return getValue(key, structx.NewList[T]())
}

// GetLSet
func GetLSet[T comparable](s *store, key string) (*structx.LSet[T], error) {
	return getValue(key, structx.NewLSet[T]())
}

// GetMap
func GetMap[K comparable, V any](key string) (structx.Map[K, V], error) {
	return getValue(key, structx.Map[K, V]{})
}

// GetSyncMap
func GetSyncMap[T any](key string) (*structx.SyncMap[string, T], error) {
	return getValue(key, structx.NewSyncMap[string, T]())
}

// GetTrie
func GetTrie[T any](key string) (*structx.Trie[T], error) {
	return getValue(key, structx.NewTrie[T]())
}

// GetZset
func GetZset[K, V base.Ordered](key string) (*structx.ZSet[K, V], error) {
	return getValue(key, structx.NewZSet[K, V]())
}

// GetBitMap
func (s *store) GetBitMap(key string) (*structx.BitMap, error) {
	return getValue(key, structx.NewBitMap())
}

// GetBloom
func (s *store) GetBloom(key string) (*structx.Bloom, error) {
	return getValue(key, structx.NewBloom())
}

// GetSignIn
func (s *store) GetSignIn(key string) (*structx.SignIn, error) {
	return getValue(key, structx.NewSignIn())
}

// GetCustomType
func GetCustomType[T base.Marshaler](key string, data T) (T, error) {
	return getValue(key, data)
}
