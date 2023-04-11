package store

import (
	"reflect"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

// DB
func DB() *store { return db }

// Set
func (s *store) Set(key string, value any) {
	sd := s.getShard(key)

	// 1{key}|{value}\n
	sd.Lock()
	sd.encodeBytes(OP_SET)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encodeBytes(lineSpr...)
	sd.Unlock()

	sd.Set(key, value)
}

// SetWithTTL
func (s *store) SetWithTTL(key string, value any, ttl time.Duration) {
	sd := s.getShard(key)
	ts := GlobalTime() + int64(ttl)

	// 2{key}|{ttl}|{value}\n
	sd.Lock()
	sd.encodeBytes(OP_SET_WITH_TTL)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(sprChar)
	sd.encodeInt64(ts)
	sd.encodeBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encodeBytes(lineSpr...)
	sd.Unlock()

	sd.SetWithTTL(key, value, ttl)
}

// Remove
func (s *store) Remove(key string) (any, bool) {
	sd := s.getShard(key)

	// 3{key}\n
	sd.Lock()
	sd.encodeBytes(OP_REMOVE)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(lineSpr...)
	sd.Unlock()

	return sd.Remove(key)
}

// Persist removes the expiration from a key
func (s *store) Persist(key string) bool {
	sd := s.getShard(key)

	// 4{key}\n
	sd.Lock()
	sd.encodeBytes(OP_PERSIST)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(lineSpr...)
	sd.Unlock()

	return sd.Persist(key)
}

// HGet
func (s *store) HGet(key string, fields ...string) (any, bool) {
	sd := s.getShard(key)

	sd.RLock()
	defer sd.RUnlock()

	m, ok := sd.Cache.Get(key)
	if ok {
		return m.(structx.MMap).Get(fields...)
	}
	return nil, false
}

// HSet
func (s *store) HSet(value any, key string, fields ...string) {
	sd := s.getShard(key)

	sd.Lock()
	defer sd.Unlock()

	// 5{key}|{fields}|{value}\n
	sd.encodeBytes(OP_HSET)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(sprChar)
	sd.Encode(fields)
	sd.encodeBytes(sprChar)
	if err := sd.Encode(value); err != nil {
		panic(err)
	}
	sd.encodeBytes(lineSpr...)

	m, ok := sd.Cache.Get(key)
	if ok {
		m.(structx.MMap).Set(value, fields...)
	} else {
		m := structx.MMap{}
		m.Set(value, fields...)
		sd.Cache.Set(key, m)
	}
}

// HRemove
func (s *store) HRemove(key string, fields ...string) (any, bool) {
	sd := s.getShard(key)

	sd.Lock()
	defer sd.Unlock()

	// 6{key}|{fields}\n
	sd.encodeBytes(OP_HREMOVE)
	sd.encodeBytes(base.S2B(&key)...)
	sd.encodeBytes(sprChar)
	sd.Encode(fields)
	sd.encodeBytes(lineSpr...)

	m, ok := sd.Cache.Get(key)
	if ok {
		return m.(structx.MMap).Remove(fields...)
	}
	return nil, false
}

// Type returns the type of the value stored at key
func (s *store) Type(key string) reflect.Type {
	sd := s.getShard(key)
	v, ok := sd.Get(key)
	if ok {
		return reflect.TypeOf(v)
	}
	return nil
}

// Flush writes all the buf data to disk
func (s *store) Flush() error {
	for _, sd := range s.shards {
		if _, err := sd.WriteBuffer(); err != nil {
			return err
		}
	}
	return nil
}

// Count
func (s *store) Count() int {
	var sum int
	for _, s := range s.shards {
		sum += s.Count()
	}
	return sum
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

// Incr
func (s *store) Incr(key string, incr float64) (val float64, err error) {
	val, err = s.GetFloat64(key)
	if err != nil {
		return -1, err
	}
	val += incr
	s.Set(key, val)
	return
}

// GetString
func (s *store) GetString(k string) (v string, err error) { getValue(k, &v); return }

// GetInt
func (s *store) GetInt(k string) (v int, err error) { getValue(k, &v); return }

// GetInt32
func (s *store) GetInt32(k string) (v int32, err error) { getValue(k, &v); return }

// GetInt64
func (s *store) GetInt64(k string) (v int64, err error) { getValue(k, &v); return }

// GetUint
func (s *store) GetUint(k string) (v uint, err error) { getValue(k, &v); return }

// GetUint32
func (s *store) GetUint32(k string) (v uint32, err error) { getValue(k, &v); return }

// GetUint64
func (s *store) GetUint64(k string) (v uint64, err error) { getValue(k, &v); return }

// GetFloat32
func (s *store) GetFloat32(k string) (v float32, err error) { getValue(k, &v); return }

// GetFloat64
func (s *store) GetFloat64(k string) (v float64, err error) { getValue(k, &v); return }

// GetBool
func (s *store) GetBool(k string) (v bool, err error) { getValue(k, &v); return }

// GetIntSlice
func (s *store) GetIntSlice(k string) (v []int, err error) { getValue(k, &v); return }

// GetStringSlice
func (s *store) GetStringSlice(k string) (v []string, err error) { getValue(k, &v); return }

// GetTime
func (s *store) GetTime(k string) (v time.Time, err error) { getValue(k, &v); return }

// GetList
func GetList[T comparable](key string) (*structx.List[T], error) {
	return getValue(key, structx.NewList[T]())
}

// GetSet
func GetSet[T comparable](s *store, key string) (structx.Set[T], error) {
	return getValue(key, structx.NewSet[T]())
}

// GetMap
func GetMap[K comparable, V any](key string) (structx.Map[K, V], error) {
	return getValue(key, structx.NewMap[K, V]())
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
func GetZset[K, S base.Ordered, V any](key string) (*structx.ZSet[K, S, V], error) {
	return getValue(key, structx.NewZSet[K, S, V]())
}

// GetBitMap
func (s *store) GetBitMap(key string) (*structx.BitMap, error) {
	return getValue(key, structx.NewBitMap())
}

// GetBloom
func (s *store) GetBloom(key string) (*structx.Bloom, error) {
	return getValue(key, structx.NewBloom())
}

// GetMMap
func (s *store) GetMMap(key string) (structx.MMap, error) {
	return getValue(key, structx.MMap{})
}

// Get
func Get[T any](key string, data T) (T, error) {
	return getValue(key, data)
}
