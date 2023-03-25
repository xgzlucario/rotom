package store

import (
	"reflect"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
	"github.com/zeebo/xxh3"
)

// DB
func DB() *store { return db }

// Set
func (s *store) Set(key string, value any) {
	shard := s.getShard(key)

	src, _ := shard.EncodeValue(value)

	// 1{key}|{value}\n
	shard.Lock()
	shard.buffer.WriteByte(OP_SET)
	shard.buffer.Write(base.S2B(&key))
	shard.buffer.WriteByte(spr)
	shard.buffer.Write(src)
	shard.buffer.Write(lineSpr)
	shard.Unlock()

	shard.Set(key, value)
}

// SetWithTTL
func (s *store) SetWithTTL(key string, value any, ttl time.Duration) {
	shard := s.getShard(key)
	ts := GlobalTime() + int64(ttl)

	src, _ := shard.EncodeValue(value)
	ttlStr, _ := shard.EncodeValue(ts)

	// 2{key}|{ttl}|{value}\n
	shard.Lock()
	shard.buffer.WriteByte(OP_SET_WITH_TTL)
	shard.buffer.Write(base.S2B(&key))
	shard.buffer.WriteByte(spr)
	shard.buffer.Write(ttlStr)
	shard.buffer.WriteByte(spr)
	shard.buffer.Write(src)
	shard.buffer.Write(lineSpr)
	shard.Unlock()

	shard.SetWithTTL(key, value, ttl)
}

// Remove
func (s *store) Remove(key string) (any, bool) {
	shard := s.getShard(key)

	// 3{key}\n
	shard.Lock()
	shard.buffer.WriteByte(OP_REMOVE)
	shard.buffer.Write(base.S2B(&key))
	shard.buffer.Write(lineSpr)
	shard.Unlock()

	return shard.Remove(key)
}

// Persist removes the expiration from a key
func (s *store) Persist(key string) bool {
	shard := s.getShard(key)

	// 4{key}\n
	shard.Lock()
	shard.buffer.WriteByte(OP_PERSIST)
	shard.buffer.Write(base.S2B(&key))
	shard.buffer.Write(lineSpr)
	shard.Unlock()

	return shard.Persist(key)
}

// Type returns the type of the value stored at key
func (s *store) Type(key string) reflect.Type {
	shard := s.getShard(key)
	v, ok := shard.Get(key)
	if ok {
		return reflect.TypeOf(v)
	}
	return nil
}

// Commit commits all changes and persist to disk immediately
func (s *store) Commit() error {
	for _, shard := range s.shards {
		if _, err := shard.WriteBuffer(); err != nil {
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

func (s *store) getShard(key string) *storeShard {
	return s.shards[xxh3.HashString(key)&(ShardCount-1)]
}

func (s *store) DEBUGgetShard(key string) *storeShard {
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

func getValue[T any](key string, vptr T) (T, error) {
	shard := db.getShard(key)
	// get
	val, ok := shard.Get(key)
	if !ok {
		return vptr, base.ErrKeyNotFound(key)
	}

	// type assertion
	obj, ok := val.(T)
	if ok {
		return obj, nil
	}

	// unmarshal
	buf := val.([]byte)
	if err := shard.DecodeValue(buf, vptr); err != nil {
		return vptr, err
	}
	shard.Set(key, vptr)

	return vptr, nil
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

// GetTime
func (s *store) GetTime(k string) (v time.Time, err error) { getValue(k, &v); return }

// GetStringSlice
func (s *store) GetStringSlice(k string) (v []string, err error) { getValue(k, &v); return }

// GetList
func GetList[T comparable](key string) (*structx.List[T], error) {
	return getValue(key, structx.NewList[T]())
}

// GetSet
func GetSet[T comparable](s *store, key string) (structx.Set[T], error) {
	return getValue(key, structx.NewMapSet[T]())
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

// GetCustomType
func GetCustomType[T base.Marshaler](key string, data T) (T, error) {
	return getValue(key, data)
}
