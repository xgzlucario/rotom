package main

import (
	"github.com/cockroachdb/swiss"
	"github.com/xgzlucario/rotom/internal/timer"
	"time"
)

// Dict is the hashmap for rotom.
type Dict struct {
	data   *swiss.Map[string, any]
	expire *swiss.Map[string, int64]
}

func init() {
	timer.Init()
}

func New() *Dict {
	return &Dict{
		data:   swiss.New[string, any](64),
		expire: swiss.New[string, int64](64),
	}
}

func (dict *Dict) Get(key string) (any, int) {
	data, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return nil, KEY_NOT_EXIST
	}

	ts, ok := dict.expire.Get(key)
	if !ok {
		return data, TTL_FOREVER
	}

	// key expired
	now := timer.GetNanoTime()
	if ts < now {
		dict.delete(key)
		return nil, KEY_NOT_EXIST
	}

	return data, int(ts-now) / int(time.Second)
}

func (dict *Dict) Set(key string, data any) {
	dict.data.Put(key, data)
}

func (dict *Dict) SetWithTTL(key string, data any, ttl int64) {
	if ttl > 0 {
		dict.expire.Put(key, ttl)
	}
	dict.data.Put(key, data)
}

func (dict *Dict) delete(key string) {
	dict.data.Delete(key)
	dict.expire.Delete(key)
}

func (dict *Dict) Delete(key string) bool {
	_, ok := dict.data.Get(key)
	if !ok {
		return false
	}
	dict.delete(key)
	return true
}

// SetTTL set expire time for key.
// return `0` if key not exist or expired.
// return `1` if set success.
func (dict *Dict) SetTTL(key string, ttl int64) int {
	_, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return 0
	}

	// check key if already expired
	ts, ok := dict.expire.Get(key)
	if ok && ts < timer.GetNanoTime() {
		dict.delete(key)
		return 0
	}

	// set ttl
	dict.expire.Put(key, ttl)
	return 1
}

func (dict *Dict) EvictExpired() {
	var count int
	dict.expire.All(func(key string, ts int64) bool {
		if timer.GetNanoTime() > ts {
			dict.Delete(key)
		}
		count++
		return count <= 20
	})
}
