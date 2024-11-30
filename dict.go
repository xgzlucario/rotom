package main

import (
	"github.com/cockroachdb/swiss"
	"time"
)

// Dict is the hashmap for rotom.
type Dict struct {
	data   *swiss.Map[string, any]
	expire *swiss.Map[string, int64]
}

func New() *Dict {
	return &Dict{
		data:   swiss.New[string, any](64),
		expire: swiss.New[string, int64](64),
	}
}

func (dict *Dict) Get(key string) (any, int64) {
	data, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return nil, KeyNotExist
	}

	ts, ok := dict.expire.Get(key)
	if !ok {
		return data, KeepTTL
	}

	// key expired
	now := time.Now().UnixNano()
	if ts < now {
		dict.delete(key)
		return nil, KeyNotExist
	}

	return data, (ts - now) / int64(time.Second)
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
	if ok && ts < time.Now().UnixNano() {
		dict.delete(key)
		return 0
	}

	// set ttl
	dict.expire.Put(key, ttl)
	return 1
}

func (dict *Dict) EvictExpired() {
	var count int
	now := time.Now().UnixNano()
	dict.expire.All(func(key string, ts int64) bool {
		if now > ts {
			dict.Delete(key)
		}
		count++
		return count <= 20
	})
}
