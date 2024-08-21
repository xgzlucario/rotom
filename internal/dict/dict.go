package dict

import (
	"sync/atomic"
	"time"
)

const (
	TTL_FOREVER   = -1
	KEY_NOT_EXIST = -2
)

var (
	_sec  atomic.Uint32
	_nsec atomic.Int64
)

func init() {
	// init backend ticker
	tk := time.NewTicker(time.Millisecond / 10)
	go func() {
		for t := range tk.C {
			_sec.Store(uint32(t.Unix()))
			_nsec.Store(t.UnixNano())
		}
	}()
}

func GetNanoTime() int64 {
	return _nsec.Load()
}

// Dict is the hashmap for Rotom.
type Dict struct {
	data   map[string]any
	expire map[string]int64
}

func New() *Dict {
	return &Dict{
		data:   make(map[string]any, 64),
		expire: make(map[string]int64, 64),
	}
}

func (dict *Dict) Get(key string) (any, int) {
	data, ok := dict.data[key]
	if !ok {
		// key not exist
		return nil, KEY_NOT_EXIST
	}

	nsec, ok := dict.expire[key]
	if !ok {
		return data, TTL_FOREVER
	}

	// key expired
	if nsec < _nsec.Load() {
		delete(dict.data, key)
		delete(dict.expire, key)
		return nil, KEY_NOT_EXIST
	}

	return data, nsec2duration(nsec)
}

func (dict *Dict) Set(key string, data any) {
	dict.data[key] = data
}

func (dict *Dict) SetWithTTL(key string, data any, ttl int64) {
	if ttl > 0 {
		dict.expire[key] = ttl
	}
	dict.data[key] = data
}

func (dict *Dict) Delete(key string) bool {
	_, ok := dict.data[key]
	if !ok {
		return false
	}
	delete(dict.data, key)
	delete(dict.expire, key)
	return true
}

// SetTTL set expire time for key.
// return `0` if key not exist or expired.
// return `1` if set successed.
func (dict *Dict) SetTTL(key string, ttl int64) int {
	_, ok := dict.data[key]
	if !ok {
		// key not exist
		return 0
	}

	// check key if already expired
	nsec, ok := dict.expire[key]
	if ok && nsec < _nsec.Load() {
		delete(dict.data, key)
		delete(dict.expire, key)
		return 0
	}

	// set ttl
	dict.expire[key] = ttl
	return 1
}

func (dict *Dict) EvictExpired() {
	var count int
	for key, nsec := range dict.expire {
		if _nsec.Load() > nsec {
			delete(dict.expire, key)
			delete(dict.data, key)
		}
		count++
		if count > 20 {
			return
		}
	}
}

func nsec2duration(nsec int64) (second int) {
	return int(nsec-_nsec.Load()) / int(time.Second)
}
