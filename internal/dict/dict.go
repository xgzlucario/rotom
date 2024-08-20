package dict

import (
	"sync/atomic"
	"time"

	"github.com/dolthub/swiss"
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
	data   *swiss.Map[string, any]
	expire *swiss.Map[string, int64]
}

func New() *Dict {
	return &Dict{
		data:   swiss.NewMap[string, any](64),
		expire: swiss.NewMap[string, int64](64),
	}
}

func (dict *Dict) Get(key string) (any, int) {
	data, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return nil, KEY_NOT_EXIST
	}

	nsec, ok := dict.expire.Get(key)
	if !ok {
		return data, TTL_FOREVER
	}

	// key expired
	if nsec < _nsec.Load() {
		dict.data.Delete(key)
		dict.expire.Delete(key)
		return nil, KEY_NOT_EXIST
	}

	return data, nsec2duration(nsec)
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

func (dict *Dict) Delete(key string) bool {
	_, ok := dict.data.Get(key)
	if !ok {
		return false
	}
	dict.data.Delete(key)
	dict.expire.Delete(key)
	return true
}

// SetTTL set expire time for key.
// return `0` if key not exist or expired.
// return `1` if set successed.
func (dict *Dict) SetTTL(key string, ttl int64) int {
	_, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return 0
	}

	// check key if already expired
	nsec, ok := dict.expire.Get(key)
	if ok && nsec < _nsec.Load() {
		dict.data.Delete(key)
		dict.expire.Delete(key)
		return 0
	}

	// set ttl
	dict.expire.Put(key, ttl)
	return 1
}

func (dict *Dict) EvictExpired() {
	var count int
	dict.expire.Iter(func(key string, nsec int64) bool {
		if _nsec.Load() > nsec {
			dict.expire.Delete(key)
			dict.data.Delete(key)
		}
		count++
		return count > 20
	})
}

func nsec2duration(nsec int64) (second int) {
	return int(nsec-_nsec.Load()) / int(time.Second)
}
