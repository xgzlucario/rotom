package dict

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/swiss"
)

const (
	TTL_DEFAULT   = -1
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
	data   *swiss.Map[string, *Object]
	expire *swiss.Map[string, int64]
}

func New() *Dict {
	return &Dict{
		data:   swiss.New[string, *Object](64),
		expire: swiss.New[string, int64](64),
	}
}

func (dict *Dict) Get(key string) (*Object, int) {
	object, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return nil, KEY_NOT_EXIST
	}

	object.lastAccessd = _sec.Load()

	if object.hasTTL {
		nsec, _ := dict.expire.Get(key)
		// key expired
		if nsec < _nsec.Load() {
			dict.data.Delete(key)
			dict.expire.Delete(key)
			return nil, KEY_NOT_EXIST
		}
		return object, nsec2duration(nsec)
	}

	return object, TTL_DEFAULT
}

func (dict *Dict) Set(key string, data any) {
	dict.data.Put(key, &Object{
		typ:         typeOfData(data),
		lastAccessd: _sec.Load(),
		data:        data,
	})
}

func (dict *Dict) SetWithTTL(key string, data any, ttl int64) {
	dict.data.Put(key, &Object{
		typ:         typeOfData(data),
		lastAccessd: _sec.Load(),
		data:        data,
		hasTTL:      true,
	})
	dict.expire.Put(key, ttl)
}

func (dict *Dict) Delete(key string) bool {
	object, ok := dict.data.Get(key)
	if !ok {
		return false
	}
	dict.data.Delete(key)
	if object.hasTTL {
		dict.expire.Delete(key)
	}
	return true
}

// SetTTL set expire time for key.
// return `0` if key not exist or expired.
// return `1` if set successed.
func (dict *Dict) SetTTL(key string, ttl int64) int {
	object, ok := dict.data.Get(key)
	if !ok {
		// key not exist
		return 0
	}
	if object.hasTTL {
		nsec, _ := dict.expire.Get(key)
		// key expired
		if nsec < _nsec.Load() {
			dict.data.Delete(key)
			dict.expire.Delete(key)
			return 0
		}
	}
	// set ttl
	object.hasTTL = true
	dict.expire.Put(key, ttl)
	return 1
}

func (dict *Dict) EvictExpired() {
	var count int
	dict.expire.All(func(key string, nsec int64) bool {
		if _nsec.Load() > nsec {
			dict.expire.Delete(key)
			dict.data.Delete(key)
		}
		count++
		return count <= 20
	})
}
