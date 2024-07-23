package dict

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/swiss"
)

var (
	_sec  atomic.Uint32
	_nsec atomic.Int64
)

func init() {
	// init backend ticker
	tk := time.NewTicker(time.Microsecond)
	go func() {
		for t := range tk.C {
			_sec.Store(uint32(t.Unix()))
			_nsec.Store(t.UnixNano())
		}
	}()
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

func (dict *Dict) Get(key string) (*Object, bool) {
	object, ok := dict.data.Get(key)
	if !ok {
		return nil, false
	}

	if object.hasTTL {
		nsecTTL, ok := dict.expire.Get(key)
		if !ok || nsecTTL < _nsec.Load() {
			// expired
			dict.data.Delete(key)
			dict.expire.Delete(key)
			return nil, false
		}
	}

	switch object.typ {
	case TypeZipMapC, TypeZipSetC:
		object.data.(Compressor).Decompress()
		object.typ -= 1
	}

	// update access time
	object.lastAccessd = _sec.Load()

	return object, true
}

func (dict *Dict) Set(key string, typ Type, data any) {
	dict.data.Put(key, &Object{
		typ:         typ,
		lastAccessd: _sec.Load(),
		data:        data,
	})
}

func (dict *Dict) Remove(key string) bool {
	_, ok := dict.data.Get(key)
	dict.data.Delete(key)
	dict.expire.Delete(key)
	return ok
}

func (dict *Dict) SetTTL(key string, expiration int64) bool {
	object, ok := dict.data.Get(key)
	if !ok {
		return false
	}
	object.hasTTL = true
	dict.expire.Put(key, expiration)
	return true
}

func (dict *Dict) EvictExpired() {
	nanosec := time.Now().UnixNano()
	count := 0
	dict.expire.All(func(key string, value int64) bool {
		if nanosec > value {
			dict.expire.Delete(key)
			dict.data.Delete(key)
		}
		count++
		return count <= 20
	})
}
