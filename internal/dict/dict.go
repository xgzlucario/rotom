package dict

import (
	"time"

	"github.com/cockroachdb/swiss"
)

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

	// if object.hasTTL {
	// ttl, ok := dict.expire.Get(key)
	// if ttl > 0 || !ok { //
	// }
	// }

	switch object.typ {
	case TypeZipMapC, TypeZipSetC:
		object.data.(Compressor).Decompress()
		object.typ -= 1
	}

	object.updateLRU()

	return object, true
}

func (dict *Dict) Set(key string, typ Type, data any) {
	dict.data.Put(key, &Object{
		typ:  typ,
		lru:  uint32(time.Now().Unix()),
		data: data,
	})
}

func (dict *Dict) Remove(key string) bool {
	_, ok := dict.data.Get(key)
	dict.data.Delete(key)
	dict.expire.Delete(key)
	return ok
}

func (dict *Dict) SetTTL(key string, expiration int64) bool {
	_, ok := dict.data.Get(key)
	if !ok {
		return false
	}
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
