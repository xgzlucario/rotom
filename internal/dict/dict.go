package dict

import (
	"github.com/bytedance/sonic"
	"strconv"
	"time"
)

type Object interface {
	GetType() ObjectType
	Encode() ([]byte, error)
	Decode([]byte) error
}

type ObjectType byte

const (
	TypeString ObjectType = iota + 1
	TypeInteger
	TypeMap
	TypeZipMap
	TypeSet
	TypeZipSet
	TypeList
	TypeZSet
)

const (
	TTL_FOREVER   = -1
	KEY_NOT_EXIST = -2
)

// Dict is the hashmap for rotom.
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

	ts, ok := dict.expire[key]
	if !ok {
		return data, TTL_FOREVER
	}

	// key expired
	now := time.Now().UnixNano()
	if ts < now {
		delete(dict.data, key)
		delete(dict.expire, key)
		return nil, KEY_NOT_EXIST
	}

	return data, int(ts-now) / int(time.Second)
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
// return `1` if set success.
func (dict *Dict) SetTTL(key string, ttl int64) int {
	_, ok := dict.data[key]
	if !ok {
		// key not exist
		return 0
	}

	// check key if already expired
	ts, ok := dict.expire[key]
	if ok && ts < time.Now().UnixNano() {
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
	now := time.Now().UnixNano()
	for key, ts := range dict.expire {
		if now > ts {
			delete(dict.expire, key)
			delete(dict.data, key)
		}
		count++
		if count > 20 {
			return
		}
	}
}

type KVEntry struct {
	ObjectType ObjectType `json:"p"`
	Key        string     `json:"k"`
	Ttl        int64      `json:"t"`
	Data       []byte     `json:"v"`
}

type RESPWriter interface {
	WriteArrayHead(int)
	WriteBulk([]byte)
}

type RESPReader interface {
	ReadArrayHead(int)
	ReadBulk([]byte)
}

func (dict *Dict) EncodeTo(writer RESPWriter) error {
	writer.WriteArrayHead(len(dict.data))
	var entry KVEntry

	for k, v := range dict.data {
		ttl, ok := dict.expire[k]
		if !ok {
			ttl = TTL_FOREVER
		}
		entry.Key = k
		entry.Ttl = ttl

		switch vtype := v.(type) {
		case string:
			entry.ObjectType = TypeString
			entry.Data = []byte(vtype)
		case []byte:
			entry.ObjectType = TypeString
			entry.Data = vtype
		case int:
			entry.ObjectType = TypeInteger
			entry.Data = []byte(strconv.Itoa(vtype))
		case Object:
			entry.ObjectType = vtype.GetType()
			data, err := vtype.Encode()
			if err != nil {
				return err
			}
			entry.Data = data
		}
		entryBytes, _ := sonic.Marshal(entry)
		writer.WriteBulk(entryBytes)
	}
	return nil
}
