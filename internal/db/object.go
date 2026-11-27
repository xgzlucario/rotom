package db

import (
	"errors"
	"fmt"

	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
)

// Values stored in the keyspace, by type and encoding:
//
//	string  []byte
//	hash    *hash.ZipMap
//	list    *list.QuickList
//	set     *hash.ZipSet (compact) -> *hash.Set
//	zset    *zset.ZipZSet (compact) -> *zset.ZSet
//
// Every type switch over values lives in this file.

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// Type is the logical type reported by TYPE.
type Type uint8

const (
	TypeNone Type = iota
	TypeString
	TypeHash
	TypeList
	TypeSet
	TypeZSet
)

func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	}
	return "none"
}

// Encoding is the physical representation. It is persisted in RDB files,
// so the values must never change.
type Encoding uint8

const (
	EncodingUnknown   Encoding = 0
	EncodingString    Encoding = 1
	_                 Encoding = 2 // was: integer string
	EncodingZipMap    Encoding = 3
	EncodingHashSet   Encoding = 4
	EncodingZipSet    Encoding = 5
	EncodingQuickList Encoding = 6
	EncodingSkipList  Encoding = 7
	EncodingZipZSet   Encoding = 8
)

func TypeOf(v any) Type {
	switch v.(type) {
	case []byte:
		return TypeString
	case *hash.ZipMap:
		return TypeHash
	case *list.QuickList:
		return TypeList
	case *hash.ZipSet, *hash.Set:
		return TypeSet
	case *zset.ZipZSet, *zset.ZSet:
		return TypeZSet
	}
	return TypeNone
}

func EncodingOf(v any) Encoding {
	switch v.(type) {
	case []byte:
		return EncodingString
	case *hash.ZipMap:
		return EncodingZipMap
	case *hash.Set:
		return EncodingHashSet
	case *hash.ZipSet:
		return EncodingZipSet
	case *list.QuickList:
		return EncodingQuickList
	case *zset.ZSet:
		return EncodingSkipList
	case *zset.ZipZSet:
		return EncodingZipZSet
	}
	return EncodingUnknown
}

// newEncoded returns an empty container of the given encoding.
func newEncoded(enc Encoding) (iface.Encoder, error) {
	switch enc {
	case EncodingZipMap:
		return hash.New(), nil
	case EncodingHashSet:
		return hash.NewSet(), nil
	case EncodingZipSet:
		return hash.NewZipSet(), nil
	case EncodingQuickList:
		return list.New(), nil
	case EncodingSkipList:
		return zset.New(), nil
	case EncodingZipZSet:
		return zset.NewZipZSet(), nil
	}
	return nil, fmt.Errorf("unknown encoding: %d", enc)
}

// Thresholds for converting compact encodings, like redis'
// set-max-listpack-entries / zset-max-listpack-entries / *-max-listpack-value.
const (
	MaxZipSetEntries  = 512
	MaxZipZSetEntries = 256
	MaxZipValueLen    = 64
)

// lookup returns the value of key as T. A missing key returns ok == false.
func lookup[T any](d *Dict, key string) (v T, ok bool, err error) {
	object, ok := d.Get(key)
	if !ok {
		return v, false, nil
	}
	v, ok = object.(T)
	if !ok {
		return v, false, ErrWrongType
	}
	return v, true, nil
}

// lookupOrCreate returns the value of key as T, creating it if missing.
func lookupOrCreate[T any](d *Dict, key string, create func() T) (T, error) {
	v, ok, err := lookup[T](d, key)
	if err != nil || ok {
		return v, err
	}
	v = create()
	d.Set(key, v)
	return v, nil
}

// GetString returns the string at key. ok is false if the key is missing.
func (d *Dict) GetString(key string) ([]byte, bool, error) {
	return lookup[[]byte](d, key)
}

// GetHash returns the hash at key, or nil if the key is missing.
func (d *Dict) GetHash(key string) (iface.MapI, error) {
	v, _, err := lookup[*hash.ZipMap](d, key)
	if v == nil {
		return nil, err
	}
	return v, err
}

func (d *Dict) GetList(key string) (*list.QuickList, error) {
	v, _, err := lookup[*list.QuickList](d, key)
	return v, err
}

func (d *Dict) GetSet(key string) (iface.SetI, error) {
	v, _, err := lookup[iface.SetI](d, key)
	return v, err
}

func (d *Dict) GetZSet(key string) (iface.ZSetI, error) {
	v, _, err := lookup[iface.ZSetI](d, key)
	return v, err
}

// HashForWrite returns the hash at key, creating it if missing.
func (d *Dict) HashForWrite(key string) (iface.MapI, error) {
	return lookupOrCreate(d, key, func() iface.MapI { return hash.New() })
}

func (d *Dict) ListForWrite(key string) (*list.QuickList, error) {
	return lookupOrCreate(d, key, list.New)
}

// SetForWrite returns the set at key, creating it if missing. The caller
// announces that it is about to add `adding` members, the longest of which
// is maxLen bytes, so a compact set can be converted beforehand.
func (d *Dict) SetForWrite(key string, adding, maxLen int) (iface.SetI, error) {
	s, err := lookupOrCreate(d, key, func() iface.SetI { return hash.NewZipSet() })
	if err != nil {
		return nil, err
	}
	if zs, ok := s.(*hash.ZipSet); ok && (zs.Len()+adding > MaxZipSetEntries || maxLen > MaxZipValueLen) {
		s = zs.ToSet()
		d.Set(key, s)
	}
	return s, nil
}

// ZSetForWrite is like SetForWrite, for sorted sets.
func (d *Dict) ZSetForWrite(key string, adding, maxLen int) (iface.ZSetI, error) {
	z, err := lookupOrCreate(d, key, func() iface.ZSetI { return zset.NewZipZSet() })
	if err != nil {
		return nil, err
	}
	if zz, ok := z.(*zset.ZipZSet); ok && (zz.Len()+adding > MaxZipZSetEntries || maxLen > MaxZipValueLen) {
		z = zz.ToZSet()
		d.Set(key, z)
	}
	return z, nil
}

// DeleteIfEmpty removes a container key once its last element is gone, like redis.
func (d *Dict) DeleteIfEmpty(key string, length int) {
	if length == 0 {
		d.Delete(key)
	}
}

// DB is the database a command operates on.
type DB struct {
	*Dict
}

func New() *DB { return &DB{Dict: NewDict()} }
