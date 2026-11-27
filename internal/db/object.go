package db

import (
	"errors"
	"fmt"
	"iter"

	"github.com/xgzlucario/rotom/internal/codec"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/set"
	"github.com/xgzlucario/rotom/internal/zset"
)

// Values stored in the keyspace, by type and encoding:
//
//	string  []byte (entry.str)
//	hash    *hash.ZipMap
//	list    *list.QuickList
//	set     *set.ZipSet (compact) -> set.Set
//	zset    *zset.ZipZSet (compact) -> *zset.ZSet
//
// Every type switch over values lives in this file.

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// Set is implemented by both set encodings.
type Set interface {
	Add(member string) bool
	Contains(member string) bool
	Remove(member string) bool
	Pop() (string, bool)
	Len() int
	All() iter.Seq[string]
}

// ZSet is implemented by both sorted set encodings.
type ZSet interface {
	Get(member string) (float64, bool)
	Set(member string, score float64) bool
	Remove(member string) bool
	PopMin() (string, float64)
	Rank(member string) int
	Len() int
	All() iter.Seq2[string, float64]
	// Range iterates ranks start..stop inclusive, clamped to [0, Len()).
	Range(start, stop int) iter.Seq2[string, float64]
}

var (
	_ Set  = set.Set(nil)
	_ Set  = (*set.ZipSet)(nil)
	_ ZSet = (*zset.ZSet)(nil)
	_ ZSet = (*zset.ZipZSet)(nil)
)

// Thresholds for converting compact encodings, like redis'
// set-max-listpack-entries / zset-max-listpack-entries / *-max-listpack-value.
const (
	MaxZipSetEntries  = 128
	MaxZipZSetEntries = 128
	MaxZipValueLen    = 64
)

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
	return [...]string{"none", "string", "hash", "list", "set", "zset"}[t]
}

func TypeOf(v any) Type {
	switch v.(type) {
	case []byte:
		return TypeString
	case *hash.ZipMap:
		return TypeHash
	case *list.QuickList:
		return TypeList
	case Set:
		return TypeSet
	case ZSet:
		return TypeZSet
	}
	return TypeNone
}

// Encoding is the physical representation. It is persisted in RDB files,
// so the values must never change.
type Encoding uint8

const (
	EncodingString    Encoding = 1
	EncodingZipMap    Encoding = 3
	EncodingHashSet   Encoding = 4
	EncodingZipSet    Encoding = 5
	EncodingQuickList Encoding = 6
	EncodingSkipList  Encoding = 7
	EncodingZipZSet   Encoding = 8
)

func EncodingOf(v any) Encoding {
	switch v.(type) {
	case []byte:
		return EncodingString
	case *hash.ZipMap:
		return EncodingZipMap
	case set.Set:
		return EncodingHashSet
	case *set.ZipSet:
		return EncodingZipSet
	case *list.QuickList:
		return EncodingQuickList
	case *zset.ZSet:
		return EncodingSkipList
	case *zset.ZipZSet:
		return EncodingZipZSet
	}
	panic(fmt.Sprintf("unknown value type %T", v))
}

// newObject returns an empty container of the given encoding.
func newObject(enc Encoding) (codec.Encoder, error) {
	switch enc {
	case EncodingZipMap:
		return hash.New(), nil
	case EncodingHashSet:
		return set.New(), nil
	case EncodingZipSet:
		return set.NewZipSet(), nil
	case EncodingQuickList:
		return list.New(), nil
	case EncodingSkipList:
		return zset.New(), nil
	case EncodingZipZSet:
		return zset.NewZipZSet(), nil
	}
	return nil, fmt.Errorf("unknown encoding %d", enc)
}

// Type returns the type of key.
func (d *Dict) Type(key string) Type {
	e := d.lookup(key)
	if e == nil {
		return TypeNone
	}
	return TypeOf(e.value())
}

// GetString returns the string at key; ok is false if the key is missing.
// The result is owned by the dict: valid until the key is modified.
func (d *Dict) GetString(key string) (val []byte, ok bool, err error) {
	e := d.lookup(key)
	switch {
	case e == nil:
		return nil, false, nil
	case !e.isString():
		return nil, false, ErrWrongType
	}
	return e.str, true, nil
}

// get returns the container at key as T, or the zero T if key is missing.
func get[T any](d *Dict, key string) (T, error) {
	var zero T
	e := d.lookup(key)
	if e == nil {
		return zero, nil
	}
	return asObject[T](e)
}

// asObject returns the container of e as T. Strings are stored outside
// e.obj, so a string entry has a nil obj, which never matches T.
func asObject[T any](e *entry) (T, error) {
	v, ok := e.obj.(T)
	if !ok {
		return v, ErrWrongType
	}
	return v, nil
}

// getOrCreate returns the container at key as T, creating it if missing.
func getOrCreate[T any](d *Dict, key string, create func() T) (T, error) {
	if e := d.lookup(key); e != nil {
		return asObject[T](e)
	}
	v := create()
	d.setObject(key, v)
	return v, nil
}

// The Get* accessors return nil when the key is missing; the *ForWrite
// accessors create the container.

func (d *Dict) GetHash(key string) (*hash.ZipMap, error)    { return get[*hash.ZipMap](d, key) }
func (d *Dict) GetList(key string) (*list.QuickList, error) { return get[*list.QuickList](d, key) }
func (d *Dict) GetSet(key string) (Set, error)              { return get[Set](d, key) }
func (d *Dict) GetZSet(key string) (ZSet, error)            { return get[ZSet](d, key) }

func (d *Dict) HashForWrite(key string) (*hash.ZipMap, error) {
	return getOrCreate(d, key, hash.New)
}

func (d *Dict) ListForWrite(key string) (*list.QuickList, error) {
	return getOrCreate(d, key, list.New)
}

// SetForWrite returns the set at key, creating it if missing. The caller
// announces the members it is about to add, so that a compact set is
// converted beforehand if it would grow too large.
func (d *Dict) SetForWrite(key string, adding [][]byte) (Set, error) {
	s, err := getOrCreate(d, key, func() Set { return set.NewZipSet() })
	if err != nil {
		return nil, err
	}
	if zs, ok := s.(*set.ZipSet); ok && (zs.Len()+len(adding) > MaxZipSetEntries || maxLen(adding) > MaxZipValueLen) {
		s = zs.ToSet()
		d.setObject(key, s)
	}
	return s, nil
}

// ZSetForWrite is like SetForWrite, for sorted sets.
func (d *Dict) ZSetForWrite(key string, adding [][]byte) (ZSet, error) {
	z, err := getOrCreate(d, key, func() ZSet { return zset.NewZipZSet() })
	if err != nil {
		return nil, err
	}
	if zz, ok := z.(*zset.ZipZSet); ok && (zz.Len()+len(adding) > MaxZipZSetEntries || maxLen(adding) > MaxZipValueLen) {
		z = zz.ToZSet()
		d.setObject(key, z)
	}
	return z, nil
}

func maxLen(items [][]byte) int {
	n := 0
	for _, it := range items {
		n = max(n, len(it))
	}
	return n
}

// DeleteIfEmpty removes a container key once its last element is gone, like redis.
func (d *Dict) DeleteIfEmpty(key string, length int) {
	if length == 0 {
		d.Delete(key)
	}
}
