package hash

import (
	"unsafe"

	"github.com/xgzlucario/rotom/internal/list"
)

var _ MapI = (*ZipMap)(nil)

// ZipMap store datas as [key1, val1, key2, val2...] in listpack.
type ZipMap struct {
	m *list.ListPack
}

func NewZipMap() *ZipMap {
	return &ZipMap{list.NewListPack()}
}

func (zm *ZipMap) Set(key string, val []byte) (newField bool) {
	it := zm.m.Iterator().SeekLast()
	for !it.IsFirst() {
		it.Prev()
		keyBytes := it.Prev()
		if key == b2s(keyBytes) {
			// update val
			it.Next()
			it.ReplaceNext(b2s(val))
			return false
		}
	}
	zm.m.RPush(key, b2s(val))
	return true
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	it := zm.m.Iterator().SeekLast()
	for !it.IsFirst() {
		valBytes := it.Prev()
		keyBytes := it.Prev()
		if key == b2s(keyBytes) {
			return valBytes, true
		}
	}
	return nil, false
}

func (zm *ZipMap) Remove(key string) bool {
	it := zm.m.Iterator().SeekLast()
	for !it.IsFirst() {
		it.Prev()
		keyBytes := it.Prev()
		if key == b2s(keyBytes) {
			it.RemoveNext()
			it.RemoveNext()
			return true
		}
	}
	return false
}

func (zm *ZipMap) Len() int {
	return zm.m.Size() / 2
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	it := zm.m.Iterator().SeekLast()
	for !it.IsFirst() {
		valBytes := it.Prev()
		keyBytes := it.Prev()
		fn(b2s(keyBytes), valBytes)
	}
}

func (zm *ZipMap) ToMap() *Map {
	m := NewMap()
	zm.Scan(func(key string, value []byte) {
		m.Set(key, value)
	})
	return m
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
