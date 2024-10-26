package hash

import (
	"io"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/list"
)

var _ MapI = (*ZipMap)(nil)

// ZipMap store data as [val1, key1, val2, key2...] in listpack.
type ZipMap struct {
	data *list.ListPack
}

func NewZipMap() *ZipMap {
	return &ZipMap{list.NewListPack()}
}

func (zm *ZipMap) find(key string) (it *list.LpIterator, val []byte) {
	it = zm.data.Iterator().SeekLast()
	for !it.IsFirst() {
		keyData := it.Prev()
		valData := it.Prev()
		if key == b2s(keyData) {
			return it, valData
		}
	}
	return nil, nil
}

func (zm *ZipMap) Set(key string, val []byte) (newField bool) {
	it, oldVal := zm.find(key)
	// update
	if it != nil {
		if len(val) == len(oldVal) {
			copy(oldVal, val)
		} else {
			it.ReplaceNext(b2s(val))
		}
		return false
	}
	// insert
	zm.data.RPush(b2s(val), key)
	return true
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	_, val := zm.find(key)
	if val != nil {
		return val, true
	}
	return nil, false
}

func (zm *ZipMap) Remove(key string) bool {
	it, _ := zm.find(key)
	if it != nil {
		it.RemoveNexts(2, nil)
		return true
	}
	return false
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	it := zm.data.Iterator().SeekLast()
	for !it.IsFirst() {
		key := it.Prev()
		val := it.Prev()
		fn(b2s(key), val)
	}
}

func (zm *ZipMap) ToMap() *Map {
	m := NewMap()
	zm.Scan(func(key string, value []byte) {
		m.Set(key, value)
	})
	return m
}

func (zm *ZipMap) Len() int { return zm.data.Size() / 2 }

func (zm *ZipMap) Encode(writer io.Writer) error {
	return zm.data.Encode(writer)
}

func (zm *ZipMap) Decode(src []byte) error {
	return zm.data.Decode(src)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
