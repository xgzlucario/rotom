package hash

import (
	"bytes"
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/list"
)

var _ iface.MapI = (*ZipMap)(nil)

// ZipMap store data as [entryN, ..., entry1, entry0] in listpack.
type ZipMap struct {
	data *list.ListPack
}

func NewZipMap() *ZipMap {
	return &ZipMap{data: list.NewListPack()}
}

func (zm *ZipMap) buildKey(key string) []byte {
	entry := make([]byte, 0, 16)
	entry = binary.AppendUvarint(entry, uint64(len(key)))
	return append(entry, key...)
}

// entry store as [keyLen, key, val].
func (zm *ZipMap) encode(key string, val []byte) []byte {
	return append(zm.buildKey(key), val...)
}

func (*ZipMap) decode(entry []byte) (string, []byte) {
	klen, n := binary.Uvarint(entry)
	key := entry[n : klen+uint64(n)]
	val := entry[klen+uint64(n):]
	return b2s(key), val
}

func (zm *ZipMap) seek(key string) (it *list.LpIterator, entry []byte) {
	it = zm.data.Iterator().SeekLast()
	prefix := zm.buildKey(key)
	for !it.IsFirst() {
		entry = it.Prev()
		if bytes.HasPrefix(entry, prefix) {
			return it, entry
		}
	}
	return nil, nil
}

func (zm *ZipMap) Set(key string, val []byte) bool {
	it, _ := zm.seek(key)
	entry := b2s(zm.encode(key, val))
	// update
	if it != nil {
		it.ReplaceNext(entry)
		return false
	}
	// insert
	zm.data.RPush(entry)
	return true
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	it, entry := zm.seek(key)
	if it != nil {
		_, val := zm.decode(entry)
		return val, true
	}
	return nil, false
}

func (zm *ZipMap) Remove(key string) bool {
	it, _ := zm.seek(key)
	if it != nil {
		it.RemoveNext()
		return true
	}
	return false
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	it := zm.data.Iterator().SeekLast()
	for !it.IsFirst() {
		key, val := zm.decode(it.Prev())
		fn(key, val)
	}
}

func (zm *ZipMap) ToMap() *Map {
	m := NewMap()
	zm.Scan(func(key string, value []byte) {
		m.Set(key, value)
	})
	return m
}

func (zm *ZipMap) Len() int { return zm.data.Size() }

func (zm *ZipMap) Encode(writer *resp.Writer) error {
	return zm.data.Encode(writer)
}

func (zm *ZipMap) Decode(reader *resp.Reader) error {
	return zm.data.Decode(reader)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
