package hash

import (
	"encoding/binary"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/list"
	"github.com/zeebo/xxh3"
)

var _ MapI = (*ZipMap)(nil)

// ZipMap store datas as [entry1, entry2, entry3...] in listpack.
type ZipMap struct {
	data *list.ListPack
}

// zipmapEntry is data format in zipmap.
/*
	entry format:
	+-----------------+-----+-----+--------------+
	| val_len(varint) | val | key | hash(1 Byte) |
	+-----------------+-----+-----+--------------+
*/
type zipmapEntry struct{}

func (zipmapEntry) encode(key string, val []byte) []byte {
	buf := make([]byte, len(key)+len(val)+2)[:0]
	buf = binary.AppendUvarint(buf, uint64(len(val)))
	buf = append(buf, val...)
	buf = append(buf, key...)
	buf = append(buf, byte(xxh3.HashString(key)))
	return buf
}

func (zipmapEntry) decode(buf []byte) (key string, val []byte) {
	vlen, n := binary.Uvarint(buf)
	val = buf[n : n+int(vlen)]
	key = b2s(buf[n+int(vlen) : len(buf)-1])
	return
}

func NewZipMap() *ZipMap {
	return &ZipMap{list.NewListPack()}
}

func (zm *ZipMap) find(key string) (it *list.LpIterator, val []byte) {
	it = zm.data.Iterator().SeekLast()
	hash := byte(xxh3.HashString(key))

	for !it.IsFirst() {
		entry := it.Prev()

		if entry[len(entry)-1] != hash {
			continue
		}

		kb, vb := zipmapEntry{}.decode(entry)
		if key == kb {
			return it, vb
		}
	}
	return nil, nil
}

func (zm *ZipMap) Set(key string, val []byte) (newField bool) {
	entry := zipmapEntry{}.encode(key, val)
	it, _ := zm.find(key)
	if it != nil {
		it.ReplaceNext(b2s(entry))
		return false
	}
	zm.data.RPush(b2s(entry))
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
		it.RemoveNexts(1, nil)
		return true
	}
	return false
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	it := zm.data.Iterator().SeekLast()
	for !it.IsFirst() {
		kb, vb := zipmapEntry{}.decode(it.Prev())
		fn(kb, vb)
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

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
