package hash

import (
	"encoding/binary"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/list"
)

var _ MapI = (*ZipMap)(nil)

// ZipMap store datas as [entry1, entry2, entry3...] in listpack.
type ZipMap struct {
	m *list.ListPack
}

func NewZipMap() *ZipMap {
	return &ZipMap{list.NewListPack()}
}

// encodeEntry encode data to [vlen, val, key].
func encodeEntry(key string, val []byte) []byte {
	buf := make([]byte, 0, len(key)+len(val)+1)
	buf = binary.AppendUvarint(buf, uint64(len(val)))
	buf = append(buf, val...)
	return append(buf, key...)
}

func decodeEntry(buf []byte) (key string, val []byte) {
	vlen, n := binary.Uvarint(buf)
	val = buf[n : n+int(vlen)]
	key = b2s(buf[n+int(vlen):])
	return
}

func (zm *ZipMap) Set(key string, val []byte) (newField bool) {
	it := zm.m.Iterator().SeekLast()
	b := key[len(key)-1]
	newEntry := b2s(encodeEntry(key, val))

	for !it.IsFirst() {
		entry := it.Prev()
		// fast equal
		if entry[len(entry)-1] != b {
			continue
		}

		kb, _ := decodeEntry(entry)
		if key == kb {
			it.ReplaceNext(newEntry)
			return false
		}
	}
	zm.m.RPush(newEntry)
	return true
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	it := zm.m.Iterator().SeekLast()
	b := key[len(key)-1]

	for !it.IsFirst() {
		entry := it.Prev()
		// fast equal
		if entry[len(entry)-1] != b {
			continue
		}

		kb, vb := decodeEntry(entry)
		if key == kb {
			return vb, true
		}
	}
	return nil, false
}

func (zm *ZipMap) Remove(key string) bool {
	it := zm.m.Iterator().SeekLast()
	b := key[len(key)-1]

	for !it.IsFirst() {
		entry := it.Prev()
		// fast equal
		if entry[len(entry)-1] != b {
			continue
		}

		kb, _ := decodeEntry(entry)
		if key == kb {
			it.RemoveNexts(1, nil)
			return true
		}
	}
	return false
}

func (zm *ZipMap) Scan(fn func(string, []byte)) {
	it := zm.m.Iterator().SeekLast()
	for !it.IsFirst() {
		kb, vb := decodeEntry(it.Prev())
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

func (zm *ZipMap) Len() int { return zm.m.Size() }

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
