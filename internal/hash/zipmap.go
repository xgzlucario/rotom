package hash

import (
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
	"github.com/zeebo/xxh3"
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

// entry store as [hash(1), vLen(varint), val, key].
func (zm *ZipMap) encode(key string, val []byte) string {
	entry := make([]byte, 0, 2+len(key)+len(val))
	entry = append(entry, byte(xxh3.HashString(key)))
	entry = binary.AppendUvarint(entry, uint64(len(val)))
	entry = append(entry, val...)
	entry = append(entry, key...)
	return b2s(entry)
}

func (*ZipMap) decode(entry []byte) (string, []byte) {
	entry = entry[1:]
	vlen, n := binary.Uvarint(entry)
	val := entry[n : vlen+uint64(n)]
	key := entry[vlen+uint64(n):]
	return b2s(key), val
}

func (zm *ZipMap) seek(key string) (*list.LpIterator, []byte) {
	hash := byte(xxh3.HashString(key))
	for it := zm.data.Iterator().SeekLast(); !it.IsFirst(); {
		entry := it.Prev()
		if hash == entry[0] {
			preKey, preVal := zm.decode(entry)
			if key == preKey {
				return it, preVal
			}
		}
	}
	return nil, nil
}

func (zm *ZipMap) Set(key string, val []byte) bool {
	it, oldVal := zm.seek(key)
	if it != nil {
		if len(oldVal) == len(val) {
			copy(oldVal, val)
			return false
		}
		it.ReplaceNext(zm.encode(key, val))
		return false
	}
	zm.data.RPush(zm.encode(key, val))
	return true
}

func (zm *ZipMap) Get(key string) ([]byte, bool) {
	it, val := zm.seek(key)
	if it != nil {
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
	for it := zm.data.Iterator().SeekLast(); !it.IsFirst(); {
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
