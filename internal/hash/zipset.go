package hash

import (
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/zeebo/xxh3"
)

var _ SetI = (*ZipSet)(nil)

// ZipSet store datas as [entry1, entry2, entry3...] in listpack.
/*
	entry format:
	+-----+--------------+
	| key | hash(1 Byte) |
	+-----+--------------+
*/
type ZipSet struct {
	data *list.ListPack
}

func NewZipSet() *ZipSet {
	return &ZipSet{list.NewListPack()}
}

func (ZipSet) encode(key string) []byte {
	buf := make([]byte, len(key)+1)[:0]
	buf = append(buf, key...)
	return append(buf, byte(xxh3.HashString(key)))
}

func (ZipSet) decode(src []byte) (key string) {
	return b2s(src[:len(src)-1])
}

func (zs *ZipSet) Add(key string) (newField bool) {
	if zs.Exist(key) {
		return false
	}
	entry := zs.encode(key)
	zs.data.RPush(b2s(entry))
	return true
}

func (zs *ZipSet) Exist(key string) bool {
	it := zs.data.Iterator().SeekLast()
	hash := byte(xxh3.HashString(key))

	for !it.IsFirst() {
		entry := it.Prev()
		if entry[len(entry)-1] != hash {
			continue
		}
		kb := zs.decode(entry)
		if key == kb {
			return true
		}
	}
	return false
}

func (zs *ZipSet) Remove(key string) bool {
	it := zs.data.Iterator().SeekLast()
	hash := byte(xxh3.HashString(key))

	for !it.IsFirst() {
		entry := it.Prev()
		if entry[len(entry)-1] != hash {
			continue
		}
		kb := zs.decode(entry)
		if key == kb {
			it.RemoveNexts(1, nil)
			return true
		}
	}
	return false
}

func (zs *ZipSet) Scan(fn func(string)) {
	it := zs.data.Iterator().SeekLast()
	for !it.IsFirst() {
		entry := it.Prev()
		key := zs.decode(entry)
		fn(key)
	}
}

func (zs *ZipSet) Pop() (string, bool) {
	key, ok := zs.data.RPop()
	if ok {
		return key[:len(key)-1], true
	}
	return "", false
}

func (zs *ZipSet) Len() int { return zs.data.Size() }

func (zs *ZipSet) ToSet() *Set {
	s := NewSet()
	zs.Scan(func(key string) {
		s.Add(key)
	})
	return s
}
