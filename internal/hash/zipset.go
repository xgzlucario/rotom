package hash

import (
	"github.com/xgzlucario/rotom/internal/list"
)

var _ SetI = (*ZipSet)(nil)

// ZipSet store datas as [key1, key2, key3...] in listpack.
type ZipSet struct {
	data *list.ListPack
}

func NewZipSet() *ZipSet {
	return &ZipSet{list.NewListPack()}
}

func (zs *ZipSet) Add(key string) (newField bool) {
	if zs.Exist(key) {
		return false
	}
	zs.data.RPush(key)
	return true
}

func (zs *ZipSet) Exist(key string) bool {
	it := zs.data.Iterator().SeekLast()

	for !it.IsFirst() {
		entry := it.Prev()
		if key == b2s(entry) {
			return true
		}
	}
	return false
}

func (zs *ZipSet) Remove(key string) bool {
	it := zs.data.Iterator().SeekLast()

	for !it.IsFirst() {
		entry := it.Prev()
		if key == b2s(entry) {
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
		fn(b2s(entry))
	}
}

func (zs *ZipSet) Pop() (string, bool) {
	return zs.data.RPop()
}

func (zs *ZipSet) Len() int { return zs.data.Size() }

func (zs *ZipSet) ToSet() *Set {
	s := NewSet()
	zs.Scan(func(key string) {
		s.Add(key)
	})
	return s
}
