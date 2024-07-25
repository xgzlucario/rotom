package hash

import (
	"github.com/xgzlucario/rotom/internal/list"
)

var _ SetI = (*ZipSet)(nil)

// ZipSet store datas as [key1, key2, key3...] in listpack.
type ZipSet struct {
	m *list.ListPack
}

func NewZipSet() *ZipSet {
	return &ZipSet{list.NewListPack()}
}

func (zs *ZipSet) Add(key string) (newField bool) {
	if zs.Exist(key) {
		return false
	}
	zs.m.RPush(key)
	return true
}

func (zs *ZipSet) Exist(key string) bool {
	it := zs.m.Iterator().SeekLast()
	for !it.IsFirst() {
		if key == b2s(it.Prev()) {
			return true
		}
	}
	return false
}

func (zs *ZipSet) Remove(key string) bool {
	it := zs.m.Iterator().SeekLast()
	for !it.IsFirst() {
		if key == b2s(it.Prev()) {
			it.RemoveNexts(1, nil)
			return true
		}
	}
	return false
}

func (zs *ZipSet) Scan(fn func(string)) {
	it := zs.m.Iterator().SeekLast()
	for !it.IsFirst() {
		fn(b2s(it.Prev()))
	}
}

func (zs *ZipSet) Pop() (string, bool) { return zs.m.RPop() }

func (zs *ZipSet) Len() int { return zs.m.Size() }

func (zs *ZipSet) ToSet() *Set {
	s := NewSet()
	zs.Scan(func(key string) {
		s.Add(key)
	})
	return s
}
