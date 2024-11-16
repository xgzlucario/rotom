package zset

import (
	"bytes"
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/list"
	"math"
	"strings"
	"unsafe"
)

var (
	order = binary.LittleEndian
)

// ZipZSet store data as [score1, key1, score2, key2...] in listpack.
// exp: [100, "xgz", 200, "abc", 300, "xgz"]
type ZipZSet struct {
	data *list.ListPack
}

func NewZipZSet() *ZipZSet {
	return &ZipZSet{
		data: list.NewListPack(),
	}
}

func (zs *ZipZSet) Get(key string) (float64, bool) {
	it, val := zs.seek(key)
	if it != nil {
		return b2f(val), true
	}
	return 0, false
}

func (zs *ZipZSet) Set(key string, score float64) (newField bool) {
	it, _ := zs.seek(key)
	if it != nil {
		it.RemoveNexts(2, nil)
	}
	zs.insert(key, score)
	return it == nil
}

func (zs *ZipZSet) Remove(key string) bool {
	it, _ := zs.seek(key)
	if it != nil {
		it.RemoveNexts(2, nil)
	}
	return false
}

func (zs *ZipZSet) seek(key string) (it *list.LpIterator, val []byte) {
	it = zs.data.Iterator().SeekLast()
	for !it.IsFirst() {
		kBytes := it.Prev()
		vBytes := it.Prev()
		if key == b2s(kBytes) {
			return it, vBytes
		}
	}
	return nil, nil
}

func (zs *ZipZSet) insert(key string, score float64) {
	it := zs.data.Iterator().SeekLast()
	bScore := f2b(score)
	for !it.IsFirst() {
		kBytes := it.Prev()
		vBytes := it.Prev()

		// compare score first
		n := bytes.Compare(bScore, vBytes)
		if n < 0 {
			continue
		}
		if n == 0 {
			if strings.Compare(key, b2s(kBytes)) < 0 {
				continue
			}
		}
		goto DO
	}
DO:
	it.Insert(b2s(bScore), key)
}

func (zs *ZipZSet) Len() int {
	return zs.data.Size() / 2
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func f2b(f float64) []byte {
	return order.AppendUint64(nil, math.Float64bits(f))
}

func b2f(b []byte) float64 {
	return math.Float64frombits(order.Uint64(b))
}
