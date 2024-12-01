package zset

import (
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/list"
	"math"
	"unsafe"
)

var (
	_ iface.ZSetI = (*ZipZSet)(nil)

	order = binary.LittleEndian
)

// ZipZSet store data as [score1, key1, score2, key2...] in listpack.
// exp: [300, "xgz", 200, "abc", 100, "xgz"]
type ZipZSet struct {
	data *list.ListPack
}

func NewZipZSet() *ZipZSet {
	return &ZipZSet{data: list.NewListPack()}
}

// entry store as [score(8 bytes), key].
func (*ZipZSet) encode(key string, score float64) []byte {
	entry := make([]byte, 0, 8+len(key))
	entry = order.AppendUint64(entry, math.Float64bits(score))
	return append(entry, key...)
}

func (*ZipZSet) decode(entry []byte) (string, float64) {
	return b2s(entry[8:]), b2f(entry[:8])
}

func (zs *ZipZSet) Get(key string) (float64, bool) {
	it, _, score := zs.rank(key)
	if it != nil {
		return score, true
	}
	return 0, false
}

func (zs *ZipZSet) Set(key string, score float64) bool {
	ok := zs.Remove(key)
	zs.insert(key, score)
	return !ok
}

func (zs *ZipZSet) Remove(key string) bool {
	it, _, _ := zs.rank(key)
	if it != nil {
		it.RemoveNext()
	}
	return it != nil
}

func (zs *ZipZSet) rank(key string) (*list.LpIterator, int, float64) {
	it := zs.data.Iterator().SeekLast()
	for i := 0; !it.IsFirst(); i++ {
		prevKey, prevScore := zs.decode(it.Prev())
		if key == prevKey {
			return it, i, prevScore
		}
	}
	return nil, -1, 0
}

func (zs *ZipZSet) insert(key string, score float64) {
	it := zs.data.Iterator().SeekLast()
	for !it.IsFirst() {
		prevKey, prevScore := zs.decode(it.Prev())
		if score < prevScore {
			it.Next()
			goto DO
		}
		if score == prevScore {
			if key < prevKey {
				it.Next()
				goto DO
			}
		}
	}
DO:
	it.Insert(b2s(zs.encode(key, score)))
}

func (zs *ZipZSet) PopMin() (string, float64) {
	entry, ok := zs.data.RPop()
	if ok {
		return zs.decode(s2b(entry))
	}
	return "", 0
}

func (zs *ZipZSet) Rank(key string) int {
	_, index, _ := zs.rank(key)
	return index
}

func (zs *ZipZSet) Scan(fn func(key string, score float64)) {
	it := zs.data.Iterator().SeekLast()
	for !it.IsFirst() {
		entry := it.Prev()
		key, score := zs.decode(entry)
		fn(key, score)
	}
}

func (zs *ZipZSet) Len() int {
	return zs.data.Len()
}

func (zs *ZipZSet) ToZSet() *ZSet {
	zs2 := New()
	zs.Scan(func(key string, score float64) {
		zs2.Set(key, score)
	})
	return zs2
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func s2b(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func b2f(b []byte) float64 {
	return math.Float64frombits(order.Uint64(b))
}
