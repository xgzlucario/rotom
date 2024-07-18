package list

import (
	"encoding/binary"
	"slices"

	"github.com/pierrec/lz4/v4"
	"github.com/xgzlucario/rotom/internal/utils"
)

const (
	maxListPackSize = 8 * 1024
)

var (
	bpool = utils.NewBufferPool()

	c lz4.Compressor
)

// ListPack is a lists of strings serialization format on Redis.
/*
	ListPack data content:
	+--------+--------+-----+--------+
	| entry0 | entry1 | ... | entryN |
	+--------+--------+-----+--------+
	    |
	  entry0 content:
	+------------+--------------+---------------------+
	|  data_len  |     data     |      entry_len      |
	+------------+--------------+---------------------+
	|<- varint ->|<- data_len ->|<- varint(reverse) ->|
	|<------- entry_len ------->|

	Using this structure, it is fast to iterate from both sides.
*/
type ListPack struct {
	srcLen uint32 // srcLen is data size before compressed.
	size   uint32
	data   []byte
}

func NewListPack() *ListPack {
	return &ListPack{data: make([]byte, 0, 32)}
}

func (lp *ListPack) Size() int {
	return int(lp.size)
}

func (lp *ListPack) LPush(data ...string) {
	if len(data) > 1 {
		slices.Reverse(data)
	}
	lp.Iterator().Insert(data...)
}

func (lp *ListPack) RPush(data ...string) {
	lp.Iterator().SeekLast().Insert(data...)
}

func (lp *ListPack) LPop() (val string, ok bool) {
	lp.Iterator().RemoveNexts(1, func(b []byte) {
		val, ok = string(b), true
	})
	return
}

func (lp *ListPack) RPop() (val string, ok bool) {
	if lp.Size() == 0 {
		return "", false
	}
	it := lp.Iterator().SeekLast()
	it.Prev()
	it.RemoveNexts(1, func(b []byte) {
		val, ok = string(b), true
	})
	return
}

func (lp *ListPack) Compress() {
	if lp.srcLen > 0 {
		return
	}
	if len(lp.data) == 0 {
		return
	}
	lp.srcLen = uint32(len(lp.data))

	dst := bpool.Get(lz4.CompressBlockBound(len(lp.data)))
	n, _ := c.CompressBlock(lp.data, dst)

	bpool.Put(lp.data)
	lp.data = dst[:n]
}

func (lp *ListPack) Decompress() {
	if lp.srcLen == 0 {
		return
	}
	dst := bpool.Get(int(lp.srcLen))
	n, _ := lz4.UncompressBlock(lp.data, dst)

	bpool.Put(lp.data)
	lp.data = dst[:n]
	lp.srcLen = 0
}

type lpIterator struct {
	*ListPack
	index int
}

func (lp *ListPack) Iterator() *lpIterator {
	if lp.srcLen > 0 {
		lp.Decompress()
	}
	return &lpIterator{ListPack: lp}
}

func (it *lpIterator) SeekFirst() *lpIterator {
	it.index = 0
	return it
}

func (it *lpIterator) SeekLast() *lpIterator {
	it.index = len(it.data)
	return it
}

func (it *lpIterator) IsFirst() bool { return it.index == 0 }

func (it *lpIterator) IsLast() bool { return it.index == len(it.data) }

func (it *lpIterator) Next() []byte {
	//
	//    index     dataStartPos    dataEndPos            indexNext
	//      |            |              |                     |
	//      +------------+--------------+---------------------+-----+
	//  --> |  data_len  |     data     |      entry_len      | ... |
	//      +------------+--------------+---------------------+-----+
	//      |<--- n ---->|<- data_len ->|<-- size_entry_len ->|
	//
	dataLen, n := binary.Uvarint(it.data[it.index:])
	indexNext := it.index + n + int(dataLen) + SizeUvarint(dataLen+uint64(n))

	dataStartPos := it.index + n
	dataEndPos := dataStartPos + int(dataLen)

	data := it.data[dataStartPos:dataEndPos]
	it.index = indexNext

	return data
}

func (it *lpIterator) Prev() []byte {
	//
	//    indexNext  dataStartPos    dataEndPos               index
	//        |            |              |                     |
	//  +-----+------------+--------------+---------------------+
	//  | ... |  data_len  |     data     |      entry_len      | <--
	//  +-----+------------+--------------+---------------------+
	//        |<--- n ---->|<- data_len ->|<-- size_entry_len ->|
	//        |<------ entry_len -------->|
	//
	entryLen, sizeEntryLen := uvarintReverse(it.data[:it.index])
	indexNext := it.index - int(entryLen) - sizeEntryLen

	dataLen, n := binary.Uvarint(it.data[indexNext:])
	dataStartPos := indexNext + n
	dataEndPos := dataStartPos + int(dataLen)

	data := it.data[dataStartPos:dataEndPos]
	it.index = indexNext

	return data
}

func (it *lpIterator) Insert(datas ...string) {
	var alloc []byte
	for _, data := range datas {
		alloc = appendEntry(alloc, data)
		it.size++
	}
	it.data = slices.Insert(it.data, it.index, alloc...)
	bpool.Put(alloc)
}

func (it *lpIterator) RemoveNexts(num int, onDelete func([]byte)) {
	before := it.index

	for i := 0; i < num; i++ {
		if it.IsLast() {
			break
		}
		next := it.Next()
		if onDelete != nil {
			onDelete(next)
		}
		it.size--
	}

	after := it.index
	it.data = slices.Delete(it.data, before, after)
	it.index = before
}

func (it *lpIterator) ReplaceNext(key string) {
	if it.IsLast() {
		return
	}
	before := it.index
	it.Next()
	after := it.index

	alloc := appendEntry(nil, key)
	it.data = slices.Replace(it.data, before, after, alloc...)
	it.index = before
	bpool.Put(alloc)
}

func appendEntry(dst []byte, data string) []byte {
	if dst == nil {
		dst = bpool.Get(len(data) + 10)[:0]
	}
	before := len(dst)
	dst = appendUvarint(dst, len(data), false)
	dst = append(dst, data...)
	return appendUvarint(dst, len(dst)-before, true)
}
