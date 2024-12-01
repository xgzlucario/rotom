package list

import (
	"encoding/binary"
	"github.com/xgzlucario/rotom/internal/pool"
	"slices"
)

const (
	maxListPackSize = 16 * 1024
)

var bpool = pool.NewBufferPool()

// ListPack is a lists of strings serialization format on Redis.
/*
	ListPack data content:
	+--------+--------+-----+--------+
	| entry0 | entry1 | ... | entryN |
	+--------+--------+-----+--------+
	    |
	  entry0 content:
	+------------+--------------+---------------------+
	|  data_len  |     data     |       data_len      |
	+------------+--------------+---------------------+
	|<- varint ->|<- data_len ->|<- varint(reverse) ->|

	Using this structure, it is fast to iterate from both sides.
*/
type ListPack struct {
	size uint32
	data []byte
}

func NewListPack() *ListPack {
	return &ListPack{data: bpool.Get(32)[:0]}
}

func (lp *ListPack) Len() int {
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
	if lp.Len() == 0 {
		return
	}
	return lp.Iterator().RemoveNext(), true
}

func (lp *ListPack) RPop() (val string, ok bool) {
	if lp.Len() == 0 {
		return
	}
	it := lp.Iterator().SeekLast()
	it.Prev()
	return it.RemoveNext(), true
}

type LpIterator struct {
	*ListPack
	index int
}

func (lp *ListPack) Iterator() *LpIterator {
	return &LpIterator{ListPack: lp}
}

func (it *LpIterator) SeekLast() *LpIterator {
	it.index = len(it.data)
	return it
}

func (it *LpIterator) IsFirst() bool { return it.index == 0 }

func (it *LpIterator) IsLast() bool { return it.index == len(it.data) }

func (it *LpIterator) Next() []byte {
	//
	//    index     dataStartPos    dataEndPos   indexNext
	//      |            |              |            |
	//      +------------+--------------+------------+
	//  --> |  data_len  |     data     |  data_len  |
	//      +------------+--------------+------------+
	//      |<--- n ---->|<- data_len ->|<---- n --->|
	//
	dataLen, n := binary.Uvarint(it.data[it.index:])
	indexNext := it.index + n + int(dataLen) + n

	dataStartPos := it.index + n
	dataEndPos := dataStartPos + int(dataLen)

	data := it.data[dataStartPos:dataEndPos]
	it.index = indexNext

	return data
}

func (it *LpIterator) Prev() []byte {
	//
	//    indexNext  dataStartPos    dataEndPos      index
	//        |            |              |            |
	//  +-----+------------+--------------+------------+
	//  | ... |  data_len  |     data     |  data_len  | <--
	//  +-----+------------+--------------+------------+
	//        |<--- n ---->|<- data_len ->|<---- n --->|
	//
	dataLen, n := uvarintReverse(it.data[:it.index])
	indexNext := it.index - n - int(dataLen) - n

	dataStartPos := indexNext + n
	dataEndPos := dataStartPos + int(dataLen)

	data := it.data[dataStartPos:dataEndPos]
	it.index = indexNext

	return data
}

func (it *LpIterator) Insert(datas ...string) {
	// fast insert to tail
	if it.IsLast() {
		for _, data := range datas {
			it.data = appendEntry(it.data, data)
			it.size++
		}
		return
	}

	var alloc []byte
	for _, data := range datas {
		alloc = appendEntry(alloc, data)
		it.size++
	}
	it.data = slices.Insert(it.data, it.index, alloc...)
}

func (it *LpIterator) RemoveNext() string {
	before := it.index
	res := string(it.Next())
	it.size--
	it.data = slices.Delete(it.data, before, it.index)
	it.index = before
	return res
}

func appendEntry(dst []byte, data string) []byte {
	if dst == nil {
		sz := len(data) + 2*SizeUvarint(uint64(len(data)))
		dst = bpool.Get(sz)[:0]
	}
	dst = appendUvarint(dst, len(data), false)
	dst = append(dst, data...)
	return appendUvarint(dst, len(data), true)
}
