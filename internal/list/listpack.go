package list

import (
	"encoding/binary"
	"errors"
	"github.com/xgzlucario/rotom/internal/pool"
	"io"
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
	before := it.index
	val, ok = string(it.Prev()), true
	it.data = slices.Delete(it.data, it.index, before)
	it.size--
	return
}

type LpIterator struct {
	*ListPack
	index int
}

func (lp *ListPack) Iterator() *LpIterator {
	return &LpIterator{ListPack: lp}
}

func (lp *ListPack) Encode(writer io.Writer) error {
	size := binary.AppendUvarint(nil, uint64(lp.size))
	_, err := writer.Write(size)
	if err != nil {
		return err
	}
	_, err = writer.Write(lp.data)
	if err != nil {
		return err
	}
	return nil
}

func (lp *ListPack) Decode(src []byte) error {
	size, n := binary.Uvarint(src)
	if n == 0 {
		return errors.New("invalid listpack data format")
	}
	lp.size = uint32(size)
	lp.data = src[n:]
	return nil
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
	bpool.Put(alloc)
}

func (it *LpIterator) RemoveNexts(num int, onDelete func([]byte)) {
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

func (it *LpIterator) ReplaceNext(key string) {
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
		sz := len(data) + 2*SizeUvarint(uint64(len(data)))
		dst = bpool.Get(sz)[:0]
	}
	dst = appendUvarint(dst, len(data), false)
	dst = append(dst, data...)
	return appendUvarint(dst, len(data), true)
}
