package list

import (
	"bytes"
	"encoding/binary"
	"slices"
)

var (
	maxListPackSize = 8 * 1024

	bpool = NewBufferPool()
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
	size uint16
	data []byte
}

func NewListPack() *ListPack {
	return &ListPack{data: make([]byte, 0, 32)}
}

func (lp *ListPack) Size() int {
	return int(lp.size)
}

// lpIterator is listpack iterator.
type lpIterator func(data []byte, index int, startPos, endPos int) (stop bool)

func (lp *ListPack) iterFront(start, end int, f lpIterator) {
	if end == -1 {
		end = lp.Size()
	}
	var index int
	for i := 0; i < end && index < len(lp.data); i++ {
		//
		//    index     dataStartPos    dataEndPos            indexNext
		//      |            |              |                     |
		//      +------------+--------------+---------------------+-----+
		//  --> |  data_len  |     data     |      entry_len      | ... |
		//      +------------+--------------+---------------------+-----+
		//      |<--- n ---->|<- data_len ->|<-- size_entry_len ->|
		//
		dataLen, n := binary.Uvarint(lp.data[index:])
		indexNext := index + n + int(dataLen) + +SizeUvarint(dataLen+uint64(n))

		if i >= start {
			dataStartPos := index + n
			dataEndPos := dataStartPos + int(dataLen)

			data := lp.data[dataStartPos:dataEndPos]
			if f(data, i, index, indexNext) {
				return
			}
		}
		index = indexNext
	}
}

func (lp *ListPack) iterBack(start, end int, f lpIterator) {
	if end == -1 {
		end = lp.Size()
	}
	var index = len(lp.data)
	for i := 0; i < end && index > 0; i++ {
		//
		//    indexNext  dataStartPos    dataEndPos               index
		//        |            |              |                     |
		//  +-----+------------+--------------+---------------------+
		//  | ... |  data_len  |     data     |      entry_len      | <--
		//  +-----+------------+--------------+---------------------+
		//        |<--- n ---->|<- data_len ->|<-- size_entry_len ->|
		//        |<------ entry_len -------->|
		//
		entryLen, sizeEntryLen := uvarintReverse(lp.data[:index])
		indexNext := index - int(entryLen) - sizeEntryLen

		if i >= start {
			dataLen, n := binary.Uvarint(lp.data[indexNext:])
			dataStartPos := indexNext + n
			dataEndPos := dataStartPos + int(dataLen)

			data := lp.data[dataStartPos:dataEndPos]
			if f(data, i, indexNext, index) {
				return
			}
		}
		index = indexNext
	}
}

// find quickly locates the element based on index.
// When the target index is in the first half, use forward traversal;
// otherwise, use reverse traversal.
func (lp *ListPack) find(index int, fn func(old []byte, index int, startPos, endPos int)) {
	if lp.size == 0 || index >= lp.Size() || index < 0 {
		return
	}
	if index <= lp.Size()/2 {
		lp.iterFront(index, index+1, func(old []byte, index int, startPos, endPos int) bool {
			fn(old, index, startPos, endPos)
			return true
		})
	} else {
		index = lp.Size() - index - 1
		lp.iterBack(index, index+1, func(old []byte, index int, startPos, endPos int) bool {
			fn(old, index, startPos, endPos)
			return true
		})
	}
}

// Insert datas into listpack.
// index = 0: same as `LPush`
// index = -1: same as `RPush`
func (lp *ListPack) Insert(index int, datas ...string) {
	if index == -1 {
		index = lp.Size()
	}

	// rpush
	if index == lp.Size() {
		for _, data := range datas {
			lp.data = appendEntry(lp.data, data)
			lp.size++
		}
		return
	}

	// insert
	if index < lp.Size() {
		var pos int
		lp.find(index, func(_ []byte, _, startPos, _ int) {
			pos = startPos
		})

		var alloc []byte
		for _, data := range datas {
			alloc = appendEntry(alloc, data)
			lp.size++
		}
		lp.data = slices.Insert(lp.data, pos, alloc...)
		bpool.Put(alloc)
	}
}

func (lp *ListPack) Set(index int, data string) (ok bool) {
	if index == -1 {
		index = lp.Size() - 1
	}
	lp.find(index, func(old []byte, _, startPos, endPos int) {
		if len(data) == len(old) {
			copy(old, data)
		} else {
			alloc := appendEntry(nil, data)
			lp.data = slices.Replace(lp.data, startPos, endPos, alloc...)
			bpool.Put(alloc)
		}
		ok = true
	})
	return
}

func (lp *ListPack) Remove(index int) (val string, ok bool) {
	if index == -1 {
		index = lp.Size() - 1
	}
	lp.find(index, func(data []byte, _, startPos, endPos int) {
		val = string(data)
		lp.data = slices.Delete(lp.data, startPos, endPos)
		lp.size--
		ok = true
	})
	return
}

func (lp *ListPack) RemoveFirst(data string) (res int, ok bool) {
	lp.iterFront(0, -1, func(old []byte, index int, startPos, endPos int) bool {
		if bytes.Equal(s2b(&data), old) {
			lp.data = slices.Delete(lp.data, startPos, endPos)
			lp.size--
			res, ok = index, true
		}
		return ok
	})
	return
}

func (lp *ListPack) Range(start, end int, fn func(data []byte, index int) (stop bool)) {
	lp.iterFront(start, end, func(data []byte, index int, _, _ int) bool {
		return fn(data, index)
	})
}

func (lp *ListPack) RevRange(start, end int, fn func(data []byte, index int) (stop bool)) {
	lp.iterBack(start, end, func(data []byte, index int, _, _ int) bool {
		return fn(data, index)
	})
}

// encode data to [data_len, data, entry_len].
func appendEntry(dst []byte, data string) []byte {
	if dst == nil {
		dst = bpool.Get(maxListPackSize)[:0]
	}
	before := len(dst)
	dst = appendUvarint(dst, len(data), false)
	dst = append(dst, data...)
	return appendUvarint(dst, len(dst)-before, true)
}
