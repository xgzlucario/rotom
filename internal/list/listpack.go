package list

import (
	"encoding/binary"
	"slices"

	"github.com/klauspost/compress/zstd"
	"github.com/xgzlucario/rotom/internal/pkg"
)

var (
	maxListPackSize = 8 * 1024

	bpool = pkg.NewBufferPool()

	encoder, _ = zstd.NewWriter(nil)
	decoder, _ = zstd.NewReader(nil)
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
	compress bool
	size     uint16
	data     []byte
}

func NewListPack() *ListPack {
	return &ListPack{data: bpool.Get(32)[:0]}
}

func (lp *ListPack) Size() int {
	return int(lp.size)
}

func (lp *ListPack) LPush(data ...string) {
	lp.Iterator().Insert(data...)
}

func (lp *ListPack) RPush(data ...string) {
	lp.Iterator().SeekEnd().Insert(data...)
}

func (lp *ListPack) LPop() (string, bool) {
	return lp.Iterator().RemoveNext()
}

func (lp *ListPack) RPop() (string, bool) {
	return lp.Iterator().SeekEnd().RemovePrev()
}

func (lp *ListPack) Compress() {
	if !lp.compress {
		lp.data = encoder.EncodeAll(lp.data, make([]byte, 0, len(lp.data)/4))
		lp.compress = true
	}
}

func (lp *ListPack) Decompress() {
	if lp.compress {
		lp.data, _ = decoder.DecodeAll(lp.data, nil)
		lp.compress = false
	}
}

type lpIterator struct {
	*ListPack
	index int
}

func (lp *ListPack) Iterator() *lpIterator {
	return &lpIterator{ListPack: lp}
}

func (it *lpIterator) SeekBegin() *lpIterator {
	it.index = 0
	return it
}

func (it *lpIterator) SeekEnd() *lpIterator {
	it.index = len(it.data)
	return it
}

func (it *lpIterator) IsBegin() bool { return it.index == 0 }

func (it *lpIterator) IsEnd() bool { return it.index == len(it.data) }

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
	if it.IsEnd() {
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

func (it *lpIterator) RemoveNext() (string, bool) {
	if it.IsEnd() {
		return "", false
	}
	before := it.index
	data := string(it.Next())
	after := it.index
	it.data = slices.Delete(it.data, before, after)
	it.index = min(before, after)
	it.size--
	return data, true
}

func (it *lpIterator) RemovePrev() (string, bool) {
	if it.IsBegin() {
		return "", false
	}
	before := it.index
	data := string(it.Prev())
	after := it.index
	it.data = slices.Delete(it.data, after, before)
	it.size--
	return data, true
}

func appendEntry(dst []byte, data string) []byte {
	if dst == nil {
		dst = bpool.Get(maxListPackSize)[:0]
	}
	before := len(dst)
	dst = appendUvarint(dst, len(data), false)
	dst = append(dst, data...)
	return appendUvarint(dst, len(dst)-before, true)
}
