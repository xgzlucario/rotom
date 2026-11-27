// Package list implements the redis list type as a quicklist.
package list

import (
	"iter"

	"github.com/xgzlucario/rotom/internal/codec"
	"github.com/xgzlucario/rotom/internal/listpack"
)

// maxNodeSize is the listpack size (bytes) after which a new node is started,
// like redis' list-max-listpack-size -2 (8KB).
const maxNodeSize = 8 * 1024

// QuickList is a doubly linked list of listpacks, as in redis:
//
//	        +-----------+     +-----------+             +-----------+
//	head -> | listpack0 | <-> | listpack1 | <-> ... <-> | listpackN | <- tail
//	        +-----------+     +-----------+             +-----------+
//
// Small listpacks keep memory compact and cache friendly; the links keep
// pushes and pops at both ends O(1) for any list length.
type QuickList struct {
	head, tail *node
	length     int
}

type node struct {
	prev, next *node
	lp         listpack.ListPack
}

func New() *QuickList { return &QuickList{} }

func (ql *QuickList) Len() int { return ql.length }

func (ql *QuickList) LPush(v string) {
	if ql.head == nil || ql.head.lp.Size() >= maxNodeSize {
		n := &node{next: ql.head}
		if ql.head != nil {
			ql.head.prev = n
		} else {
			ql.tail = n
		}
		ql.head = n
	}
	ql.head.lp.PushFront(v)
	ql.length++
}

func (ql *QuickList) RPush(v string) {
	if ql.tail == nil || ql.tail.lp.Size() >= maxNodeSize {
		ql.pushNode(&node{})
	}
	ql.tail.lp.PushBack(v)
	ql.length++
}

func (ql *QuickList) pushNode(n *node) {
	n.prev = ql.tail
	if ql.tail != nil {
		ql.tail.next = n
	} else {
		ql.head = n
	}
	ql.tail = n
}

func (ql *QuickList) LPop() (string, bool) {
	if ql.head == nil {
		return "", false
	}
	v, _ := ql.head.lp.PopFront()
	ql.length--
	if ql.head.lp.Len() == 0 {
		ql.unlink(ql.head)
	}
	return v, true
}

func (ql *QuickList) RPop() (string, bool) {
	if ql.tail == nil {
		return "", false
	}
	v, _ := ql.tail.lp.PopBack()
	ql.length--
	if ql.tail.lp.Len() == 0 {
		ql.unlink(ql.tail)
	}
	return v, true
}

func (ql *QuickList) unlink(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		ql.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		ql.tail = n.prev
	}
}

// All iterates the list front to back. Values alias the list and are only
// valid until it is modified.
func (ql *QuickList) All() iter.Seq[[]byte] {
	return ql.Range(0, ql.length-1)
}

// Range iterates indexes start..stop inclusive, which the caller must have
// clamped to [0, Len()). Whole nodes are skipped from the closer end, so the
// cost is O(nodes) + O(stop-start).
func (ql *QuickList) Range(start, stop int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if start > stop || ql.length == 0 {
			return
		}
		// find the node holding start, and its index within that node
		var n *node
		if start < ql.length/2 {
			for n = ql.head; start >= n.lp.Len(); n = n.next {
				start, stop = start-n.lp.Len(), stop-n.lp.Len()
			}
		} else {
			offset := ql.length
			for n = ql.tail; ; n = n.prev {
				offset -= n.lp.Len()
				if start >= offset {
					break
				}
			}
			start, stop = start-offset, stop-offset
		}

		remaining := stop - start + 1
		for ; n != nil; n = n.next {
			c := n.lp.Front()
			for v, ok := c.Next(); ok; v, ok = c.Next() {
				if start > 0 {
					start--
					continue
				}
				if !yield(v) {
					return
				}
				if remaining--; remaining == 0 {
					return
				}
			}
		}
	}
}

// Encode writes [length, nodes, listpack...].
func (ql *QuickList) Encode(w *codec.Writer) {
	nodes := 0
	for n := ql.head; n != nil; n = n.next {
		nodes++
	}
	w.WriteUvarint(uint64(ql.length))
	w.WriteUvarint(uint64(nodes))
	for n := ql.head; n != nil; n = n.next {
		n.lp.Encode(w)
	}
}

func (ql *QuickList) Decode(r *codec.Reader) {
	length := r.ReadUvarint()
	nodes := r.ReadLen()
	for range nodes {
		n := &node{}
		n.lp.Decode(r)
		if n.lp.Len() == 0 {
			panic(codec.ErrCorrupted)
		}
		ql.pushNode(n)
		ql.length += n.lp.Len()
	}
	if uint64(ql.length) != length {
		panic(codec.ErrCorrupted)
	}
}
