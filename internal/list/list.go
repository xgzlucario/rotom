package list

import "math"

//	 +------------------------------ QuickList -----------------------------+
//	 |	     +-----------+     +-----------+             +-----------+      |
//	head --- | listpack0 | <-> | listpack1 | <-> ... <-> | listpackN | --- tail
//	         +-----------+     +-----------+             +-----------+
//
// QuickList is double linked listpack, implement redis quicklist data structure,
// based on listpack rather than ziplist to optimize cascade update.
type QuickList struct {
	size       int
	head, tail *Node
}

type Node struct {
	*ListPack
	prev, next *Node
}

// New create a quicklist instance.
func New() *QuickList {
	n := newNode()
	return &QuickList{head: n, tail: n}
}

func newNode() *Node {
	return &Node{ListPack: NewListPack()}
}

// LPush
func (ls *QuickList) LPush(key string) {
	if len(ls.head.data)+len(key) >= maxListPackSize {
		n := newNode()
		n.next = ls.head
		ls.head.prev = n
		ls.head = n
	}
	ls.size++
	ls.head.LPush(key)
}

// RPush
func (ls *QuickList) RPush(key string) {
	if len(ls.tail.data)+len(key) >= maxListPackSize {
		n := newNode()
		ls.tail.next = n
		n.prev = ls.tail
		ls.tail = n
	}
	ls.size++
	ls.tail.RPush(key)
}

// LPop
func (ls *QuickList) LPop() (key string, ok bool) {
	for lp := ls.head; lp != nil; lp = lp.next {
		if lp.size > 0 {
			ls.size--
			return lp.LPop()
		}
		ls.free(lp)
	}
	return
}

// RPop
func (ls *QuickList) RPop() (key string, ok bool) {
	for lp := ls.tail; lp != nil; lp = lp.prev {
		if lp.size > 0 {
			ls.size--
			return lp.RPop()
		}
		ls.free(lp)
	}
	return
}

// free release empty list node.
func (ls *QuickList) free(n *Node) {
	if n.prev != nil && n.next != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		bpool.Put(n.data)
		n = nil
	}
}

func (ls *QuickList) Size() int {
	return ls.size
}

func (ls *QuickList) Range(start, end int, f func(data []byte)) {
	if end == -1 {
		end = math.MaxInt
	}
	count := end - start

	lp := ls.head
	for lp != nil && start > lp.Size() {
		start -= lp.Size()
		lp = lp.next
	}

	it := lp.Iterator().SeekFirst()
	for range start {
		it.Next()
	}

	for range count {
		if it.IsLast() {
			if lp.next == nil {
				return
			}
			lp = lp.next
			it = lp.Iterator().SeekFirst()
		}
		f(it.Next())
	}
}

func (ls *QuickList) RevRange(start, end int, f func(data []byte)) {
	if end == -1 {
		end = math.MaxInt
	}
	count := end - start

	lp := ls.tail
	for lp != nil && start > lp.Size() {
		start -= lp.Size()
		lp = lp.prev
	}

	it := lp.Iterator().SeekLast()
	for range start {
		it.Prev()
	}

	for range count {
		if it.IsFirst() {
			if lp.prev == nil {
				return
			}
			lp = lp.prev
			it = lp.Iterator().SeekLast()
		}
		f(it.Prev())
	}
}
