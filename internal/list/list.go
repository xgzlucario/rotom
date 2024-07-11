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
	head, tail *Node
}

type Node struct {
	*ListPack
	prev, next *Node
}

func SetMaxListPackSize(s int) {
	maxListPackSize = s
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
	ls.tail.RPush(key)
}

// LPop
func (ls *QuickList) LPop() (key string, ok bool) {
	for lp := ls.head; lp != nil; lp = lp.next {
		if lp.size > 0 {
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

func (ls *QuickList) Size() (n int) {
	for lp := ls.head; lp != nil; lp = lp.next {
		n += lp.Size()
	}
	return
}

type lsIterator func(data []byte)

func (ls *QuickList) Range(start, end int, f lsIterator) {
	if end == -1 {
		end = math.MaxInt
	}
	for lp := ls.head; lp != nil; lp = lp.next {
		it := lp.NewIterator().SeekBegin()
		for !it.IsEnd() {
			f(it.Next())
		}
	}
}

func (ls *QuickList) RevRange(start, end int, f lsIterator) {
	if end == -1 {
		end = math.MaxInt
	}
	for lp := ls.tail; lp != nil; lp = lp.prev {
		it := lp.NewIterator().SeekEnd()
		for !it.IsBegin() {
			f(it.Prev())
		}
	}
}
