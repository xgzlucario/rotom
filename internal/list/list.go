package list

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
	// ls.head.Insert(0, key)
}

// RPush
func (ls *QuickList) RPush(key string) {
	if len(ls.tail.data)+len(key) >= maxListPackSize {
		n := newNode()
		ls.tail.next = n
		n.prev = ls.tail
		ls.tail = n
	}
	// ls.tail.Insert(-1, key)
}

// Index
func (ls *QuickList) Index(i int) (val string, ok bool) {
	ls.Range(i, i+1, func(key []byte) bool {
		val, ok = string(key), true
		return true
	})
	return
}

// LPop
func (ls *QuickList) LPop() (string, bool) {
	return ls.Remove(0)
}

// RPop
func (ls *QuickList) RPop() (key string, ok bool) {
	for lp := ls.tail; lp != nil; lp = lp.prev {
		if lp.size > 0 {
			// return lp.Remove(-1)
		}
		ls.free(lp)
	}
	return
}

// free release empty list node.
func (ls *QuickList) free(n *Node) {
	if n.size == 0 && n.prev != nil && n.next != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		bpool.Put(n.data)
		n = nil
	}
}

// find quickly locates `listpack` and it `indexInternal` based on index.
func (ls *QuickList) find(index int) (*Node, int) {
	var n *Node
	for n = ls.head; n != nil && index >= n.Size(); n = n.next {
		index -= n.Size()
	}
	return n, index
}

// Set
func (ls *QuickList) Set(index int, key string) bool {
	// lp, indexInternal := ls.find(index)
	// if lp != nil {
	// 	return lp.Set(indexInternal, key)
	// }
	return false
}

// Remove
func (ls *QuickList) Remove(index int) (val string, ok bool) {
	// lp, indexInternal := ls.find(index)
	// if lp != nil {
	// 	val, ok = lp.Remove(indexInternal)
	// 	ls.free(lp)
	// }
	return
}

// RemoveFirst
func (ls *QuickList) RemoveFirst(key string) (res int, ok bool) {
	// for lp := ls.head; lp != nil; lp = lp.next {
	// 	if lp.size == 0 {
	// 		ls.free(lp)

	// 	} else {
	// 		n, ok := lp.RemoveFirst(key)
	// 		if ok {
	// 			return res + n, true
	// 		} else {
	// 			res += lp.Size()
	// 		}
	// 	}
	// }
	return 0, false
}

// Size
func (ls *QuickList) Size() (n int) {
	for lp := ls.head; lp != nil; lp = lp.next {
		n += lp.Size()
	}
	return
}

type lsIterator func(data []byte) (stop bool)

func (ls *QuickList) iterFront(start, end int, f lsIterator) {
	// count := end - start
	// if end == -1 {
	// 	count = math.MaxInt
	// }
	// if start < 0 || count < 0 {
	// 	return
	// }

	// lp, indexInternal := ls.find(start)

	// var stop bool
	// for !stop && count > 0 && lp != nil {
	// 	lp.Range(indexInternal, -1, func(data []byte, _ int) bool {
	// 		stop = f(data)
	// 		count--
	// 		return stop || count == 0
	// 	})
	// 	lp = lp.next
	// 	indexInternal = 0
	// }
}

func (ls *QuickList) iterBack(start, end int, f lsIterator) {
	// count := end - start
	// if end == -1 {
	// 	count = math.MaxInt
	// }
	// if start < 0 || count < 0 {
	// 	return
	// }

	// lp := ls.tail
	// for start > lp.Size() {
	// 	start -= lp.Size()
	// 	lp = lp.prev
	// 	if lp == nil {
	// 		return
	// 	}
	// }

	// var stop bool
	// for !stop && count > 0 && lp != nil {
	// 	lp.RevRange(start, -1, func(data []byte, _ int) bool {
	// 		stop = f(data)
	// 		count--
	// 		return stop || count == 0
	// 	})
	// 	lp = lp.prev
	// 	start = 0
	// }
}

// Range
func (ls *QuickList) Range(start, end int, f lsIterator) {
	ls.iterFront(start, end, f)
}

// RevRange
func (ls *QuickList) RevRange(start, end int, f lsIterator) {
	ls.iterBack(start, end, f)
}
