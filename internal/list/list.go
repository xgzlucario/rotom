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
	if n.size == 0 && n.prev != nil && n.next != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		bpool.Put(n.data)
		n = nil
	}
}

// Size
func (ls *QuickList) Size() (n int) {
	for lp := ls.head; lp != nil; lp = lp.next {
		n += lp.Size()
	}
	return
}

// Index
func (ls *QuickList) Index(index int) (string, bool) {
	for lp := ls.head; lp != nil; lp = lp.next {
		if index > lp.Size() {
			index -= lp.Size()
		} else {
			it := lp.NewIterator()
			var data []byte
			for index >= 0 {
				data = it.Next()
				index--
			}
			return string(data), true
		}
	}
	return "", false
}

type lsIterator func(data []byte) (stop bool)

// Range
func (ls *QuickList) Range(start, end int, f lsIterator) {
}

// RevRange
func (ls *QuickList) RevRange(start, end int, f lsIterator) {
}
