package list

import (
	"github.com/bytedance/sonic"
	"io"
)

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

func (ls *QuickList) Size() int { return ls.size }

func (ls *QuickList) RangeCount(start, stop int) int {
	if start < 0 {
		start += ls.Size()
	}
	if stop < 0 {
		stop += ls.Size()
	}
	start = max(0, start)
	stop = min(ls.Size(), stop)

	if start <= stop {
		return min(ls.Size(), stop-start+1)
	}
	return 0
}

func (ls *QuickList) Range(start, stop int, fn func(data []byte)) {
	count := ls.RangeCount(start, stop)
	if count == 0 {
		return
	}
	if start < 0 {
		start += ls.Size()
	}

	lp := ls.head
	for lp != nil && start > lp.Size() {
		start -= lp.Size()
		lp = lp.next
	}
	if lp == nil {
		return
	}
	it := lp.Iterator()
	for range start {
		it.Next()
	}

	for range count {
		if it.IsLast() {
			lp = lp.next
			it = lp.Iterator()
		}
		fn(it.Next())
	}
}

type ListPackData struct {
	Data []byte
	Size uint32
}

func (ls *QuickList) Encode(writer io.Writer) error {
	var data []ListPackData
	for p := ls.head; p != nil; p = p.next {
		data = append(data, ListPackData{Data: p.data, Size: p.size})
	}
	return sonic.ConfigDefault.NewEncoder(writer).Encode(data)
}

func (ls *QuickList) Decode(src []byte) error {
	var datas []ListPackData
	if err := sonic.Unmarshal(src, &datas); err != nil {
		return err
	}

	// head node
	n := newNode()
	n.size = datas[0].Size
	n.data = datas[0].Data
	ls.head = n
	ls.tail = n
	cur := n

	for _, data := range datas[1:] {
		n := newNode()
		n.size = data.Size
		n.data = data.Data
		cur.next = n
		n.prev = cur
		ls.tail = n
		cur = n
	}
	return nil
}
