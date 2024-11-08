package list

import (
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
	"github.com/zyedidia/generic/list"
)

var (
	_ iface.Encoder = (*ListPack)(nil)
	_ iface.Encoder = (*QuickList)(nil)
)

//	 +------------------------------ QuickList -----------------------------+
//	 |	     +-----------+     +-----------+             +-----------+      |
//	head --- | listpack0 | <-> | listpack1 | <-> ... <-> | listpackN | --- tail
//	         +-----------+     +-----------+             +-----------+
//
// QuickList is double linked listpack, implement redis quicklist data structure,
// based on listpack rather than ziplist to optimize cascade update.
type QuickList struct {
	size int
	ls   *list.List[*ListPack]
}

// New create a quicklist instance.
func New() *QuickList {
	return &QuickList{
		ls: list.New[*ListPack](),
	}
}

func (ls *QuickList) head() *ListPack {
	if ls.ls.Front == nil {
		ls.ls.PushFront(NewListPack())
	}
	return ls.ls.Front.Value
}

func (ls *QuickList) tail() *ListPack {
	if ls.ls.Back == nil {
		ls.ls.PushBack(NewListPack())
	}
	return ls.ls.Back.Value
}

func (ls *QuickList) LPush(keys ...string) {
	if len(ls.head().data) >= maxListPackSize {
		ls.ls.PushFront(NewListPack())
	}
	ls.head().LPush(keys...)
	ls.size += len(keys)
}

func (ls *QuickList) RPush(keys ...string) {
	if len(ls.tail().data) >= maxListPackSize {
		ls.ls.PushBack(NewListPack())
	}
	ls.tail().RPush(keys...)
	ls.size += len(keys)
}

func (ls *QuickList) LPop() (key string, ok bool) {
	if ls.Size() == 0 {
		return
	}
	for n := ls.ls.Front; n != nil && n.Value.size == 0; n = n.Next {
		ls.ls.Remove(n)
	}
	ls.size--
	return ls.head().LPop()
}

func (ls *QuickList) RPop() (key string, ok bool) {
	if ls.Size() == 0 {
		return
	}
	for n := ls.ls.Back; n != nil && n.Value.size == 0; n = n.Prev {
		ls.ls.Remove(n)
	}
	ls.size--
	return ls.tail().RPop()
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

func (ls *QuickList) Range(start int, fn func(key []byte) (stop bool)) {
	if start < 0 {
		start += ls.Size()
	}

	lp := ls.ls.Front
	for lp != nil && start > lp.Value.Size() {
		start -= lp.Value.Size()
		lp = lp.Next
	}
	if lp == nil {
		return
	}

	it := lp.Value.Iterator()
	for range start {
		it.Next()
	}
	for {
		if it.IsLast() {
			lp = lp.Next
			if lp == nil || lp.Value.Size() == 0 {
				return
			}
			it = lp.Value.Iterator()
		}
		if fn(it.Next()) {
			return
		}
	}
}

func (ls *QuickList) Encode(writer *resp.Writer) error {
	num := 0
	for n := ls.ls.Front; n != nil; n = n.Next {
		num++
	}
	writer.WriteArrayHead(num)
	for n := ls.ls.Front; n != nil; n = n.Next {
		if err := n.Value.Encode(writer); err != nil {
			return err
		}
	}
	return nil
}

func (ls *QuickList) Decode(reader *resp.Reader) error {
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	for range n {
		lp := NewListPack()
		if err = lp.Decode(reader); err != nil {
			return err
		}
		ls.ls.PushBack(lp)
		ls.size += lp.Size()
	}
	return nil
}
