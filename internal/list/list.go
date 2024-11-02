package list

import (
	"github.com/bytedance/sonic"
	"github.com/zyedidia/generic/list"
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
	ls   *list.List[*ListPack] // linked-list
}

// New create a quicklist instance.
func New() *QuickList {
	ls := list.New[*ListPack]()
	ls.PushFront(NewListPack())
	return &QuickList{ls: ls}
}

func (ls *QuickList) head() *ListPack {
	return ls.ls.Front.Value
}

func (ls *QuickList) tail() *ListPack {
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

func (ls *QuickList) Range(start, stop int, fn func(data []byte)) {
	count := ls.RangeCount(start, stop)
	if count == 0 {
		return
	}
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
	for range count {
		if it.IsLast() {
			lp = lp.Next
			it = lp.Value.Iterator()
		}
		fn(it.Next())
	}
}

type ListPackData struct {
	Data []byte
	Size uint32
}

func (ls *QuickList) Encode() ([]byte, error) {
	var data []ListPackData
	for n := ls.ls.Front; n != nil; n = n.Next {
		data = append(data, ListPackData{
			Data: n.Value.data,
			Size: n.Value.size,
		})
	}
	return sonic.Marshal(data)
}

func (ls *QuickList) Decode(src []byte) error {
	var datas []ListPackData
	if err := sonic.Unmarshal(src, &datas); err != nil {
		return err
	}
	// init
	ls.size = 0
	ls.ls = list.New[*ListPack]()

	for _, data := range datas {
		n := NewListPack()
		n.size = data.Size
		n.data = data.Data

		ls.size += int(data.Size)
		ls.ls.PushBack(n)
	}
	return nil
}
