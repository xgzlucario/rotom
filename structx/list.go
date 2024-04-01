package structx

import (
	"encoding/binary"
	"sync"

	"github.com/bytedance/sonic"
)

const (
	eachBlkMaxSize = 4 * 1024
)

type listBlk struct {
	data       []byte
	n          int
	prev, next *listBlk
}

func newBlk() *listBlk {
	return &listBlk{
		data: make([]byte, 0, eachBlkMaxSize),
	}
}

func (b *listBlk) lpush(item string) (full bool) {
	nb := make([]byte, 0, cap(b.data))
	nb = binary.AppendUvarint(nb, uint64(len(item)))
	nb = append(nb, item...)
	nb = append(nb, b.data...)
	b.data = nb
	b.n++
	return len(b.data) >= eachBlkMaxSize
}

func (b *listBlk) rpush(item string) (full bool) {
	b.data = binary.AppendUvarint(b.data, uint64(len(item)))
	b.data = append(b.data, item...)
	b.n++
	return len(b.data) >= eachBlkMaxSize
}

func (b *listBlk) iter(start, end int, f func(string) (stop bool)) {
	var index int
	for i := 0; index < len(b.data) && i < end; i++ {
		// klen
		klen, n := binary.Uvarint(b.data[index:])
		index += n

		if i >= start {
			// key
			key := b.data[index+index+int(klen)]
			if f(string(key)) {
				return
			}
		}
		index += int(klen)
	}
}

// List is a double linked bytes list.
type List struct {
	mu         sync.RWMutex
	head, tail *listBlk
}

// NewList
func NewList() *List {
	blk := newBlk()
	return &List{head: blk, tail: blk}
}

// LPush
func (l *List) LPush(items ...string) {
	l.mu.Lock()
	for i := len(items) - 1; i >= 0; i-- {
		if l.head.lpush(items[i]) {
			node := newBlk()
			node.next = l.head
			l.head.prev = node
			l.head = node
		}
	}
	l.mu.Unlock()
}

// RPush
func (l *List) RPush(items ...string) {
	l.mu.Lock()
	for _, item := range items {
		if l.tail.rpush(item) {
			node := newBlk()
			l.tail.next = node
			node.prev = l.tail
			l.tail = node
		}
	}
	l.mu.Unlock()
}

// Index
func (l *List) Index(i int) (v string, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for cur := l.head; i > cur.n; cur = cur.next {
		i -= cur.n
	}

	return
}

// LPop
func (l *List) LPop() (v string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// n := l.ls.Begin()
	// if n.IsValid() {
	// 	v = n.Get()
	// 	l.ls.Remove(n)
	// 	return v, true
	// }
	return
}

// RPop
func (l *List) RPop() (v string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// n := l.ls.End()
	// if n.IsValid() {
	// 	v = n.Get()
	// 	l.ls.Remove(n)
	// 	return v, true
	// }
	return
}

// Size
func (l *List) Size() int {
	l.mu.RLock()
	n := l.head.n
	l.mu.RUnlock()
	return n
}

// Keys
func (l *List) Keys() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return nil
}

// MarshalJSON
func (l *List) MarshalJSON() ([]byte, error) {
	return sonic.Marshal(l.Keys())
}

// UnmarshalJSON
func (l *List) UnmarshalJSON(src []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var items []string
	if err := sonic.Unmarshal(src, &items); err != nil {
		return err
	}

	l = NewList()
	for _, item := range items {
		l.head.rpush(item)
	}

	return nil
}
