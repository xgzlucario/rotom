package structx

import (
	"sync"

	"github.com/bytedance/sonic"
	"github.com/zyedidia/generic/ulist"
)

const (
	defaultEntryBlockSize = 64
)

// List implements a doubly-linked list.
type List[V any] struct {
	mu sync.RWMutex
	ls *ulist.UList[V]
}

// NewList
func NewList[V any]() *List[V] {
	return &List[V]{
		ls: ulist.New[V](defaultEntryBlockSize),
	}
}

// LPush
func (l *List[V]) LPush(items ...V) {
	l.mu.Lock()
	for i := len(items) - 1; i >= 0; i-- {
		l.ls.PushFront(items[i])
	}
	l.mu.Unlock()
}

// RPush
func (l *List[V]) RPush(items ...V) {
	l.mu.Lock()
	for _, item := range items {
		l.ls.PushBack(item)
	}
	l.mu.Unlock()
}

// Index
func (l *List[V]) Index(i int) (v V, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for n := l.ls.Begin(); n.IsValid(); n.Next() {
		if i == 0 {
			return n.Get(), true
		}
		i--
	}
	return
}

// LPop
func (l *List[V]) LPop() (v V, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := l.ls.Begin()
	if n.IsValid() {
		v = n.Get()
		l.ls.Remove(n)
		return v, true
	}
	return
}

// RPop
func (l *List[V]) RPop() (v V, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := l.ls.End()
	if n.IsValid() {
		v = n.Get()
		l.ls.Remove(n)
		return v, true
	}
	return
}

// Size
func (l *List[V]) Size() int {
	l.mu.RLock()
	n := l.ls.Size()
	l.mu.RUnlock()
	return n
}

// MarshalJSON
func (l *List[V]) MarshalJSON() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	arr := make([]V, 0, l.ls.Size())
	for n := l.ls.Begin(); n.IsValid(); n.Next() {
		arr = append(arr, n.Get())
	}

	return sonic.Marshal(arr)
}

// UnmarshalJSON
func (l *List[V]) UnmarshalJSON(src []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var items []V
	if err := sonic.Unmarshal(src, &items); err != nil {
		return err
	}

	l = NewList[V]()
	for _, item := range items {
		l.ls.PushBack(item)
	}

	return nil
}
