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
type List struct {
	mu sync.RWMutex
	ls *ulist.UList[string]
}

// NewList
func NewList() *List {
	return &List{
		ls: ulist.New[string](defaultEntryBlockSize),
	}
}

// LPush
func (l *List) LPush(items ...string) {
	l.mu.Lock()
	for i := len(items) - 1; i >= 0; i-- {
		l.ls.PushFront(items[i])
	}
	l.mu.Unlock()
}

// RPush
func (l *List) RPush(items ...string) {
	l.mu.Lock()
	for _, item := range items {
		l.ls.PushBack(item)
	}
	l.mu.Unlock()
}

// Index
func (l *List) Index(i int) (v string, ok bool) {
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
func (l *List) LPop() (v string, ok bool) {
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
func (l *List) RPop() (v string, ok bool) {
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
func (l *List) Size() int {
	l.mu.RLock()
	n := l.ls.Size()
	l.mu.RUnlock()
	return n
}

// Keys
func (l *List) Keys() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	arr := make([]string, 0, l.ls.Size())
	for n := l.ls.Begin(); n.IsValid(); n.Next() {
		arr = append(arr, n.Get())
	}
	return arr
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
		l.ls.PushBack(item)
	}

	return nil
}
