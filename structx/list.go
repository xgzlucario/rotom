package structx

import (
	"encoding/binary"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	eachNodeMaxSize = 4 * 1024

	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	decoder, _ = zstd.NewReader(nil)
)

// List is a double linked ziplist.
type List struct {
	mu         sync.RWMutex
	head, tail *lnode
}

type lnode struct {
	data       []byte
	n          int
	prev, next *lnode
}

func SetEachNodeMaxSize(s int) {
	eachNodeMaxSize = s
}

// NewList
func NewList() *List {
	blk := newNode()
	return &List{head: blk, tail: blk}
}

func newNode() *lnode {
	return &lnode{data: make([]byte, 0, eachNodeMaxSize)}
}

func (b *lnode) lpush(key string) (full bool) {
	alloc := append(
		binary.AppendUvarint(nil, uint64(len(key))),
		key...,
	)
	b.data = slices.Insert(b.data, 0, alloc...)
	b.n++
	return len(b.data) >= eachNodeMaxSize
}

func (b *lnode) rpush(key string) (full bool) {
	b.data = binary.AppendUvarint(b.data, uint64(len(key)))
	b.data = append(b.data, key...)
	b.n++
	return len(b.data) >= eachNodeMaxSize
}

func (b *lnode) iter(start int, f func(start, end int, key string) (stop bool)) {
	var index int
	for i := 0; index < len(b.data); i++ {
		// klen
		klen, n := binary.Uvarint(b.data[index:])
		index += n
		if i >= start {
			// key
			key := b.data[index : index+int(klen)]
			if f(index-n, index+int(klen), string(key)) {
				return
			}
		}
		index += int(klen)
	}
}

func (l *List) lpush(key string) {
	if l.head.lpush(key) {
		node := newNode()
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
}

// LPush
func (l *List) LPush(keys ...string) {
	l.mu.Lock()
	for _, k := range keys {
		l.lpush(k)
	}
	l.mu.Unlock()
}

func (l *List) rpush(key string) {
	if l.tail.rpush(key) {
		node := newNode()
		l.tail.next = node
		node.prev = l.tail
		l.tail = node
	}
}

// RPush
func (l *List) RPush(keys ...string) {
	l.mu.Lock()
	for _, k := range keys {
		l.rpush(k)
	}
	l.mu.Unlock()
}

// Index
func (l *List) Index(i int) (v string, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var cur *lnode
	for cur = l.head; cur != nil && i >= cur.n; cur = cur.next {
		i -= cur.n
	}
	if cur == nil {
		return
	}
	cur.iter(i, func(_, _ int, s string) (stop bool) {
		v = s
		ok = true
		return true
	})
	return
}

// LPop
func (l *List) LPop() (key string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// remove empty head node
	for l.head.n == 0 {
		if l.head.next == nil {
			return
		}
		l.head = l.head.next
		l.head.prev = nil
	}

	cur := l.head
	cur.iter(0, func(_, end int, s string) (stop bool) {
		key = s
		cur.data = cur.data[end:]
		cur.n--
		return true
	})
	return key, true
}

// RPop
func (l *List) RPop() (key string, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// remove empty tail node
	for l.tail.n == 0 {
		if l.tail.prev == nil {
			return "", false
		}
		l.tail = l.tail.prev
		l.tail.next = nil
	}

	cur := l.tail
	cur.iter(cur.n-1, func(start, _ int, s string) (stop bool) {
		key = s
		cur.data = cur.data[:start]
		cur.n--
		return true
	})
	return key, true
}

// Size
func (l *List) Size() (n int) {
	l.mu.RLock()
	for cur := l.head; cur != nil; cur = cur.next {
		n += cur.n
	}
	l.mu.RUnlock()
	return
}

// Keys
func (l *List) Keys() (keys []string) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for cur := l.head; cur != nil; cur = cur.next {
		cur.iter(0, func(start, end int, key string) (stop bool) {
			keys = append(keys, key)
			return false
		})
	}
	return
}

// Marshal
func (l *List) Marshal() []byte {
	buf := make([]byte, 0, 1024)
	l.mu.RLock()
	for cur := l.head; cur != nil; cur = cur.next {
		buf = append(buf, cur.data...)
	}
	l.mu.RUnlock()

	// compress
	cbuf := make([]byte, 0, len(buf)/2)
	return encoder.EncodeAll(buf, cbuf)
}

// Unmarshal
func (l *List) Unmarshal(src []byte) error {
	data, err := decoder.DecodeAll(src, nil)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l = NewList()

	var index int
	for index < len(data) {
		// klen
		klen, n := binary.Uvarint(data[index:])
		index += n
		// key
		key := data[index : index+int(klen)]
		l.rpush(string(key))
		index += int(klen)
	}
	return nil
}
