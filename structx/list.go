package structx

import (
	"encoding/binary"
	"math"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
	cache "github.com/xgzlucario/GigaCache"
)

var (
	eachNodeMaxSize = 4 * 1024

	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	decoder, _ = zstd.NewReader(nil)

	bpool = cache.NewBufferPool()
)

// List is double linked ziplist.
/*
    +--- HEAD                               TAIL ---+
    |                                               |
    o------+-----+------+-----+-----+               o------+-----+------+-----+-----+
    | klen | key | klen | key | ... | <--> ... <--> | klen | key | klen | key | ... |
    +------+-----+------+-----+-----+               +------+-----+------+-----+-----+
	|<---         lnode         --->|               |<---         lnode         --->|
*/
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
	return &lnode{data: bpool.Get(eachNodeMaxSize)[:0]}
}

func (b *lnode) lpush(key string) {
	alloc := bpool.Get(len(key) + 5)[:0]
	alloc = append(
		binary.AppendUvarint(alloc, uint64(len(key))),
		key...,
	)
	b.data = slices.Insert(b.data, 0, alloc...)
	b.n++
	bpool.Put(alloc)
}

func (b *lnode) rpush(key string) {
	b.data = append(
		binary.AppendUvarint(b.data, uint64(len(key))),
		key...,
	)
	b.n++
}

func (b *lnode) iter(start int, f iterator) {
	var index int
	for i := 0; i < b.n; i++ {
		// klen
		klen, n := binary.Uvarint(b.data[index:])
		index += n
		if i >= start {
			// key
			key := b.data[index : index+int(klen)]
			if f(b, index-n, index+int(klen), key) {
				return
			}
		}
		index += int(klen)
	}
}

func (l *List) lpush(key string) {
	if len(l.head.data)+len(key) >= eachNodeMaxSize {
		node := newNode()
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	l.head.lpush(key)
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
	if len(l.tail.data)+len(key) >= eachNodeMaxSize {
		node := newNode()
		l.tail.next = node
		node.prev = l.tail
		l.tail = node
	}
	l.tail.rpush(key)
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
func (l *List) Index(i int) (val string, ok bool) {
	l.Range(i, i+1, func(key string) bool {
		val = key
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
		bpool.Put(l.head.data)
		l.head = l.head.next
		l.head.prev = nil
	}

	l.head.iter(0, func(cur *lnode, _, end int, bkey []byte) bool {
		key = string(bkey)
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
			return
		}
		bpool.Put(l.tail.data)
		l.tail = l.tail.prev
		l.tail.next = nil
	}

	l.tail.iter(l.tail.n-1, func(cur *lnode, start, _ int, bkey []byte) bool {
		key = string(bkey)
		cur.data = cur.data[:start]
		cur.n--
		return true
	})
	return key, true
}

// Delete
func (l *List) Delete(index int) (key string, ok bool) {
	l.mu.Lock()
	l.iter(index, index+1, func(node *lnode, dataStart, dataEnd int, bkey []byte) bool {
		key = string(bkey)
		node.data = slices.Delete(node.data, dataStart, dataEnd)
		node.n--
		ok = true
		return true
	})
	l.mu.Unlock()
	return
}

// Set
func (l *List) Set(index int, key string) (ok bool) {
	l.mu.Lock()
	l.iter(index, index+1, func(node *lnode, dataStart, dataEnd int, _ []byte) bool {
		alloc := bpool.Get(len(key) + 5)[:0]
		alloc = append(
			binary.AppendUvarint(alloc, uint64(len(key))),
			key...,
		)
		node.data = slices.Replace(node.data, dataStart, dataEnd, alloc...)
		bpool.Put(alloc)
		ok = true
		return true
	})
	l.mu.Unlock()
	return
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

// iterator iter each keys in list by dataStart, dataEnd, and raw key.
type iterator func(node *lnode, dataStart, dataEnd int, key []byte) (stop bool)

// iter
func (l *List) iter(start, end int, f iterator) {
	// param check
	count := end - start
	if end == -1 {
		count = math.MaxInt
	}
	if start < 0 || count < 0 {
		return
	}

	cur := l.head
	// skip nodes
	for start > cur.n {
		start -= cur.n
		cur = cur.next
		if cur == nil {
			return
		}
	}

	var stop bool
	for !stop && count > 0 && cur != nil {
		cur.iter(start, func(node *lnode, dataStart, dataEnd int, key []byte) bool {
			stop = f(node, dataStart, dataEnd, key)
			count--
			return stop || count == 0
		})
		cur = cur.next
		start = 0
	}
}

// Range
func (l *List) Range(start, end int, f func(string) (stop bool)) {
	l.mu.RLock()
	l.iter(start, end, func(_ *lnode, _, _ int, bkey []byte) bool {
		return f(string(bkey))
	})
	l.mu.RUnlock()
}

// Keys
func (l *List) Keys() (keys []string) {
	l.Range(0, -1, func(key string) bool {
		keys = append(keys, key)
		return false
	})
	return
}

// Marshal
func (l *List) Marshal() []byte {
	buf := bpool.Get(eachNodeMaxSize)[:0]
	l.mu.RLock()
	for cur := l.head; cur != nil; cur = cur.next {
		buf = append(buf, cur.data...)
	}
	l.mu.RUnlock()

	// compress
	cbuf := bpool.Get(len(buf) / 3)[:0]
	cbuf = encoder.EncodeAll(buf, cbuf)
	bpool.Put(buf)
	return cbuf
}

// Unmarshal requires an initialized List.
func (l *List) Unmarshal(src []byte) error {
	data, err := decoder.DecodeAll(src, nil)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

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
