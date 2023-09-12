package structx

import "sync"

// List
type List[T any] struct {
	sync.RWMutex
	len     int
	tail    int
	buckets []*LBucket[T]
}

type LBucket[T any] struct {
	data []T
}

const (
	bucketLength = 128
)

// NewList
func NewList[T any]() *List[T] {
	b := &LBucket[T]{
		data: make([]T, 0, bucketLength),
	}
	return &List[T]{
		buckets: []*LBucket[T]{b},
	}
}

// RPush
func (l *List[T]) RPush(elem T) {
	l.Lock()
	defer l.Unlock()
	b := l.buckets[l.tail]

	if b.isFull() {
		l.tail++
		b = &LBucket[T]{
			data: make([]T, 0, bucketLength),
		}
		l.buckets = append(l.buckets, b)
	}

	b.data = append(b.data, elem)
	l.len++
}

// LPush
func (l *List[T]) LPush(elem T) {
	l.Lock()
	defer l.Unlock()
	b := l.buckets[0]

	if b.isFull() {
		b = &LBucket[T]{
			data: make([]T, 0, bucketLength),
		}
		l.buckets = append([]*LBucket[T]{b}, l.buckets...)
		l.tail++

		b.data = append(b.data, elem)

	} else {
		b.data = append([]T{elem}, b.data...)
	}

	l.len++
}

// RPop
func (l *List[T]) RPop() (val T, ok bool) {
	l.Lock()
	defer l.Unlock()
	if l.len == 0 {
		return
	}

L:
	b := l.buckets[l.tail]

	if !b.isEmpty() {
		val = b.data[len(b.data)-1]
		b.data = b.data[:len(b.data)-1]
		l.len--

		return val, true

	} else {
		// delete bucket
		clear(b.data)
		l.buckets = l.buckets[:l.tail]
		l.tail--
		goto L
	}
}

// LPop
func (l *List[T]) LPop() (val T, ok bool) {
	l.Lock()
	defer l.Unlock()
	if l.len == 0 {
		return
	}

L:
	b := l.buckets[0]

	if !b.isEmpty() {
		val = b.data[0]
		b.data = b.data[1:]
		l.len--

		return val, true

	} else {
		// delete bucket
		clear(b.data)
		l.buckets = l.buckets[1:]
		l.tail--
		goto L
	}
}

// Index
func (l *List[T]) Index(i int) (val T, ok bool) {
	l.RLock()
	defer l.RUnlock()

	if i < 0 || i >= l.len {
		return
	}

	var sum int
	for _, b := range l.buckets {
		if sum+len(b.data) > i {
			return b.data[i-sum], true
		}
		sum += len(b.data)
	}

	return
}

// Range
func (l *List[T]) Range(f func(elem T) bool) {
	l.RLock()
	defer l.RUnlock()

	for _, b := range l.buckets {
		for _, v := range b.data {
			if !f(v) {
				return
			}
		}
	}
}

// Len
func (l *List[T]) Len() int {
	l.RLock()
	defer l.RUnlock()

	return l.len
}

// MarshalJSON
func (l *List[T]) MarshalJSON() ([]byte, error) {
	l.RLock()
	defer l.RUnlock()

	return nil, nil
}

// UnmarshalJSON
func (l *List[T]) UnmarshalJSON(b []byte) error {
	l.Lock()
	defer l.Unlock()

	return nil
}

// isFull
func (b *LBucket[T]) isFull() bool {
	return len(b.data) == bucketLength
}

// isEmpty
func (b *LBucket[T]) isEmpty() bool {
	return len(b.data) == 0
}
