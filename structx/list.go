package structx

// List
type List[T any] struct {
	len     int
	rb      int
	buckets []*LBucket[T]
}

type LBucket[T any] struct {
	data []T
}

const (
	bucketLength = 128
)

// isFull
func (b *LBucket[T]) isFull() bool {
	return len(b.data) == bucketLength
}

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
	b := l.buckets[l.rb]
	if b.isFull() {
		l.rb++
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
	b := l.buckets[0]
	if b.isFull() {
		b = &LBucket[T]{
			data: make([]T, 0, bucketLength),
		}
		l.buckets = append([]*LBucket[T]{b}, l.buckets...)
		l.rb++

		b.data = append(b.data, elem)

	} else {
		b.data = append([]T{elem}, b.data...)
	}

	l.len++
}

// RPop
func (l *List[T]) RPop() (T, bool) {
	var v T
	return v, true
}

// LPop
func (l *List[T]) LPop() (T, bool) {
	var v T
	return v, true
}

// Range
func (l *List[T]) Range(f func(elem T) bool) {
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
	return l.len
}

// MarshalJSON
func (l *List[T]) MarshalJSON() ([]byte, error) {
	return nil, nil
}

// UnmarshalJSON
func (l *List[T]) UnmarshalJSON(b []byte) error {
	return nil
}
