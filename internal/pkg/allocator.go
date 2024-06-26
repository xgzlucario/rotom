package pkg

import (
	"sync"

	"github.com/cockroachdb/swiss"
)

// Allocator is a group pool for swissmap.
type Allocator[K comparable, V any] struct {
	pool      *sync.Pool
	miss, hit uint64
}

func NewAllocator[K comparable, V any]() *Allocator[K, V] {
	return &Allocator[K, V]{
		pool: &sync.Pool{
			New: func() interface{} { return new([]swiss.Group[K, V]) },
		},
	}
}

func (p *Allocator[K, V]) Alloc(want int) []swiss.Group[K, V] {
	buf := p.pool.Get().(*[]swiss.Group[K, V])

	if cap(*buf) < want {
		*buf = make([]swiss.Group[K, V], want)
		p.miss++

	} else {
		*buf = (*buf)[:want]
		p.hit++
	}

	return *buf
}

func (p *Allocator[K, V]) Free(b []swiss.Group[K, V]) {
	p.pool.Put(&b)
}

func (p *Allocator[K, V]) Miss() uint64 {
	return p.miss
}

func (p *Allocator[K, V]) Hit() uint64 {
	return p.hit
}
