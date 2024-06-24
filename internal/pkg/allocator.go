package pkg

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/swiss"
)

type Allocator[K comparable, V any] struct {
	pool      *sync.Pool
	miss, hit atomic.Uint64
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
		p.miss.Add(1)

	} else {
		*buf = (*buf)[:want]
		p.hit.Add(1)
	}

	return *buf
}

func (p *Allocator[K, V]) Free(b []swiss.Group[K, V]) {
	p.pool.Put(&b)
}

func (p *Allocator[K, V]) Miss() uint64 {
	return p.miss.Load()
}

func (p *Allocator[K, V]) Hit() uint64 {
	return p.hit.Load()
}
