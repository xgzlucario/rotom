package utils

import (
	"sync"
)

// BufferPool is a bytes buffer pool.
type BufferPool struct {
	pool      *sync.Pool
	miss, hit uint64
}

// Get returns buffer with length of want.
func (p *BufferPool) Get(want int) []byte {
	buf := p.pool.Get().(*[]byte)

	if cap(*buf) < want {
		*buf = make([]byte, want)
		p.miss++

	} else {
		*buf = (*buf)[:want]
		p.hit++
	}

	return *buf
}

// Put adds given buffer to the pool.
func (p *BufferPool) Put(b []byte) {
	p.pool.Put(&b)
}

// NewBufferPool creates a new buffer pool instance.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: &sync.Pool{
			New: func() interface{} { return new([]byte) },
		},
	}
}
