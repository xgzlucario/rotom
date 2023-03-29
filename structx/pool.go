package structx

import (
	"runtime"

	"github.com/sourcegraph/conc/pool"
)

// Pool
type Pool struct {
	*pool.Pool
}

// NewPool
func NewPool() *Pool {
	return &Pool{pool.New()}
}

// NewDefaultPool
func NewDefaultPool() *Pool {
	return &Pool{pool.New().WithMaxGoroutines(runtime.NumCPU())}
}
