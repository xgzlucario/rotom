package structx


import (
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
