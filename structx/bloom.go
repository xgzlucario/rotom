package structx

import (
	"github.com/bits-and-blooms/bloom"
)

// Bloom
type Bloom struct {
	*bloom.BloomFilter
}

// NewBloom
func NewBloom() *Bloom {
	return &Bloom{bloom.NewWithEstimates(100*10000, 0.01)}
}
