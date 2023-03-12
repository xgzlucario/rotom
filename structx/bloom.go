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
	return &Bloom{bloom.NewWithEstimates(1000000, 0.01)}
}
