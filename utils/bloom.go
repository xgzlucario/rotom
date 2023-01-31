package structx


import "github.com/bits-and-blooms/bloom/v3"

type Bloom struct {
	*bloom.BloomFilter
}

func NewBloom() *Bloom {
	return &Bloom{bloom.NewWithEstimates(1000000, 0.01)}
}
