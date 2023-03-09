package structx

import (
	"github.com/bits-and-blooms/bloom"
	"github.com/xgzlucario/rotom/base"
)

// Bloom
type Bloom struct {
	*bloom.BloomFilter
}

// NewBloom
func NewBloom() *Bloom {
	return &Bloom{bloom.NewWithEstimates(1000000, 0.01)}
}

// MarshalJSON
func (b *Bloom) MarshalJSON() ([]byte, error) {
	src, err := b.BloomFilter.GobEncode()
	if err != nil {
		return nil, err
	}
	return base.ZstdEncode(src), nil
}

// UnmarshalJSON
func (b *Bloom) UnmarshalJSON(src []byte) error {
	src, err := base.ZstdDecode(src)
	if err != nil {
		return err
	}
	return b.BloomFilter.GobDecode(src)
}
