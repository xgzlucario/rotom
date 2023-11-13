package codeman

import (
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	// decoder, _ = zstd.NewReader(nil)
)

// Block is basic storage union for rotom.
// +---------------------+-----------------------------+----------+
// |   blk_len(varint)   |      blk_data(blk_len)      |  crc(4)  |
// +---------------------+-----------------------------+----------+
type Block struct {
	b []byte
}

func NewBlock(buf []byte) *Block {
	return &Block{b: buf}
}

func (s *Block) Checksum() uint32 {
	return crc32.ChecksumIEEE(s.b)
}

func (s *Block) Len() int {
	return len(s.b)
}

func (s *Block) Compress() []byte {
	return encoder.EncodeAll(s.b, nil)
}
