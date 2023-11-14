package codeman

import (
	"github.com/klauspost/compress/zstd"
)

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	decoder, _ = zstd.NewReader(nil)
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

func (s *Block) Len() int {
	return len(s.b)
}

func Compress(src, dst []byte) []byte {
	return encoder.EncodeAll(src, dst)
}

func Decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
