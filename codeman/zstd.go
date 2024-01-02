package codeman

import (
	"github.com/klauspost/compress/zstd"
)

var (
	encoder, _ = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	decoder, _ = zstd.NewReader(nil)
)

// Compress
func Compress(src, dst []byte) []byte {
	if dst == nil {
		dst = make([]byte, 0, len(src)/4)
	}
	return encoder.EncodeAll(src, dst)
}

// Decompress
func Decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
