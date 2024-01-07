package wal

import "github.com/klauspost/compress/zstd"

var (
	encoder, _ = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	decoder, _ = zstd.NewReader(nil)
)

// compress
func compress(src, dst []byte) []byte {
	if dst == nil {
		dst = make([]byte, 0, len(src)/4)
	}
	return encoder.EncodeAll(src, dst)
}

// decompress
func decompress(src, dst []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}
