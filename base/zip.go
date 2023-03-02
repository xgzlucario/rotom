package base

import (
	"github.com/klauspost/compress/zstd"
)

// Use Zstd Compress

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(1))
	decoder, _ = zstd.NewReader(nil)
)

func ZstdEncode(src []byte) []byte {
	return encoder.EncodeAll(src, nil)
}

func ZstdDecode(src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, nil)
}
