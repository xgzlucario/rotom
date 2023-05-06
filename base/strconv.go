package base

import (
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	decoder, _ = zstd.NewReader(nil)
)

// string and bytes convert unsafe
func S2B(str *string) []byte {
	strHeader := (*[2]uintptr)(unsafe.Pointer(str))
	byteSliceHeader := [3]uintptr{
		strHeader[0], strHeader[1], strHeader[1],
	}
	return *(*[]byte)(unsafe.Pointer(&byteSliceHeader))
}

func B2S(buf []byte) *string {
	return (*string)(unsafe.Pointer(&buf))
}

// Zstd encoder and decoder
func ZstdEncode(src []byte) []byte {
	return encoder.EncodeAll(src, nil)
}

func ZstdDecode(src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, nil)
}
