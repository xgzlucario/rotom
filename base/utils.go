package base

import (
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/klauspost/compress/zstd"
)

// marshal
func MarshalJSON(data any) ([]byte, error) {
	return sonic.Marshal(data)
}

func UnmarshalJSON(src []byte, data any) error {
	return sonic.Unmarshal(src, data)
}

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

// zstd compress
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
