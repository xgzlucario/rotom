package base

import (
	"unsafe"

	"github.com/bytedance/sonic"
)

// marshal
func MarshalJSON(data any) ([]byte, error) {
	if _, ok := data.(Marshaler); ok {
		return data.(Marshaler).MarshalJSON()
	}
	return sonic.Marshal(data)
}

func UnmarshalJSON(src []byte, data any) error {
	if _, ok := data.(Marshaler); ok {
		return data.(Marshaler).UnmarshalJSON(src)
	}
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
