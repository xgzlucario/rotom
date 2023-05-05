package base

import (
	"unsafe"
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
