package base

import (
	"context"
	"time"
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

// Go start a background worker
func Go(ctx context.Context, interval time.Duration, f func()) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(interval)
				f()
			}
		}
	}()
}
