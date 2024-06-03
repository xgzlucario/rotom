package main

import "unsafe"

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func ToLowerNoCopy(b []byte) []byte {
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return b
}
