package dict

import (
	"errors"
	"math/bits"
	"unsafe"
)

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

type HashFn func(string) uint64

// MemHash is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aes hash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// SizeUvarint
// See https://go-review.googlesource.com/c/go/+/572196/1/src/encoding/binary/varint.go#174
func SizeUvarint(x uint64) int {
	return int(9*uint32(bits.Len64(x))+64) / 64
}

type Options struct {
	ShardCount uint32

	// Default size of the bucket initial.
	IndexSize  int
	BufferSize int

	// Migrate threshold for a bucket to trigger a migration.
	MigrateRatio float64
}

var DefaultOptions = Options{
	ShardCount:   1024,
	IndexSize:    1024,
	BufferSize:   64 * KB,
	MigrateRatio: 0.4,
}

func validateOptions(options Options) error {
	if options.ShardCount == 0 {
		return errors.New("cache/options: invalid shard count")
	}
	return nil
}
