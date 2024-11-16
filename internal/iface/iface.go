package iface

import "github.com/xgzlucario/rotom/internal/resp"

type Encoder interface {
	Encode(writer *resp.Writer) error
	Decode(reader *resp.Reader) error
}

type MapI interface {
	Encoder
	Set(key string, val []byte) bool
	Get(key string) ([]byte, bool)
	Remove(key string) bool
	Len() int
	Scan(fn func(key string, val []byte))
}

type SetI interface {
	Encoder
	Add(key string) bool
	Exist(key string) bool
	Remove(key string) bool
	Pop() (key string, ok bool)
	Scan(fn func(key string))
	Len() int
}

type ListI interface {
	Encoder
}

type ZSetI interface {
	Encoder
	Get(key string) (score float64, ok bool)
	Set(key string, score float64) bool
	Remove(key string) bool
	Len() int
	PopMin() (key string, score float64)
	Rank(key string) int
	Scan(fn func(key string, score float64))
}
