package dict

import (
	"math"
	"time"
)

type Idx struct {
	hi uint32 // hi is position of data.
	lo int64  // lo is timestamp of key.
}

func (i Idx) start() int {
	return int(i.hi)
}

func (i Idx) expired() bool {
	return i.lo > noTTL && i.lo < time.Now().UnixNano()
}

func (i Idx) expiredWith(nanosec int64) bool {
	return i.lo > noTTL && i.lo < nanosec
}

func (i Idx) setTTL(ts int64) Idx {
	i.lo = ts
	return i
}

func (i Idx) setStart(start int) Idx {
	check(start)
	i.hi = uint32(start)
	return i
}

func check(x int) {
	if x > math.MaxUint32 {
		panic("x overflows the limit of uint32")
	}
}

func newIdx(start int, ttl int64) Idx {
	check(start)
	return Idx{hi: uint32(start), lo: ttl}
}
