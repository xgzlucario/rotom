package timer

import (
	"sync/atomic"
	"time"
)

var (
	nanotime atomic.Int64
)

func Init() {
	go func() {
		tk := time.NewTicker(time.Millisecond / 10)
		for t := range tk.C {
			nanotime.Store(t.UnixNano())
		}
	}()
}

func GetNanoTime() int64 {
	return nanotime.Load()
}
