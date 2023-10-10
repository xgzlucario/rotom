package base

import (
	"runtime"
	"sync/atomic"
)

// LfLocker is a lock-free locker.
type LfLocker struct {
	ref *int32
}

func NewLfLocker() *LfLocker {
	return &LfLocker{ref: new(int32)}
}

func (l *LfLocker) Lock() {
	for !atomic.CompareAndSwapInt32(l.ref, 0, -1) {
		runtime.Gosched()
	}
}

func (l *LfLocker) Unlock() {
	for !atomic.CompareAndSwapInt32(l.ref, -1, 0) {
		runtime.Gosched()
	}
}

func (l *LfLocker) RLock() {
	for {
		oldRef := atomic.LoadInt32(l.ref)
		if oldRef >= 0 && atomic.CompareAndSwapInt32(l.ref, oldRef, oldRef+1) {
			break
		}
		runtime.Gosched()
	}
}

func (l *LfLocker) RUnlock() {
	for {
		oldRef := atomic.LoadInt32(l.ref)
		if oldRef > 0 && atomic.CompareAndSwapInt32(l.ref, oldRef, oldRef-1) {
			break
		}
		runtime.Gosched()
	}
}
