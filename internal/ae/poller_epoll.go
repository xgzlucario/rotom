//go:build linux

package ae

import (
	"errors"

	"golang.org/x/sys/unix"
)

type poller struct {
	fd     int
	events []unix.EpollEvent
}

func newPoller() (*poller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &poller{fd: fd, events: make([]unix.EpollEvent, 1024)}, nil
}

func toEpoll(mask Mask) uint32 {
	var ev uint32
	if mask&Readable != 0 {
		ev |= unix.EPOLLIN
	}
	if mask&Writable != 0 {
		ev |= unix.EPOLLOUT
	}
	return ev
}

func (p *poller) update(fd int, oldMask, newMask Mask) error {
	ev := &unix.EpollEvent{Fd: int32(fd), Events: toEpoll(newMask)}
	switch {
	case oldMask == None:
		return unix.EpollCtl(p.fd, unix.EPOLL_CTL_ADD, fd, ev)
	case newMask == None:
		return unix.EpollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, ev)
	default:
		return unix.EpollCtl(p.fd, unix.EPOLL_CTL_MOD, fd, ev)
	}
}

func (p *poller) wait(timeoutMs int, fn func(fd int, mask Mask)) {
	n, err := unix.EpollWait(p.fd, p.events, timeoutMs)
	if err != nil {
		if !errors.Is(err, unix.EINTR) {
			panic(err)
		}
		return
	}
	for _, ev := range p.events[:n] {
		var mask Mask
		if ev.Events&unix.EPOLLIN != 0 {
			mask |= Readable
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			mask |= Writable
		}
		// report errors/hangups as both, so handlers observe them on read/write.
		if ev.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
			mask |= Readable | Writable
		}
		fn(int(ev.Fd), mask)
	}
}

func (p *poller) close() error { return unix.Close(p.fd) }
