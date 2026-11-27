//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package ae

import (
	"errors"

	"golang.org/x/sys/unix"
)

type poller struct {
	fd      int
	events  []unix.Kevent_t
	changes []unix.Kevent_t
}

func newPoller() (*poller, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	return &poller{fd: fd, events: make([]unix.Kevent_t, 1024)}, nil
}

func (p *poller) update(fd int, oldMask, newMask Mask) error {
	p.changes = p.changes[:0]
	add := func(filter int16, flags uint16) {
		var ev unix.Kevent_t
		unix.SetKevent(&ev, fd, int(filter), int(flags))
		p.changes = append(p.changes, ev)
	}
	if oldMask&Readable == 0 && newMask&Readable != 0 {
		add(unix.EVFILT_READ, unix.EV_ADD)
	} else if oldMask&Readable != 0 && newMask&Readable == 0 {
		add(unix.EVFILT_READ, unix.EV_DELETE)
	}
	if oldMask&Writable == 0 && newMask&Writable != 0 {
		add(unix.EVFILT_WRITE, unix.EV_ADD)
	} else if oldMask&Writable != 0 && newMask&Writable == 0 {
		add(unix.EVFILT_WRITE, unix.EV_DELETE)
	}
	if len(p.changes) == 0 {
		return nil
	}
	_, err := unix.Kevent(p.fd, p.changes, nil, nil)
	return err
}

func (p *poller) wait(timeoutMs int, fn func(fd int, mask Mask)) {
	ts := unix.NsecToTimespec(int64(timeoutMs) * 1e6)
	n, err := unix.Kevent(p.fd, nil, p.events, &ts)
	if err != nil {
		if !errors.Is(err, unix.EINTR) {
			panic(err)
		}
		return
	}
	for _, ev := range p.events[:n] {
		var mask Mask
		switch ev.Filter {
		case unix.EVFILT_READ:
			mask = Readable
		case unix.EVFILT_WRITE:
			mask = Writable
		}
		if ev.Flags&(unix.EV_EOF|unix.EV_ERROR) != 0 {
			mask |= Readable | Writable
		}
		fn(int(ev.Ident), mask)
	}
}

func (p *poller) close() error { return unix.Close(p.fd) }
