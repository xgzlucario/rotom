// Package ae is a small single-threaded event loop modeled after redis ae.c.
//
// File events are registered per fd with a readable and/or writable mask,
// each with its own handler. Time events return the number of milliseconds
// until they should fire again, or NoMore to be removed.
package ae

import (
	"time"
)

type Mask uint8

const (
	None     Mask = 0
	Readable Mask = 1 << 0
	Writable Mask = 1 << 1
)

// NoMore is returned by a TimeProc to indicate that it should not be rescheduled.
const NoMore int64 = -1

// maxWaitMs bounds how long the loop blocks when no time events are pending,
// so that Stop() is observed in a timely manner.
const maxWaitMs = 1000

type FileProc func(loop *Loop, fd int, extra any)

// TimeProc returns the delay in milliseconds before its next run, or NoMore.
type TimeProc func(loop *Loop, id int, extra any) int64

type fileEvent struct {
	mask  Mask
	rproc FileProc
	wproc FileProc
	extra any
}

type timeEvent struct {
	id      int
	when    int64 // unix ms
	proc    TimeProc
	extra   any
	deleted bool
}

type fired struct {
	fd   int
	mask Mask
}

type Loop struct {
	poller      *poller
	events      map[int]*fileEvent
	timeEvents  []*timeEvent
	nextTimeID  int
	beforeSleep func(*Loop)
	stopped     bool
	fired       []fired
}

func New() (*Loop, error) {
	p, err := newPoller()
	if err != nil {
		return nil, err
	}
	return &Loop{
		poller:     p,
		events:     make(map[int]*fileEvent),
		nextTimeID: 1,
		fired:      make([]fired, 0, 128),
	}, nil
}

// AddFileEvent adds mask to the events monitored on fd. The handler is used
// for every bit in mask; extra replaces the previous client data.
func (l *Loop) AddFileEvent(fd int, mask Mask, proc FileProc, extra any) error {
	fe := l.events[fd]
	if fe == nil {
		fe = &fileEvent{}
	}
	newMask := fe.mask | mask
	if newMask != fe.mask {
		if err := l.poller.update(fd, fe.mask, newMask); err != nil {
			return err
		}
	}
	fe.mask = newMask
	if mask&Readable != 0 {
		fe.rproc = proc
	}
	if mask&Writable != 0 {
		fe.wproc = proc
	}
	fe.extra = extra
	l.events[fd] = fe
	return nil
}

// DelFileEvent removes mask from the events monitored on fd.
func (l *Loop) DelFileEvent(fd int, mask Mask) {
	fe := l.events[fd]
	if fe == nil {
		return
	}
	newMask := fe.mask &^ mask
	if newMask != fe.mask {
		// errors here mean the fd is already gone; nothing useful to do.
		_ = l.poller.update(fd, fe.mask, newMask)
	}
	fe.mask = newMask
	if newMask == None {
		delete(l.events, fd)
	}
}

// FileEvents returns the mask currently monitored on fd.
func (l *Loop) FileEvents(fd int) Mask {
	if fe := l.events[fd]; fe != nil {
		return fe.mask
	}
	return None
}

// AddTimeEvent schedules proc to run after ms milliseconds.
func (l *Loop) AddTimeEvent(ms int64, proc TimeProc, extra any) int {
	id := l.nextTimeID
	l.nextTimeID++
	l.timeEvents = append(l.timeEvents, &timeEvent{
		id:    id,
		when:  nowMs() + ms,
		proc:  proc,
		extra: extra,
	})
	return id
}

func (l *Loop) DelTimeEvent(id int) {
	for _, te := range l.timeEvents {
		if te.id == id {
			te.deleted = true
		}
	}
}

// SetBeforeSleep registers fn to be called before every poll.
func (l *Loop) SetBeforeSleep(fn func(*Loop)) { l.beforeSleep = fn }

// Stop makes Main return after the current iteration.
func (l *Loop) Stop() { l.stopped = true }

func (l *Loop) Main() {
	l.stopped = false
	for !l.stopped {
		l.ProcessEvents()
	}
}

func (l *Loop) Close() error { return l.poller.close() }

func nowMs() int64 { return time.Now().UnixMilli() }

// waitTimeout returns how long the poller may block, in ms.
func (l *Loop) waitTimeout() int {
	timeout := int64(maxWaitMs)
	now := nowMs()
	for _, te := range l.timeEvents {
		if te.deleted {
			continue
		}
		timeout = min(timeout, te.when-now)
	}
	return int(max(timeout, 0))
}

// ProcessEvents runs one iteration: beforeSleep, poll, file events, time events.
func (l *Loop) ProcessEvents() {
	if l.beforeSleep != nil {
		l.beforeSleep(l)
	}

	l.fired = l.fired[:0]
	l.poller.wait(l.waitTimeout(), func(fd int, mask Mask) {
		l.fired = append(l.fired, fired{fd, mask})
	})

	for _, f := range l.fired {
		// Look the event up again: an earlier handler in this batch may
		// have removed or replaced it.
		fe := l.events[f.fd]
		if fe == nil {
			continue
		}
		if fe.mask&f.mask&Readable != 0 {
			fe.rproc(l, f.fd, fe.extra)
		}
		fe = l.events[f.fd]
		if fe == nil {
			continue
		}
		if fe.mask&f.mask&Writable != 0 {
			fe.wproc(l, f.fd, fe.extra)
		}
	}

	l.processTimeEvents()
}

func (l *Loop) processTimeEvents() {
	now := nowMs()
	// iterate over a snapshot: procs may add new time events.
	n := len(l.timeEvents)
	for i := range n {
		te := l.timeEvents[i]
		if te.deleted || te.when > now {
			continue
		}
		next := te.proc(l, te.id, te.extra)
		if next == NoMore {
			te.deleted = true
		} else {
			te.when = nowMs() + next
		}
	}
	// compact deleted events
	alive := l.timeEvents[:0]
	for _, te := range l.timeEvents {
		if !te.deleted {
			alive = append(alive, te)
		}
	}
	clear(l.timeEvents[len(alive):])
	l.timeEvents = alive
}
