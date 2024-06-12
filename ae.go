package main

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type FeType int

const (
	AE_READABLE FeType = iota + 1
	AE_WRITABLE
)

type TeType int

const (
	AE_NORMAL TeType = iota + 1
	AE_ONCE
)

type FileProc func(loop *AeLoop, fd int, extra interface{})
type TimeProc func(loop *AeLoop, id int, extra interface{})

type AeFileEvent struct {
	fd    int
	mask  FeType
	proc  FileProc
	extra interface{}
}

type AeTimeEvent struct {
	id       int
	mask     TeType
	when     int64 // ms
	interval int64 // ms
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

var fe2ep = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}

func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	} else {
		return fd * -1
	}
}

func (loop *AeLoop) getEpollMask(fd int) (ev uint32) {
	if loop.FileEvents[getFeKey(fd, AE_READABLE)] != nil {
		ev |= fe2ep[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, AE_WRITABLE)] != nil {
		ev |= fe2ep[AE_WRITABLE]
	}
	return
}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc FileProc, extra interface{}) {
	// epoll ctl
	ev := loop.getEpollMask(fd)
	if ev&fe2ep[mask] != 0 {
		// event is already registered
		return
	}
	op := unix.EPOLL_CTL_ADD
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	ev |= fe2ep[mask]
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		logger.Error().Msgf("epoll ctl error: %v", err)
		return
	}
	// ae ctl
	loop.FileEvents[getFeKey(fd, mask)] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	// epoll ctl
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= ^fe2ep[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Error().Msgf("epoll del error: %v", err)
		}
	}
	// ae ctl
	loop.FileEvents[getFeKey(fd, mask)] = nil
}

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra interface{}) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	te := AeTimeEvent{
		id:       id,
		mask:     mask,
		interval: interval,
		when:     GetMsTime() + interval,
		proc:     proc,
		extra:    extra,
		next:     loop.TimeEvents,
	}
	loop.TimeEvents = &te
	return id
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}

func AeLoopCreate() (*AeLoop, error) {
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
	}, nil
}

func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	timeout := loop.nearestTime() - GetMsTime()
	if timeout <= 0 {
		timeout = 10 // at least wait 10ms
	}

	var events [128]unix.EpollEvent
retry:
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		// interrupted system call
		if err == unix.EINTR {
			goto retry
		}
		logger.Error().Msgf("epoll wait error: %v", err)
		return
	}

	// collect file events
	for _, ev := range events[:n] {
		if ev.Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[getFeKey(int(ev.Fd), AE_READABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[getFeKey(int(ev.Fd), AE_WRITABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}

	// collect time events
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = GetMsTime() + te.interval
		}
	}
	for _, fe := range fes {
		fe.proc(loop, fe.fd, fe.extra)
	}
}

func (loop *AeLoop) AeMain() {
	for {
		loop.AeProcess(loop.AeWait())
	}
}
