package main

import (
	"github.com/xgzlucario/rotom/internal/dict"
	"golang.org/x/sys/unix"
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

	_fevents []*AeFileEvent // fes cache
}

func (loop *AeLoop) AddRead(fd int, proc FileProc, extra interface{}) {
	err := unix.EpollCtl(loop.fileEventFd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: unix.EPOLLIN,
	})
	if err != nil {
		panic(err)
	}
	loop.FileEvents[fd] = &AeFileEvent{
		fd:    fd,
		proc:  proc,
		extra: extra,
	}
}

func (loop *AeLoop) ModRead(fd int, proc FileProc, extra interface{}) {
	err := unix.EpollCtl(loop.fileEventFd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: unix.EPOLLIN,
	})
	if err != nil {
		panic(err)
	}
	fe := loop.FileEvents[fd]
	fe.proc = proc
	fe.extra = extra
}

func (loop *AeLoop) ModWrite(fd int, proc FileProc, extra interface{}) {
	err := unix.EpollCtl(loop.fileEventFd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: unix.EPOLLOUT,
	})
	if err != nil {
		panic(err)
	}
	fe := loop.FileEvents[fd]
	fe.proc = proc
	fe.extra = extra
}

func (loop *AeLoop) ModDetach(fd int) {
	err := unix.EpollCtl(loop.fileEventFd, unix.EPOLL_CTL_DEL, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: unix.EPOLLIN | unix.EPOLLOUT,
	})
	if err != nil {
		panic(err)
	}
	delete(loop.FileEvents, fd)
}

func GetMsTime() int64 {
	return dict.GetNanoTime() / 1e6
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
		_fevents:        make([]*AeFileEvent, 128), // pre alloc
	}, nil
}

func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		nearest = min(nearest, p.when)
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
		log.Error().Msgf("epoll wait error: %v", err)
		return
	}

	// collect file events
	fes = loop._fevents[:0]
	for _, ev := range events[:n] {
		if ev.Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[int(ev.Fd)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[int(ev.Fd)]
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
	for !loop.stop {
		loop.AeProcess(loop.AeWait())
	}
}
