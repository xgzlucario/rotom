package ae

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func TestLoop(t *testing.T) {
	ast := assert.New(t)
	loop, err := New()
	ast.Nil(err)
	defer loop.Close()

	var fds [2]int
	ast.Nil(unix.Pipe(fds[:]))
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	ast.Nil(unix.SetNonblock(fds[0], true))

	var got []byte
	ast.Nil(loop.AddFileEvent(fds[0], Readable, func(l *Loop, fd int, extra any) {
		var buf [16]byte
		n, _ := unix.Read(fd, buf[:])
		got = append(got, buf[:n]...)
		ast.Equal("extra", extra)
		l.DelFileEvent(fd, Readable)
	}, "extra"))
	ast.Equal(Readable, loop.FileEvents(fds[0]))

	ticks := 0
	loop.AddTimeEvent(1, func(l *Loop, id int, _ any) int64 {
		ticks++
		if ticks == 3 {
			l.Stop()
			return NoMore
		}
		return 1
	}, nil)
	once := 0
	id := loop.AddTimeEvent(0, func(*Loop, int, any) int64 { once++; return NoMore }, nil)
	deleted := loop.AddTimeEvent(0, func(*Loop, int, any) int64 { t.Error("deleted event ran"); return NoMore }, nil)
	loop.DelTimeEvent(deleted)

	before := 0
	loop.SetBeforeSleep(func(*Loop) { before++ })

	_, _ = unix.Write(fds[1], []byte("hello"))
	loop.Main()

	ast.Equal("hello", string(got))
	ast.Equal(None, loop.FileEvents(fds[0]))
	ast.Equal(3, ticks)
	ast.Equal(1, once)
	ast.Greater(before, 0)
	ast.NotZero(id)
	ast.Empty(loop.timeEvents)
}
