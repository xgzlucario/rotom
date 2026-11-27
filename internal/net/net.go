// Package net wraps the raw socket syscalls used by the event loop.
// All sockets returned by this package are non-blocking.
package net

import (
	"errors"

	"golang.org/x/sys/unix"
)

/*
BACKLOG is TCP listen() backlog.
In high requests-per-second environments you need a high backlog in order
to avoid slow clients connections issues. Note that the Linux kernel
will silently truncate it to the value of /proc/sys/net/core/somaxconn so
make sure to raise both the value of somaxconn and tcp_max_syn_backlog
in order to get the desired effect.
*/
const BACKLOG int = 511

// IsTemporary reports whether err means "try again later" on a non-blocking socket.
func IsTemporary(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR)
}

// Accept accepts a connection and returns it as a non-blocking socket with TCP_NODELAY.
func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	if err != nil {
		return -1, err
	}
	unix.CloseOnExec(nfd)
	if err = unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
		return -1, err
	}
	// best effort, mirrors redis `tcp-nodelay`.
	_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	return nfd, nil
}

func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}

func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}

func Close(fd int) error {
	return unix.Close(fd)
}

// TcpServer creates a non-blocking listening socket on 0.0.0.0:port.
func TcpServer(port int) (fd int, err error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	defer func() {
		if err != nil {
			_ = unix.Close(s)
		}
	}()
	unix.CloseOnExec(s)
	// SO_REUSEADDR allows fast restarts; unlike SO_REUSEPORT it does not let
	// two servers silently share the same port.
	if err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return -1, err
	}
	if err = unix.SetNonblock(s, true); err != nil {
		return -1, err
	}
	if err = unix.Bind(s, &unix.SockaddrInet4{Port: port}); err != nil {
		return -1, err
	}
	if err = unix.Listen(s, BACKLOG); err != nil {
		return -1, err
	}
	return s, nil
}
