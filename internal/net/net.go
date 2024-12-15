package net

import (
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

func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
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

func TcpServer(port int) (int, error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}
	err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	if err != nil {
		return -1, err
	}
	err = unix.Bind(s, &unix.SockaddrInet4{Port: port})
	if err != nil {
		return -1, err
	}
	err = unix.Listen(s, BACKLOG)
	if err != nil {
		return -1, err
	}
	return s, nil
}
