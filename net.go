package main

import (
	"golang.org/x/sys/unix"
)

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
