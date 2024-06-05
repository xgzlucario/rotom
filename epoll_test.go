package main

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func startListener(onAccept func(net.Conn)) {
	listener, _ := net.Listen("tcp", ":20081")
	for {
		conn, _ := listener.Accept()
		onAccept(conn)
	}
}

func TestEpollCreate(t *testing.T) {
	assert := assert.New(t)
	ep, err := MkEpoll()
	assert.Greater(ep.fd, 0)
	assert.Nil(err)
}

func TestEpollAdd(t *testing.T) {
	assert := assert.New(t)

	ep, _ := MkEpoll()
	go startListener(func(c net.Conn) {
		ep.Add(c)
	})
	// wait for listener startup
	time.Sleep(time.Second / 10)

	cli, _ := net.Dial("tcp", ":20081")

	conns, err := ep.Wait()
	assert.Equal(len(conns), 0)
	assert.Nil(err)

	cli.Write([]byte("hello world"))
	// wait for event ready
	time.Sleep(time.Millisecond * 10)

	conns, _ = ep.Wait()
	assert.Equal(len(conns), 1)
}
