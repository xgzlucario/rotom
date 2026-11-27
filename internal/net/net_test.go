package net

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// retry calls fn until it returns a non-temporary result.
func retry(fn func() (int, error)) (int, error) {
	for {
		n, err := fn()
		if err != nil && IsTemporary(err) {
			time.Sleep(time.Millisecond)
			continue
		}
		return n, err
	}
}

func TestServer(t *testing.T) {
	ast := assert.New(t)
	testCount := 100

	t.Run("echo-server", func(t *testing.T) {
		fd, err := TcpServer(20083)
		ast.Nil(err)
		defer Close(fd)

		// start listener
		go func() {
			for {
				cfd, err := retry(func() (int, error) { return Accept(fd) })
				if err != nil {
					return
				}
				var buf [32]byte
				n, _ := retry(func() (int, error) { return Read(cfd, buf[:]) })
				_, _ = Write(cfd, buf[:n])
				_ = Close(cfd)
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < testCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := net.Dial("tcp", "127.0.0.1:20083")
				if !ast.Nil(err) {
					return
				}
				defer conn.Close()

				msg := fmt.Sprintf("%d", time.Now().UnixNano())
				_, _ = conn.Write([]byte(msg))

				var res [32]byte
				n, err := conn.Read(res[:])
				ast.Nil(err)
				ast.Equal(msg, string(res[:n]))
			}()
		}
		wg.Wait()
	})

	t.Run("nonblocking", func(t *testing.T) {
		fd, err := TcpServer(20084)
		ast.Nil(err)
		defer Close(fd)

		// no pending connection: must not block
		_, err = Accept(fd)
		ast.True(IsTemporary(err))
	})
}
