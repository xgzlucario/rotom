package net

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	ast := assert.New(t)
	testCount := 100

	t.Run("echo-server", func(t *testing.T) {
		fd, err := TcpServer(20083)
		ast.Nil(err)

		var wg sync.WaitGroup

		// start listener
		go func() {
			for {
				cfd, err := Accept(fd)
				ast.Nil(err)
				wg.Add(1)

				// read
				var buf [32]byte
				n, _ := Read(cfd, buf[:])

				// write
				Write(cfd, buf[:n])
				wg.Done()
			}
		}()

		// start clients
		go func() {
			for i := 0; i < testCount; i++ {
				tcpConn, err := net.Dial("tcp", "127.0.0.1:20083")
				ast.Nil(err)
				wg.Add(1)

				// write
				msg := fmt.Sprintf("%d", time.Now().UnixNano())
				tcpConn.Write([]byte(msg))

				// read
				var res [32]byte
				n, err := tcpConn.Read(res[:])
				ast.Nil(err)

				// equal
				ast.Equal(msg, string(res[:n]))
				wg.Done()
			}
		}()

		wg.Wait()
	})
}
