package main

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	assert := assert.New(t)
	testCount := 100

	t.Run("serve", func(t *testing.T) {
		fd, err := TcpServer(20083)
		assert.Nil(err)

		// start listener
		go func() {
			for {
				cfd, err := Accept(fd)
				assert.Nil(err)

				// read
				var buf [32]byte
				n, _ := Read(cfd, buf[:])

				// write
				Write(cfd, buf[:n])
			}
		}()

		var wg sync.WaitGroup

		// start clients
		go func() {
			for i := 0; i < testCount; i++ {
				tcpConn, err := net.Dial("tcp", "127.0.0.1:20083")
				assert.Nil(err)
				wg.Add(1)

				// write
				msg := fmt.Sprintf("%d", time.Now().UnixNano())
				tcpConn.Write([]byte(msg))

				// read
				var res [32]byte
				n, err := tcpConn.Read(res[:])
				assert.Nil(err)

				// equal
				assert.Equal(msg, string(res[:n]))
				wg.Done()
			}
		}()

		wg.Wait()

		time.Sleep(time.Second)
	})
}
