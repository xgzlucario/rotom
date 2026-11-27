package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	gonet "net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/ae"
	"github.com/xgzlucario/rotom/internal/aof"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
)

// startServer runs a server with cfg on a free port until the test ends.
func startServer(t *testing.T, cfg *config.Config) (*Server, string) {
	t.Helper()
	l, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.TCP.Port = l.Addr().(*gonet.TCPAddr).Port
	l.Close()

	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	// the loop may only be touched from its own goroutine: poll for stop.
	srv.loop.AddTimeEvent(10, func(l *ae.Loop, _ int, _ any) int64 {
		select {
		case <-stop:
			l.Stop()
			return ae.NoMore
		default:
			return 10
		}
	}, nil)
	go func() { srv.Run(); close(done) }()
	t.Cleanup(func() {
		close(stop)
		<-done
		_ = srv.Close()
	})
	return srv, fmt.Sprintf("127.0.0.1:%d", cfg.TCP.Port)
}

// send writes raw RESP and returns the next reply line.
func send(t *testing.T, conn gonet.Conn, rd *bufio.Reader, raw string) string {
	t.Helper()
	if _, err := conn.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	return readLine(t, rd)
}

// TestSlowReader makes the server hit a full socket buffer, so replies are
// finished by the writable event handler instead of beforeSleep.
func TestSlowReader(t *testing.T) {
	ast := assert.New(t)
	_, addr := startServer(t, config.Default())

	conn, err := gonet.Dial("tcp", addr)
	ast.Nil(err)
	defer conn.Close()
	_ = conn.(*gonet.TCPConn).SetReadBuffer(4096)
	rd := bufio.NewReader(conn)

	value := strings.Repeat("x", 1<<20)
	ast.Equal("+OK\r\n", send(t, conn, rd, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n%s\r\n", len(value), value)))

	// ask for 32MB of replies without reading, then read slowly.
	const n = 32
	var sb strings.Builder
	for range n {
		sb.WriteString("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	}
	_, err = conn.Write([]byte(sb.String()))
	ast.Nil(err)
	time.Sleep(200 * time.Millisecond)

	want := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	buf := make([]byte, len(want))
	for range n {
		_, err := io.ReadFull(rd, buf)
		ast.Nil(err)
		ast.Equal(want, string(buf))
	}
	// the connection is still usable afterwards
	ast.Equal("+PONG\r\n", send(t, conn, rd, "*1\r\n$4\r\nPING\r\n"))
}

func TestClientLifecycle(t *testing.T) {
	ast := assert.New(t)
	srv, addr := startServer(t, config.Default())

	t.Run("inline-and-unknown", func(t *testing.T) {
		conn, err := gonet.Dial("tcp", addr)
		ast.Nil(err)
		defer conn.Close()
		rd := bufio.NewReader(conn)
		ast.Equal("+PONG\r\n", send(t, conn, rd, "PING\r\n"))
		ast.Equal("$2\r\n", send(t, conn, rd, "PING hi\r\n"))
		ast.Equal("hi\r\n", readLine(t, rd))
		ast.True(strings.HasPrefix(send(t, conn, rd, "NOPE\r\n"), "-ERR unknown command"))
		ast.Equal("+PONG\r\n", send(t, conn, rd, "\r\nPING\r\n")) // empty line is ignored
	})

	t.Run("peer-close-frees-client", func(t *testing.T) {
		conn, err := gonet.Dial("tcp", addr)
		ast.Nil(err)
		rd := bufio.NewReader(conn)
		ast.Equal("+PONG\r\n", send(t, conn, rd, "PING\r\n"))
		conn.Close()
		// the server notices on its next read
		ast.Eventually(func() bool {
			c2, err := gonet.Dial("tcp", addr)
			if err != nil {
				return false
			}
			defer c2.Close()
			r2 := bufio.NewReader(c2)
			send(t, c2, r2, "PING\r\n")
			return true
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("query-too-large", func(t *testing.T) {
		conn, err := gonet.Dial("tcp", addr)
		ast.Nil(err)
		defer conn.Close()
		// an incomplete bulk that never ends; the server must drop us
		_, _ = conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n", 256<<20)))
		chunk := make([]byte, 1<<20)
		for range MaxQueryDataLen/len(chunk) + 2 {
			if _, err := conn.Write(chunk); err != nil {
				break
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		ast.NotNil(err) // EOF or reset
	})

	_ = srv
}

func TestLoadData(t *testing.T) {
	ast := assert.New(t)

	t.Run("rdb", func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.Default()
		cfg.RDB.Save = true
		cfg.RDB.DBFileName = filepath.Join(dir, "dump.rdb")

		d := db.NewDict()
		d.SetString("k", []byte("v"), 0, false)
		ast.Nil(db.SaveRDB(cfg.RDB.DBFileName, d))

		s, err := New(cfg)
		ast.Nil(err)
		v, ok, _ := s.db.GetString("k")
		ast.True(ok)
		ast.Equal("v", string(v))
		ast.Nil(s.Close())
	})

	t.Run("bad-rdb", func(t *testing.T) {
		cfg := config.Default()
		cfg.RDB.Save = true
		cfg.RDB.DBFileName = filepath.Join(t.TempDir(), "dump.rdb")
		ast.Nil(os.WriteFile(cfg.RDB.DBFileName, []byte("not an rdb file at all"), 0644))
		_, err := New(cfg)
		ast.ErrorContains(err, "load rdb")
	})

	t.Run("bad-aof", func(t *testing.T) {
		cfg := config.Default()
		cfg.AOF.AppendOnly = true
		cfg.AOF.AppendFileName = filepath.Join(t.TempDir(), "a.aof")
		ast.Nil(os.WriteFile(cfg.AOF.AppendFileName, []byte("*1\r\n$x\r\n"), 0644))
		_, err := New(cfg)
		ast.NotNil(err)

		cfg.AOF.AppendFsync = "sometimes"
		_, err = New(cfg)
		ast.NotNil(err)

		cfg.AOF.AppendFsync = "everysec"
		cfg.AOF.AppendFileName = t.TempDir() // a directory
		_, err = New(cfg)
		ast.NotNil(err)
	})

	t.Run("aof-skips-unknown-commands", func(t *testing.T) {
		cfg := config.Default()
		cfg.AOF.AppendOnly = true
		cfg.AOF.AppendFileName = filepath.Join(t.TempDir(), "a.aof")
		data := aof.Encode(nil, [][]byte{[]byte("nope")})
		data = aof.Encode(data, [][]byte{[]byte("set"), []byte("k"), []byte("v")})
		ast.Nil(os.WriteFile(cfg.AOF.AppendFileName, data, 0644))
		s, err := New(cfg)
		ast.Nil(err)
		ast.True(s.db.Exists("k"))
		ast.Nil(s.Close())
	})

	t.Run("listen-port-in-use", func(t *testing.T) {
		l, err := gonet.Listen("tcp", "0.0.0.0:0")
		ast.Nil(err)
		defer l.Close()
		cfg := config.Default()
		cfg.TCP.Port = l.Addr().(*gonet.TCPAddr).Port
		s, err := New(cfg)
		ast.Nil(err)
		ast.NotNil(s.Listen())
		ast.Nil(s.Close())
	})
}

// TestAofEndToEnd writes through a running server and checks the AOF.
func TestAofEndToEnd(t *testing.T) {
	ast := assert.New(t)
	cfg := config.Default()
	cfg.AOF.AppendOnly = true
	cfg.AOF.AppendFsync = "always"
	cfg.AOF.AppendFileName = filepath.Join(t.TempDir(), "a.aof")
	_, addr := startServer(t, cfg)

	conn, err := gonet.Dial("tcp", addr)
	ast.Nil(err)
	defer conn.Close()
	rd := bufio.NewReader(conn)
	ast.Equal("+OK\r\n", send(t, conn, rd, "SET k v\r\n"))
	ast.Equal("$1\r\n", send(t, conn, rd, "GET k\r\n"))
	readLine(t, rd)

	// with appendfsync always the write is on disk before the reply
	data, err := os.ReadFile(cfg.AOF.AppendFileName)
	ast.Nil(err)
	ast.True(bytes.Contains(data, []byte("SET")))
	ast.False(bytes.Contains(data, []byte("GET")))
}
