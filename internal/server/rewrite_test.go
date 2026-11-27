package server

import (
	"bufio"
	"fmt"
	gonet "net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/config"
)

func aofConfig(t *testing.T) *config.Config {
	cfg := config.Default()
	cfg.AOF.AppendOnly = true
	cfg.AOF.AppendFsync = "always" // on disk before the reply
	cfg.AOF.AppendDirName = t.TempDir()
	cfg.AOF.AutoRewritePercentage = 0
	return cfg
}

// writeBatch runs n rounds of INCR / RPUSH / SET starting at round from.
func writeBatch(t *testing.T, conn gonet.Conn, rd *bufio.Reader, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		send(t, conn, rd, "INCR counter\r\n")
		send(t, conn, rd, fmt.Sprintf("RPUSH list %d\r\n", i))
		send(t, conn, rd, fmt.Sprintf("SET key:%d val:%d EX 1000\r\n", i, i))
	}
}

// checkReload loads the AOF directory into a fresh server, as a restart
// after a crash at this instant would, and checks that n rounds are there.
func checkReload(t *testing.T, cfg *config.Config, n int) {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.aof.Close()
	v, _, _ := s.db.GetString("counter")
	assert.Equal(t, fmt.Sprint(n), string(v))
	l, _ := s.db.GetList("list")
	assert.Equal(t, n, l.Len())
	for i := range n {
		v, ok, _ := s.db.GetString(fmt.Sprintf("key:%d", i))
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("val:%d", i), string(v))
		at, _ := s.db.ExpireAt(fmt.Sprintf("key:%d", i))
		assert.NotZero(t, at, "ttl survives the rewrite")
	}
}

func aofFiles(t *testing.T, dir string) (bases, incrs int) {
	b, _ := filepath.Glob(filepath.Join(dir, "*.base.*"))
	i, _ := filepath.Glob(filepath.Join(dir, "*.incr.aof"))
	return len(b), len(i)
}

func TestBgRewriteAof(t *testing.T) {
	ast := assert.New(t)
	cfg := aofConfig(t)
	_, addr := startServer(t, cfg)
	conn, err := gonet.Dial("tcp", addr)
	ast.Nil(err)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	writeBatch(t, conn, rd, 0, 100)
	ast.Equal("+Background append only file rewriting started\r\n", send(t, conn, rd, "BGREWRITEAOF\r\n"))
	// writes racing with the background base write
	writeBatch(t, conn, rd, 100, 100)

	// reload only once a rewrite is finished: loading the directory runs a
	// cleanup that would race with the live server's in-flight base file.
	// (crashes in the middle of a rewrite are covered in the aof package.)
	waitRewrite(t, cfg.AOF.AppendDirName, 1)
	checkReload(t, cfg, 200)
	writeBatch(t, conn, rd, 200, 100)
	checkReload(t, cfg, 300)

	// a second rewrite on top of the first
	ast.Equal("+Background append only file rewriting started\r\n", send(t, conn, rd, "BGREWRITEAOF\r\n"))
	reply := send(t, conn, rd, "BGREWRITEAOF\r\n")
	ast.True(strings.HasPrefix(reply, "+Background") || strings.Contains(reply, "already in progress"), reply)
	writeBatch(t, conn, rd, 300, 10)
	waitRewrite(t, cfg.AOF.AppendDirName, 2)
	checkReload(t, cfg, 310)
}

// waitRewrite waits until the base with sequence seq is the only base and a
// single incr file is left.
func waitRewrite(t *testing.T, dir string, seq int) {
	t.Helper()
	assert.Eventually(t, func() bool {
		bases, incrs := aofFiles(t, dir)
		matches, _ := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*.%d.base.rdb", seq)))
		return bases == 1 && incrs == 1 && len(matches) == 1
	}, 5*time.Second, 10*time.Millisecond, "rewrite %d finishes and old files are removed", seq)
}

func TestAutoRewriteAof(t *testing.T) {
	ast := assert.New(t)
	cfg := aofConfig(t)
	cfg.AOF.AutoRewritePercentage = 100
	cfg.AOF.AutoRewriteMinSize = 4 << 10
	_, addr := startServer(t, cfg)
	conn, err := gonet.Dial("tcp", addr)
	ast.Nil(err)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	// overwriting one key over and over: the AOF grows, the data does not.
	const rounds, size = 1000, 100
	for range rounds {
		send(t, conn, rd, "SET k "+strings.Repeat("x", size)+"\r\n")
	}
	// automatic rewrites keep the AOF far below the ~130KB written
	ast.Eventually(func() bool {
		bases, incrs := aofFiles(t, cfg.AOF.AppendDirName)
		return bases == 1 && incrs == 1 && dirSize(t, cfg.AOF.AppendDirName) < rounds*size/10
	}, 5*time.Second, 10*time.Millisecond)
	checkKey := func() {
		s, err := New(cfg)
		ast.Nil(err)
		defer s.aof.Close()
		v, _, _ := s.db.GetString("k")
		ast.Equal(strings.Repeat("x", size), string(v))
	}
	checkKey()
}

func dirSize(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, e := range entries {
		if info, err := e.Info(); err == nil {
			total += int(info.Size())
		}
	}
	return total
}

func TestBgRewriteAofDisabled(t *testing.T) {
	_, addr := startServer(t, config.Default())
	conn, err := gonet.Dial("tcp", addr)
	assert.Nil(t, err)
	defer conn.Close()
	assert.Equal(t, "-ERR AOF is not enabled\r\n", send(t, conn, bufio.NewReader(conn), "BGREWRITEAOF\r\n"))
}
