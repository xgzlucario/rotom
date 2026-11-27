package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/command"
)

// testRegression covers bugs fixed in the overhaul. It runs against both
// miniredis and rotom so that miniredis acts as the reference behaviour.
func testRegression(t *testing.T, testType string, rdb *redis.Client) {
	ast := assert.New(t)
	ctx := context.Background()

	t.Run("set-overwrites-ttl-key", func(t *testing.T) {
		rdb.Set(ctx, "reg-ttl", "v1", 100*time.Second)
		rdb.Set(ctx, "reg-ttl", "v2", 0)
		res, err := rdb.Get(ctx, "reg-ttl").Result()
		ast.Nil(err)
		ast.Equal("v2", res)
	})

	t.Run("set-xx", func(t *testing.T) {
		ok, _ := rdb.SetXX(ctx, "reg-xx", "v", 0).Result()
		ast.False(ok)
		rdb.Set(ctx, "reg-xx", "v", 0)
		ok, _ = rdb.SetXX(ctx, "reg-xx", "v2", 0).Result()
		ast.True(ok)
	})

	t.Run("set-invalid-expire", func(t *testing.T) {
		_, err := rdb.Do(ctx, "set", "reg-ex", "v", "ex", "abc").Result()
		ast.Equal(command.ErrParseInteger.Error(), err.Error())
		_, err = rdb.Do(ctx, "set", "reg-ex", "v", "ex", "0").Result()
		ast.NotNil(err)
		_, err = rdb.Get(ctx, "reg-ex").Result()
		ast.Equal(redis.Nil, err)
	})

	t.Run("incr-grows-digits", func(t *testing.T) {
		rdb.Set(ctx, "reg-incr", "9", 0)
		n, _ := rdb.Incr(ctx, "reg-incr").Result()
		ast.Equal(int64(10), n)
		res, _ := rdb.Get(ctx, "reg-incr").Result()
		ast.Equal("10", res)
	})

	t.Run("zrange-bounds", func(t *testing.T) {
		rdb.Del(ctx, "reg-z")
		rdb.ZAdd(ctx, "reg-z",
			redis.Z{Member: "a", Score: 1},
			redis.Z{Member: "b", Score: 2},
			redis.Z{Member: "c", Score: 3})
		cases := []struct {
			start, stop int64
			want        []string
		}{
			{0, 1, []string{"a", "b"}},
			{0, -2, []string{"a", "b"}},
			{-2, -1, []string{"b", "c"}},
			{1, 100, []string{"b", "c"}},
			{3, 5, []string{}},
			{-100, 0, []string{"a"}},
		}
		for _, c := range cases {
			res, err := rdb.ZRange(ctx, "reg-z", c.start, c.stop).Result()
			ast.Nil(err)
			ast.Equal(c.want, res, "zrange %d %d", c.start, c.stop)
		}
		z, _ := rdb.ZRangeWithScores(ctx, "reg-z", 0, 0).Result()
		ast.Equal([]redis.Z{{Member: "a", Score: 1}}, z)
	})

	t.Run("lrange-past-end", func(t *testing.T) {
		rdb.Del(ctx, "reg-l")
		rdb.RPush(ctx, "reg-l", "1", "2", "3")
		res, err := rdb.LRange(ctx, "reg-l", 3, 10).Result()
		ast.Nil(err)
		ast.Equal([]string{}, res)
		res, _ = rdb.LRange(ctx, "reg-l", 2, 10).Result()
		ast.Equal([]string{"3"}, res)
	})

	t.Run("bad-args-do-not-crash", func(t *testing.T) {
		_, err := rdb.Do(ctx, "zadd", "reg-bad", "1", "a", "2").Result()
		ast.NotNil(err)
		_, err = rdb.Do(ctx, "zadd", "reg-bad", "x", "a").Result()
		ast.NotNil(err)
		_, err = rdb.Do(ctx, "lrange", "reg-bad", "a", "b").Result()
		ast.NotNil(err)
		res, err := rdb.Ping(ctx).Result()
		ast.Nil(err)
		ast.Equal("PONG", res)
	})

	t.Run("empty-containers-removed", func(t *testing.T) {
		rdb.RPush(ctx, "reg-el", "1")
		rdb.LPop(ctx, "reg-el")
		rdb.SAdd(ctx, "reg-es", "1")
		rdb.SRem(ctx, "reg-es", "1")
		rdb.HSet(ctx, "reg-eh", "f", "v")
		rdb.HDel(ctx, "reg-eh", "f")
		rdb.ZAdd(ctx, "reg-ez", redis.Z{Member: "m", Score: 1})
		rdb.ZPopMin(ctx, "reg-ez")
		for _, key := range []string{"reg-el", "reg-es", "reg-eh", "reg-ez"} {
			typ, _ := rdb.Type(ctx, key).Result()
			ast.Equal("none", typ, key)
		}
	})

	if testType != testTypeRotom {
		return
	}

	t.Run("config-get", func(t *testing.T) {
		res, err := rdb.ConfigGet(ctx, "port").Result()
		ast.Nil(err)
		ast.Equal(map[string]string{"port": "7979"}, res)
		res, _ = rdb.ConfigGet(ctx, "not-exist").Result()
		ast.Empty(res)
	})

	t.Run("rdb-loaded-values-are-mutable", func(t *testing.T) {
		rdb.FlushDB(ctx)
		rdb.Set(ctx, "reg-rdb-str", "41", 0)
		rdb.HSet(ctx, "reg-rdb-hash", "f", "aaaa")
		rdb.SAdd(ctx, "reg-rdb-set", "a", "b")
		rdb.ZAdd(ctx, "reg-rdb-zset", redis.Z{Member: "a", Score: 1}, redis.Z{Member: "b", Score: 2})
		rdb.RPush(ctx, "reg-rdb-list", "a", "b")
		ast.Nil(rdb.Save(ctx).Err())
		ast.Nil(rdb.Do(ctx, "load").Err())

		// each of these mutates data that used to alias a read-only mmap.
		n, err := rdb.Incr(ctx, "reg-rdb-str").Result()
		ast.Nil(err)
		ast.Equal(int64(42), n)
		ast.Nil(rdb.HSet(ctx, "reg-rdb-hash", "f", "bbbb").Err())
		v, _ := rdb.HGet(ctx, "reg-rdb-hash", "f").Result()
		ast.Equal("bbbb", v)
		c, _ := rdb.SRem(ctx, "reg-rdb-set", "a").Result()
		ast.Equal(int64(1), c)
		c, _ = rdb.ZRem(ctx, "reg-rdb-zset", "a").Result()
		ast.Equal(int64(1), c)
		s, _ := rdb.LPop(ctx, "reg-rdb-list").Result()
		ast.Equal("a", s)
	})

	t.Run("large-reply", func(t *testing.T) {
		// much larger than a socket send buffer: exercises partial writes.
		body := bytes.Repeat([]byte("0123456789abcdef"), 4*MB/16)
		ast.Nil(rdb.Set(ctx, "reg-big", body, 0).Err())
		for range 3 {
			res, err := rdb.Get(ctx, "reg-big").Bytes()
			ast.Nil(err)
			ast.True(bytes.Equal(body, res))
		}
		rdb.Del(ctx, "reg-big")
	})

	t.Run("split-and-pipelined-input", func(t *testing.T) {
		conn, err := net.Dial("tcp", "127.0.0.1:7979")
		ast.Nil(err)
		defer conn.Close()
		rd := bufio.NewReader(conn)

		raw := "*3\r\n$3\r\nSET\r\n$5\r\nsplit\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$5\r\nsplit\r\n"
		// send byte by byte, a command never arrives in one read.
		for i := 0; i < len(raw); i++ {
			_, err = conn.Write([]byte{raw[i]})
			ast.Nil(err)
			if i%7 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
		ast.Equal("+OK\r\n", readLine(t, rd))
		ast.Equal("$5\r\n", readLine(t, rd))
		ast.Equal("hello\r\n", readLine(t, rd))

		// many pipelined commands in one write.
		var sb strings.Builder
		for range 1000 {
			sb.WriteString("*1\r\n$4\r\nPING\r\n")
		}
		_, err = conn.Write([]byte(sb.String()))
		ast.Nil(err)
		for range 1000 {
			ast.Equal("+PONG\r\n", readLine(t, rd))
		}
	})

	t.Run("protocol-error-closes-connection", func(t *testing.T) {
		conn, err := net.Dial("tcp", "127.0.0.1:7979")
		ast.Nil(err)
		defer conn.Close()
		_, _ = conn.Write([]byte("*1\r\n$abc\r\n"))
		rd := bufio.NewReader(conn)
		line := readLine(t, rd)
		ast.True(strings.HasPrefix(line, "-ERR Protocol error"), line)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = rd.ReadByte()
		ast.Equal(io.EOF, err)
	})
}

func readLine(t *testing.T, rd *bufio.Reader) string {
	t.Helper()
	line, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return line
}

func testExpire(t *testing.T, rdb *redis.Client, sleepFn func(time.Duration)) {
	ast := assert.New(t)
	ctx := context.Background()

	rdb.Set(ctx, "exp", "v", 0)
	ttl, _ := rdb.TTL(ctx, "exp").Result()
	ast.Equal(time.Duration(-1), ttl)
	ttl, _ = rdb.TTL(ctx, "exp-none").Result()
	ast.Equal(time.Duration(-2), ttl)

	ok, _ := rdb.Expire(ctx, "exp", 100*time.Second).Result()
	ast.True(ok)
	ttl, _ = rdb.TTL(ctx, "exp").Result()
	ast.Equal(100*time.Second, ttl)
	pttl, _ := rdb.PTTL(ctx, "exp").Result()
	ast.InDelta(100*time.Second, pttl, float64(time.Second))

	ok, _ = rdb.Expire(ctx, "exp-none", time.Second).Result()
	ast.False(ok)

	ok, _ = rdb.Persist(ctx, "exp").Result()
	ast.True(ok)
	ttl, _ = rdb.TTL(ctx, "exp").Result()
	ast.Equal(time.Duration(-1), ttl)

	// SET without KEEPTTL clears the TTL, with KEEPTTL keeps it.
	rdb.Expire(ctx, "exp", 100*time.Second)
	rdb.SetArgs(ctx, "exp", "v2", redis.SetArgs{KeepTTL: true})
	ttl, _ = rdb.TTL(ctx, "exp").Result()
	ast.Equal(100*time.Second, ttl)
	rdb.Set(ctx, "exp", "v3", 0)
	ttl, _ = rdb.TTL(ctx, "exp").Result()
	ast.Equal(time.Duration(-1), ttl)

	// INCR keeps the TTL.
	rdb.Set(ctx, "exp-n", "1", 100*time.Second)
	rdb.Incr(ctx, "exp-n")
	ttl, _ = rdb.TTL(ctx, "exp-n").Result()
	ast.Equal(100*time.Second, ttl)

	// PEXPIREAT in the past deletes the key.
	ok, _ = rdb.PExpireAt(ctx, "exp", time.Now().Add(-time.Hour)).Result()
	ast.True(ok)
	n, _ := rdb.Exists(ctx, "exp").Result()
	ast.Equal(int64(0), n)

	// expire on a container
	rdb.RPush(ctx, "exp-l", "a")
	rdb.PExpire(ctx, "exp-l", 50*time.Millisecond)
	sleepFn(100 * time.Millisecond)
	n, _ = rdb.Exists(ctx, "exp-l").Result()
	ast.Equal(int64(0), n)

	// SET EXAT / PXAT
	at := time.Now().Add(time.Hour)
	rdb.SetArgs(ctx, "exp-at", "v", redis.SetArgs{ExpireAt: at})
	ttl, _ = rdb.TTL(ctx, "exp-at").Result()
	ast.InDelta(time.Hour, ttl, float64(2*time.Second))
}
