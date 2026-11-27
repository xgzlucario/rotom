package command

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
	"github.com/xgzlucario/rotom/internal/resp"
)

// tester runs commands against one in-memory db and returns raw replies.
type tester struct {
	t   *testing.T
	ctx *Context
}

func newTester(t *testing.T) *tester {
	cfg := config.Default()
	cfg.RDB.DBFileName = filepath.Join(t.TempDir(), "dump.rdb")
	return &tester{t: t, ctx: &Context{W: resp.NewWriter(256), DB: db.New(), Config: cfg}}
}

func (tt *tester) do(args ...string) string {
	tt.t.Helper()
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	tt.ctx.W.Reset()
	cmd := Lookup(argv[0])
	if cmd == nil {
		tt.t.Fatalf("unknown command %s", args[0])
	}
	Execute(cmd, tt.ctx, argv[1:])
	return string(tt.ctx.W.Bytes())
}

const (
	ok       = "+OK\r\n"
	null     = "$-1\r\n"
	empty    = "*0\r\n"
	wrongTyp = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
)

func isErr(s string) bool { return strings.HasPrefix(s, "-") }

func TestKeys(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	tt.do("set", "a", "1")
	tt.do("set", "b", "2")

	ast.Equal(":2\r\n", tt.do("exists", "a", "b", "c"))
	ast.Equal("+string\r\n", tt.do("type", "a"))
	ast.Equal("+none\r\n", tt.do("type", "c"))
	ast.Equal(":1\r\n", tt.do("del", "a", "c"))

	ast.Equal(":-1\r\n", tt.do("ttl", "b"))
	ast.Equal(":-2\r\n", tt.do("pttl", "c"))
	ast.Equal(":1\r\n", tt.do("expire", "b", "100"))
	ast.Equal(":100\r\n", tt.do("ttl", "b"))
	ast.Equal(":1\r\n", tt.do("expireat", "b", "99999999999"))
	ast.Equal(":1\r\n", tt.do("pexpire", "b", "100000"))
	ast.Equal(":1\r\n", tt.do("persist", "b"))
	ast.Equal(":0\r\n", tt.do("persist", "b"))
	ast.Equal(":0\r\n", tt.do("expire", "c", "100"))
	ast.True(isErr(tt.do("expire", "b", "x")))
	ast.True(isErr(tt.do("expire", "b", "999999999999999999")))
	ast.Equal(":1\r\n", tt.do("pexpireat", "b", "1"))
	ast.Equal(":0\r\n", tt.do("exists", "b"))

	// scan: options and errors
	for i := range 30 {
		tt.do("set", "k"+string(rune('a'+i)), "v")
	}
	tt.do("rpush", "list", "x")
	ast.Contains(tt.do("scan", "0", "type", "list", "count", "1000"), "list")
	ast.Contains(tt.do("scan", "0", "match", "ka", "count", "1000"), "$2\r\nka\r\n")
	for _, bad := range [][]string{
		{"scan", "x"}, {"scan", "0", "count"}, {"scan", "0", "count", "x"},
		{"scan", "0", "count", "0"}, {"scan", "0", "foo", "bar"},
	} {
		ast.True(isErr(tt.do(bad...)), bad)
	}
}

func TestStrings(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal(null, tt.do("get", "s"))
	ast.Equal(null, tt.do("set", "s", "v", "px", "100000", "xx")) // xx: key missing
	ast.Equal(ok, tt.do("set", "s", "v", "px", "100000"))
	ast.Equal(ok, tt.do("set", "s", "v2", "xx"))
	ast.Equal(null, tt.do("set", "n", "v", "xx"))
	ast.Equal(ok, tt.do("set", "n", "v", "nx", "keepttl"))
	ast.Equal(null, tt.do("set", "n", "v", "nx"))
	ast.Equal(ok, tt.do("set", "e", "v", "exat", "99999999999"))
	for _, bad := range [][]string{
		{"set", "s", "v", "ex"}, {"set", "s", "v", "ex", "x"}, {"set", "s", "v", "ex", "0"},
		{"set", "s", "v", "nx", "xx"}, {"set", "s", "v", "keepttl", "ex", "1"}, {"set", "s", "v", "foo"},
	} {
		ast.True(isErr(tt.do(bad...)), bad)
	}

	ast.Equal(":1\r\n", tt.do("incr", "i"))
	tt.do("set", "i", "9223372036854775807")
	ast.True(isErr(tt.do("incr", "i")))
	tt.do("set", "i", "abc")
	ast.True(isErr(tt.do("incr", "i")))
	tt.do("rpush", "l", "x")
	ast.Equal(wrongTyp, tt.do("incr", "l"))
	ast.Equal(wrongTyp, tt.do("get", "l"))
}

func TestHashes(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal(":2\r\n", tt.do("hset", "h", "a", "1", "b", "2"))
	ast.True(isErr(tt.do("hset", "h", "a", "1", "b")))
	ast.Equal("$1\r\n1\r\n", tt.do("hget", "h", "a"))
	ast.Equal(null, tt.do("hget", "h", "c"))
	ast.Equal(null, tt.do("hget", "none", "c"))
	ast.Equal("*4\r\n", tt.do("hgetall", "h")[:4])
	ast.Equal(empty, tt.do("hgetall", "none"))
	ast.Equal(":0\r\n", tt.do("hdel", "none", "a"))
	ast.Equal(":2\r\n", tt.do("hdel", "h", "a", "b", "c"))
	ast.Equal("+none\r\n", tt.do("type", "h"))

	tt.do("set", "s", "v")
	for _, c := range [][]string{{"hset", "s", "a", "1"}, {"hget", "s", "a"}, {"hdel", "s", "a"}, {"hgetall", "s"}} {
		ast.Equal(wrongTyp, tt.do(c...), c)
	}
}

func TestLists(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal(":2\r\n", tt.do("rpush", "l", "b", "c"))
	ast.Equal(":3\r\n", tt.do("lpush", "l", "a"))
	ast.Equal("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", tt.do("lrange", "l", "0", "-1"))
	ast.Equal(empty, tt.do("lrange", "l", "5", "9"))
	ast.Equal(empty, tt.do("lrange", "none", "0", "-1"))
	ast.True(isErr(tt.do("lrange", "l", "a", "1")))
	ast.Equal("$1\r\na\r\n", tt.do("lpop", "l"))
	ast.Equal("$1\r\nc\r\n", tt.do("rpop", "l"))
	ast.Equal("$1\r\nb\r\n", tt.do("rpop", "l"))
	ast.Equal(null, tt.do("lpop", "l"))

	tt.do("set", "s", "v")
	for _, c := range [][]string{{"lpush", "s", "a"}, {"lpop", "s"}, {"lrange", "s", "0", "1"}} {
		ast.Equal(wrongTyp, tt.do(c...), c)
	}
}

func TestSets(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal(":2\r\n", tt.do("sadd", "s", "a", "b"))
	ast.Equal(":0\r\n", tt.do("sadd", "s", "a"))
	ast.Equal("*2\r\n", tt.do("smembers", "s")[:4])
	ast.Equal(empty, tt.do("smembers", "none"))
	ast.Equal(":1\r\n", tt.do("srem", "s", "a", "z"))
	ast.Equal(":0\r\n", tt.do("srem", "none", "a"))
	ast.Equal("$1\r\nb\r\n", tt.do("spop", "s"))
	ast.Equal(null, tt.do("spop", "s"))

	// a long member converts to the hash set encoding
	tt.do("sadd", "big", strings.Repeat("x", db.MaxZipValueLen+1))
	ast.Equal("+set\r\n", tt.do("type", "big"))

	tt.do("set", "str", "v")
	for _, c := range [][]string{{"sadd", "str", "a"}, {"srem", "str", "a"}, {"smembers", "str"}, {"spop", "str"}} {
		ast.Equal(wrongTyp, tt.do(c...), c)
	}
}

func TestZSets(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal(":2\r\n", tt.do("zadd", "z", "1", "a", "2", "b"))
	ast.Equal(":0\r\n", tt.do("zadd", "z", "3", "a"))
	ast.Equal(":1\r\n", tt.do("zrank", "z", "a"))
	ast.Equal(null, tt.do("zrank", "z", "x"))
	ast.Equal(null, tt.do("zrank", "none", "x"))
	ast.Equal("*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n3\r\n", tt.do("zrange", "z", "0", "-1", "withscores"))
	ast.Equal(empty, tt.do("zrange", "z", "5", "9"))
	ast.Equal(empty, tt.do("zrange", "none", "0", "1"))
	ast.Equal(":1\r\n", tt.do("zrem", "z", "a", "x"))
	ast.Equal(":0\r\n", tt.do("zrem", "none", "a"))
	ast.Equal("*2\r\n$1\r\nb\r\n$1\r\n2\r\n", tt.do("zpopmin", "z", "5"))
	ast.Equal(empty, tt.do("zpopmin", "z"))
	ast.Equal(empty, tt.do("zpopmin", "z", "0"))

	ast.Equal(":1\r\n", tt.do("zadd", "inf", "+inf", "a"))
	ast.Equal(":1\r\n", tt.do("zadd", "inf", "-inf", "b"))
	for _, bad := range [][]string{
		{"zadd", "z", "1"}, {"zadd", "z", "x", "a"}, {"zadd", "z", "nan", "a"},
		{"zrange", "z", "a", "1"}, {"zrange", "z", "0", "1", "foo"},
		{"zpopmin", "z", "-1"}, {"zpopmin", "z", "1", "2"},
	} {
		ast.True(isErr(tt.do(bad...)), bad)
	}

	tt.do("set", "s", "v")
	for _, c := range [][]string{{"zadd", "s", "1", "a"}, {"zrank", "s", "a"}, {"zrem", "s", "a"}, {"zrange", "s", "0", "1"}, {"zpopmin", "s"}} {
		ast.Equal(wrongTyp, tt.do(c...), c)
	}
}

func TestServerCommands(t *testing.T) {
	ast := assert.New(t)
	tt := newTester(t)
	ast.Equal("+PONG\r\n", tt.do("ping"))
	ast.Equal("$2\r\nhi\r\n", tt.do("ping", "hi"))
	ast.True(isErr(tt.do("ping", "a", "b")))
	ast.Equal("*10\r\n", tt.do("hello")[:5])

	tt.do("set", "k", "v")
	ast.Equal(ok, tt.do("save"))
	ast.Equal(ok, tt.do("flushdb"))
	ast.Equal("+none\r\n", tt.do("type", "k"))
	ast.Equal(ok, tt.do("load"))
	ast.Equal("$1\r\nv\r\n", tt.do("get", "k"))

	ast.Equal("*2\r\n$4\r\nport\r\n$4\r\n6379\r\n", tt.do("config", "get", "port"))
	ast.Equal(empty, tt.do("config", "get", "nope"))
	ast.True(isErr(tt.do("config", "set", "port", "1")))
	ast.True(isErr(tt.do("config", "foo")))

	// save/load errors
	tt.ctx.Config.RDB.DBFileName = t.TempDir() // a directory
	ast.True(isErr(tt.do("save")))
	ast.True(isErr(tt.do("load")))

	ast.Contains(UnknownCommandError([]byte("x")), "unknown command 'x'")
}
