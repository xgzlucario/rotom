package command

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
	"github.com/xgzlucario/rotom/internal/resp"
)

func TestLookup(t *testing.T) {
	ast := assert.New(t)
	ast.Equal("set", Lookup([]byte("SeT")).Name)
	ast.Nil(Lookup([]byte("nope")))
	ast.Nil(Lookup(make([]byte, 100)))
	for name, cmd := range commands {
		ast.Equal(name, cmd.Name)
		ast.NotZero(cmd.arity, name)
	}
}

func TestNormalizeRange(t *testing.T) {
	ast := assert.New(t)
	cases := []struct {
		start, stop, length, from, to int
		ok                            bool
	}{
		{0, -1, 3, 0, 2, true},
		{0, 1, 3, 0, 1, true},
		{1, 100, 3, 1, 2, true},
		{-100, 0, 3, 0, 0, true},
		{3, 5, 3, 0, 0, false},
		{2, 1, 3, 0, 0, false},
		{0, -1, 0, 0, 0, false},
	}
	for _, c := range cases {
		from, to, ok := normalizeRange(c.start, c.stop, c.length)
		ast.Equal(c.ok, ok, "%+v", c)
		if ok {
			ast.Equal([2]int{c.from, c.to}, [2]int{from, to}, "%+v", c)
		}
	}
}

func TestToExpireAt(t *testing.T) {
	ast := assert.New(t)
	at, ok := toExpireAt("EX", 10, 1000)
	ast.True(ok)
	ast.Equal(int64(11000), at)
	at, _ = toExpireAt("PXAT", 5, 1000)
	ast.Equal(int64(5), at)
	_, ok = toExpireAt("EX", math.MaxInt64/10, 1000)
	ast.False(ok)
	_, ok = toExpireAt("PX", math.MaxInt64, 1000)
	ast.False(ok)
}

func run(d *db.DB, args ...string) (string, Result) {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	w := resp.NewWriter(64)
	res := Execute(Lookup(argv[0]), &Context{W: w, Args: argv[1:], DB: d, Config: config.Default()})
	return string(w.Bytes()), res
}

func TestExecute(t *testing.T) {
	ast := assert.New(t)
	d := db.New()

	out, res := run(d, "get")
	ast.Contains(out, "wrong number of arguments for 'get'")
	ast.False(res.Persist)

	out, res = run(d, "set", "k", "v")
	ast.Equal("+OK\r\n", out)
	ast.True(res.Persist)
	ast.Nil(res.Argv)

	_, res = run(d, "get", "k")
	ast.False(res.Persist, "read commands are never persisted")

	_, res = run(d, "zadd", "k", "1", "m")
	ast.False(res.Persist, "errors are never persisted")

	_, res = run(d, "del", "missing")
	ast.False(res.Persist, "no-op writes are not persisted")

	_, res = run(d, "set", "k", "v", "ex", "10")
	ast.Equal("pxat", string(res.Argv[3]))
}

func TestPanicRecovery(t *testing.T) {
	ast := assert.New(t)
	commands["boom"] = &Command{Name: "boom", arity: 1, handler: func(ctx *Context) {
		ctx.W.WriteArray(3)
		panic("bug")
	}}
	defer delete(commands, "boom")

	out, res := run(db.New(), "boom")
	ast.Equal("-ERR internal error in 'boom': bug\r\n", out, "partial reply must be discarded")
	ast.False(res.Persist)
}
