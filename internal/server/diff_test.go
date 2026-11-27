package server

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xgzlucario/rotom/internal/config"
)

var (
	diffOps  = flag.Int("diff.ops", 20000, "commands per differential test run")
	diffSeed = flag.Uint64("diff.seed", 0, "seed for the differential test (0 = random)")
)

// gen produces random but well-formed commands over a small keyspace, so
// that keys collide often and every type/error path gets exercised.
type gen struct{ r *rand.Rand }

func (g gen) key() string    { return fmt.Sprint("k", g.r.IntN(8)) }
func (g gen) member() string { return fmt.Sprint("m", g.r.IntN(12)) }
func (g gen) idx() string    { return fmt.Sprint(g.r.IntN(14) - 7) }
func (g gen) score() string {
	return []string{"0", "1", "1.5", "-2", "100", "3"}[g.r.IntN(6)]
}

func (g gen) members(fn func() string) []any {
	var res []any
	for range 1 + g.r.IntN(3) {
		res = append(res, fn())
	}
	return res
}

func (g gen) command() []any {
	k := g.key()
	c := func(args ...any) []any { return args }
	switch g.r.IntN(30) {
	case 0:
		return c("set", k, g.member())
	case 1:
		switch g.r.IntN(4) {
		case 0:
			return c("set", k, g.idx(), "nx")
		case 1:
			return c("set", k, g.idx(), "xx")
		case 2:
			return c("set", k, g.idx(), "keepttl")
		default:
			return c("set", k, g.idx(), "ex", "100")
		}
	case 2:
		return c("get", k)
	case 3:
		return c("del", k, g.key())
	case 4:
		return c("exists", k, g.key())
	case 5:
		return c("incr", k)
	case 6:
		return c("type", k)
	case 7:
		return append(c("hset", k), g.members(func() string { return g.member() + " " + g.member() })...)
	case 8:
		return c("hget", k, g.member())
	case 9:
		return append(c("hdel", k), g.members(g.member)...)
	case 10:
		return c("hgetall", k)
	case 11:
		return append(c("lpush", k), g.members(g.member)...)
	case 12:
		return append(c("rpush", k), g.members(g.member)...)
	case 13:
		return c("lpop", k)
	case 14:
		return c("rpop", k)
	case 15, 16:
		return c("lrange", k, g.idx(), g.idx())
	case 17:
		return append(c("sadd", k), g.members(g.member)...)
	case 18:
		return append(c("srem", k), g.members(g.member)...)
	case 19:
		return c("smembers", k)
	case 20, 21:
		args := c("zadd", k)
		for range 1 + g.r.IntN(3) {
			args = append(args, g.score(), g.member())
		}
		return args
	case 22:
		return append(c("zrem", k), g.members(g.member)...)
	case 23:
		return c("zrank", k, g.member())
	case 24, 25:
		args := c("zrange", k, g.idx(), g.idx())
		if g.r.IntN(2) == 0 {
			args = append(args, "withscores")
		}
		return args
	case 26:
		// count 0 is not generated: redis returns an empty array, miniredis pops everything.
		return c("zpopmin", k, fmt.Sprint(1+g.r.IntN(2)))
	case 27:
		return c("expire", k, "1000")
	case 28:
		return c("persist", k)
	default:
		return c("ttl", k)
	}
}

// fixArgs expands "a b" pairs produced for hset.
func fixArgs(args []any) []any {
	if args[0] != "hset" {
		return args
	}
	res := args[:2:2]
	for _, a := range args[2:] {
		for p := range strings.FieldsSeq(a.(string)) {
			res = append(res, p)
		}
	}
	return res
}

// normalize renders a reply so that orderless replies compare equal and
// error texts (which differ between implementations) are reduced to their kind.
func normalize(cmd string, v any, err error) string {
	if err == redis.Nil {
		return "(nil)"
	}
	if err != nil {
		if strings.HasPrefix(err.Error(), "WRONGTYPE") {
			return "(error WRONGTYPE)"
		}
		return "(error)"
	}
	switch x := v.(type) {
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = fmt.Sprint(e)
		}
		switch cmd {
		case "smembers":
			slices.Sort(parts)
		case "hgetall":
			// sort field/value pairs
			pairs := make([]string, 0, len(parts)/2)
			for i := 0; i+1 < len(parts); i += 2 {
				pairs = append(pairs, parts[i]+"="+parts[i+1])
			}
			slices.Sort(pairs)
			parts = pairs
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprint(v)
}

// TestDifferential runs the same random commands against rotom and miniredis
// and compares every reply.
func TestDifferential(t *testing.T) {
	seed := *diffSeed
	if seed == 0 {
		seed = rand.Uint64()
	}
	t.Logf("seed %d (rerun with -diff.seed=%d)", seed, seed)

	cfg := config.Default()
	cfg.TCP.Port = 7981
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Run()

	mini := miniredis.RunT(t)
	ctx := context.Background()
	rotom := redis.NewClient(&redis.Options{Addr: "127.0.0.1:7981", Protocol: 2})
	ref := redis.NewClient(&redis.Options{Addr: mini.Addr(), Protocol: 2})
	defer rotom.Close()
	defer ref.Close()

	g := gen{rand.New(rand.NewPCG(seed, seed))}
	var history []string
	for i := range *diffOps {
		args := fixArgs(g.command())
		cmd := args[0].(string)
		history = append(history, strings.TrimSpace(fmt.Sprintln(args...)))

		v1, err1 := rotom.Do(ctx, args...).Result()
		v2, err2 := ref.Do(ctx, args...).Result()
		got, want := normalize(cmd, v1, err1), normalize(cmd, v2, err2)
		if got != want {
			start := max(0, len(history)-15)
			t.Fatalf("op %d: %v\n  rotom:     %s (%v)\n  miniredis: %s (%v)\nlast commands:\n  %s",
				i, args, got, err1, want, err2, strings.Join(history[start:], "\n  "))
		}
	}
}
