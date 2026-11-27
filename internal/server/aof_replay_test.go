package server

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/aof"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/resp"
)

// replay runs cmds on a server with an AOF, waits for delay, then starts a
// second server that loads only that AOF. It returns both dicts and the AOF.
func replay(t *testing.T, delay time.Duration, cmds [][]string) (live, replayed *db.Dict, aofData []byte) {
	t.Helper()
	cfg := config.Default()
	cfg.AOF.AppendOnly = true
	cfg.AOF.AppendFsync = "no"
	cfg.AOF.AppendFileName = filepath.Join(t.TempDir(), "replay.aof")

	s1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := s1.newContext(resp.NewWriter(KB))
	for _, cmd := range cmds {
		args := make([][]byte, len(cmd))
		for i, a := range cmd {
			args[i] = []byte(a)
		}
		s1.processCommand(ctx, args, aof.Encode(nil, args))
		ctx.W.Reset()
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(delay)

	s2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	aofData, _ = os.ReadFile(cfg.AOF.AppendFileName)
	return s1.db.Dict, s2.db.Dict, aofData
}

func TestAofReplay(t *testing.T) {
	ast := assert.New(t)

	t.Run("relative-ttl-is-absolute", func(t *testing.T) {
		live, replayed, data := replay(t, 50*time.Millisecond, [][]string{
			{"set", "k1", "v", "ex", "100"},
			{"set", "k2", "v"},
			{"expire", "k2", "100"},
			{"set", "k3", "v"},
			{"pexpire", "k3", "100000"},
		})
		for _, key := range []string{"k1", "k2", "k3"} {
			at1, ok1 := live.ExpireAt(key)
			at2, ok2 := replayed.ExpireAt(key)
			ast.True(ok1 && ok2, key)
			ast.NotZero(at1, key)
			// a relative TTL would be re-based on replay, ~50ms later.
			ast.Equal(at1, at2, key)
		}
		ast.NotContains(string(data), "\r\nex\r\n")
		ast.Contains(string(data), "pxat")
		ast.Contains(string(data), "pexpireat")
	})

	t.Run("replay-equals-live", func(t *testing.T) {
		live, replayed, data := replay(t, 0, [][]string{
			{"set", "a", "1", "px", "100000"},
			{"incr", "a"},
			{"sadd", "s", "x", "y", "z", "w"},
			{"spop", "s"}, // random: persisted as SREM of the popped member
			{"spop", "s"},
			{"rpush", "l", "1", "2", "3"},
			{"lpop", "l"},
			{"hset", "h", "f", "v"},
			{"zadd", "z", "1", "m"},
			{"expire", "h", "1000"},
			{"pexpireat", "z", "1"},    // in the past: persisted as DEL
			{"set", "a", "x", "nx"},    // not applied
			{"zadd", "z2", "bad", "m"}, // error
			{"persist", "nothing"},     // no-op
		})
		ast.Equal(dump(live), dump(replayed))
		ast.NotContains(string(data), "spop")
		ast.NotContains(string(data), "bad")
		ast.NotContains(string(data), "\r\nnx\r\n")
		ast.NotContains(string(data), "persist")
	})

	t.Run("aof-wins-over-rdb", func(t *testing.T) {
		// with both enabled, loading RDB and then AOF would apply commands twice.
		dir := t.TempDir()
		cfg := testConfig(dir)
		cfg.TCP.Port = 0
		s1, _ := New(cfg)
		args := [][]byte{[]byte("rpush"), []byte("l"), []byte("a")}
		s1.processCommand(s1.newContext(resp.NewWriter(KB)), args, aof.Encode(nil, args))
		ast.Nil(db.SaveRDB(cfg.RDB.DBFileName, s1.db.Dict))
		ast.Nil(s1.Close())

		s2, err := New(cfg)
		ast.Nil(err)
		defer s2.Close()
		ls, _ := s2.db.GetList("l")
		ast.Equal(1, ls.Len())
	})
}

// dump renders a dict as comparable values.
func dump(d *db.Dict) map[string]any {
	res := map[string]any{}
	for it := range d.All() {
		var val any
		switch x := it.Value.(type) {
		case []byte:
			val = string(x)
		case db.Set:
			val = slices.Sorted(x.All())
		case *hash.ZipMap:
			m := map[string]string{}
			for f, v := range x.All() {
				m[f] = string(v)
			}
			val = m
		case *list.QuickList:
			var l []string
			for v := range x.All() {
				l = append(l, string(v))
			}
			val = l
		case db.ZSet:
			m := maps.Collect(x.All())
			val = m
		}
		res[it.Key] = []any{val, it.ExpireAt}
	}
	return res
}
