package aof

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const name = "appendonly.aof"

// state is what loading an AOF yields: the base passed to loadRDB (its
// content) and the replayed commands.
type state struct {
	base string
	cmds []string
}

func load(t *testing.T, dir string) (*AOF, state) {
	t.Helper()
	a, err := Open(dir, name, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	var st state
	err = a.Load(func(path string) error {
		b, err := os.ReadFile(path)
		st.base = string(b)
		return err
	}, func(argv [][]byte) {
		var parts []string
		for _, arg := range argv {
			parts = append(parts, string(arg))
		}
		st.cmds = append(st.cmds, strings.Join(parts, " "))
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, st
}

func cmd(args ...string) [][]byte {
	res := make([][]byte, len(args))
	for i, a := range args {
		res[i] = []byte(a)
	}
	return res
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var res []string
	for _, e := range entries {
		res = append(res, e.Name())
	}
	slices.Sort(res)
	return res
}

func TestAppendAndLoad(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()

	a, st := load(t, dir)
	ast.Equal(state{}, st)
	ast.Equal([]string{"appendonly.aof.1.incr.aof", "appendonly.aof.manifest"}, files(t, dir))

	a.Append(cmd("set", "k", "v"))
	a.AppendRaw(Encode(nil, cmd("del", "k")))
	ast.Greater(a.Size(), int64(0))
	ast.Nil(a.Write())
	ast.Nil(a.Write()) // nothing buffered
	ast.Nil(a.Sync())
	ast.Nil(a.Close())

	a, st = load(t, dir)
	ast.Equal([]string{"set k v", "del k"}, st.cmds)
	ast.Equal(a.Size(), a.SizeAfterRewrite())
	ast.Nil(a.Close())
}

func TestFsyncAlways(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, err := Open(dir, name, FsyncAlways)
	ast.Nil(err)
	ast.Nil(a.Load(nil, func([][]byte) {}))
	ast.Equal(FsyncAlways, a.Policy())
	a.Append(cmd("ping"))
	ast.Nil(a.Write())
	data, _ := os.ReadFile(filepath.Join(dir, "appendonly.aof.1.incr.aof"))
	ast.Equal("*1\r\n$4\r\nping\r\n", string(data))
	ast.Nil(a.Close())
}

func TestRewrite(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, _ := load(t, dir)
	a.Append(cmd("set", "a", "1"))

	// hold the background write, to look at the state of a crash before
	// the new base is durable.
	gate := make(chan struct{})
	writeBase = func(path string, data []byte) error { <-gate; return defaultWriteBase(path, data) }
	defer func() { writeBase = defaultWriteBase }()

	ast.Nil(a.StartRewrite([]byte("SNAPSHOT-1")))
	ast.True(a.Rewriting())
	ast.ErrorIs(a.StartRewrite(nil), ErrRewriteInProgress)

	// written after the snapshot: must survive in the new incr file
	a.Append(cmd("set", "b", "2"))
	ast.Nil(a.Write())

	// a crash before the base is durable loads old base + all incrs
	crashed, st := load(t, dir)
	ast.Equal(state{cmds: []string{"set a 1", "set b 2"}}, st)
	_ = crashed.file.Close()
	close(gate)

	for {
		done, err := a.PollRewrite()
		ast.Nil(err)
		if done {
			break
		}
	}
	ast.False(a.Rewriting())
	ast.Equal([]string{
		"appendonly.aof.1.base.rdb", "appendonly.aof.2.incr.aof", "appendonly.aof.manifest",
	}, files(t, dir))
	ast.Equal(a.Size(), a.SizeAfterRewrite())
	ast.Nil(a.Close())

	_, st = load(t, dir)
	ast.Equal(state{base: "SNAPSHOT-1", cmds: []string{"set b 2"}}, st)

	// a second rewrite replaces the base
	a, _ = load(t, dir)
	ast.Nil(a.StartRewrite([]byte("SNAPSHOT-2")))
	ast.Nil(a.Close()) // waits for the rewrite
	_, st = load(t, dir)
	ast.Equal(state{base: "SNAPSHOT-2"}, st)
	ast.Equal([]string{
		"appendonly.aof.2.base.rdb", "appendonly.aof.3.incr.aof", "appendonly.aof.manifest",
	}, files(t, dir))

	done, err := (&AOF{}).PollRewrite()
	ast.False(done)
	ast.Nil(err)
}

func TestRewriteFailure(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, _ := load(t, dir)
	a.Append(cmd("set", "a", "1"))

	writeBase = func(string, []byte) error { return errors.New("disk full") }
	defer func() { writeBase = defaultWriteBase }()

	ast.Nil(a.StartRewrite([]byte("SNAPSHOT")))
	a.Append(cmd("set", "b", "2"))
	ast.ErrorContains(a.Close(), "disk full")

	// nothing is lost: the old incr file and the new one are both kept
	_, st := load(t, dir)
	ast.Equal(state{cmds: []string{"set a 1", "set b 2"}}, st)

	// the next rewrite succeeds and drops both
	writeBase = defaultWriteBase
	a, _ = load(t, dir)
	ast.Nil(a.StartRewrite([]byte("SNAPSHOT")))
	ast.Nil(a.Close())
	_, st = load(t, dir)
	ast.Equal(state{base: "SNAPSHOT"}, st)
	ast.Len(files(t, dir), 3)
}

func TestTruncatedTail(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, _ := load(t, dir)
	a.Append(cmd("set", "a", "1"))
	ast.Nil(a.Close())

	// a crash in the middle of a write
	path := filepath.Join(dir, "appendonly.aof.1.incr.aof")
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.WriteString("*2\r\n$3\r\nde")
	_ = f.Close()

	a, st := load(t, dir)
	ast.Equal([]string{"set a 1"}, st.cmds)
	a.Append(cmd("set", "b", "2")) // appended after the repaired end
	ast.Nil(a.Close())
	_, st = load(t, dir)
	ast.Equal([]string{"set a 1", "set b 2"}, st.cmds)
}

func TestCorruption(t *testing.T) {
	ast := assert.New(t)

	t.Run("corrupted-incr", func(t *testing.T) {
		dir := t.TempDir()
		a, _ := load(t, dir)
		ast.Nil(a.Close())
		ast.Nil(os.WriteFile(filepath.Join(dir, "appendonly.aof.1.incr.aof"), []byte("*1\r\n$x\r\n"), 0644))
		a, err := Open(dir, name, FsyncNo)
		ast.Nil(err)
		ast.ErrorContains(a.Load(nil, func([][]byte) {}), "corrupted")
	})

	t.Run("truncated-incr-not-last", func(t *testing.T) {
		dir := t.TempDir()
		a, _ := load(t, dir)
		writeBase = func(string, []byte) error { return errors.New("fail") }
		ast.Nil(a.StartRewrite(nil)) // leaves two incr files
		ast.NotNil(a.Close())
		writeBase = defaultWriteBase
		ast.Nil(os.WriteFile(filepath.Join(dir, "appendonly.aof.1.incr.aof"), []byte("*1\r\n$4\r\npi"), 0644))
		a, err := Open(dir, name, FsyncNo)
		ast.Nil(err)
		ast.ErrorIs(a.Load(nil, func([][]byte) {}), errTruncated)
	})

	t.Run("bad-base", func(t *testing.T) {
		dir := t.TempDir()
		a, _ := load(t, dir)
		ast.Nil(a.StartRewrite([]byte("x")))
		ast.Nil(a.Close())
		a, err := Open(dir, name, FsyncNo)
		ast.Nil(err)
		ast.ErrorContains(a.Load(func(string) error { return errors.New("bad rdb") }, nil), "bad rdb")
	})

	t.Run("missing-file", func(t *testing.T) {
		dir := t.TempDir()
		a, _ := load(t, dir)
		ast.Nil(a.Close())
		ast.Nil(os.Remove(filepath.Join(dir, "appendonly.aof.1.incr.aof")))
		_, err := Open(dir, name, FsyncNo)
		ast.NotNil(err)
	})

	t.Run("bad-manifest", func(t *testing.T) {
		for _, content := range []string{
			"file x seq 1",           // odd fields
			"file x seq 0 type i",    // bad seq
			"file x seq 1 type z",    // bad type
			"file ../x seq 1 type i", // path
			"seq 1 type i",           // missing name
			"",                       // no incr
			"file a seq 2 type i\nfile b seq 1 type i",   // seq order
			"file a seq 1 type i\nfile b seq 1 type b",   // base after incr
			"file b seq 1 type b\nfile c seq 2 type b\n", // two bases
		} {
			dir := t.TempDir()
			ast.Nil(os.WriteFile(filepath.Join(dir, name+".manifest"), []byte(content), 0644))
			_, err := Open(dir, name, FsyncNo)
			ast.ErrorIs(err, errBadManifest, content)
		}
	})

	t.Run("not-a-dir", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "f")
		ast.Nil(os.WriteFile(file, nil, 0644))
		_, err := Open(file, name, FsyncNo)
		ast.NotNil(err)
	})
}

func TestManifestCommentsAndOrder(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	ast.Nil(os.WriteFile(filepath.Join(dir, "x.1.incr.aof"), nil, 0644))
	ast.Nil(os.WriteFile(filepath.Join(dir, "x.manifest"), []byte("# comment\n\ntype i seq 1 file x.1.incr.aof\n"), 0644))
	a, err := Open(dir, "x", FsyncNo)
	ast.Nil(err)
	ast.Equal("x.1.incr.aof", a.m.incrs[0].name)
}

func TestLegacyUpgrade(t *testing.T) {
	ast := assert.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "appendonlydir")
	ast.Nil(os.WriteFile(filepath.Join(root, name), Encode(nil, cmd("set", "old", "1")), 0644))

	a, st := load(t, dir)
	ast.Equal([]string{"set old 1"}, st.cmds)
	_, err := os.Stat(filepath.Join(root, name))
	ast.True(os.IsNotExist(err), "legacy file moved")
	a.Append(cmd("set", "new", "2"))
	ast.Nil(a.Close())

	_, st = load(t, dir)
	ast.Equal([]string{"set old 1", "set new 2"}, st.cmds)
}

// A crash after moving the legacy file in but before writing the manifest
// must not lose it.
func TestLegacyUpgradeCrash(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	ast.Nil(os.WriteFile(filepath.Join(dir, name+".1.base.aof"), Encode(nil, cmd("set", "old", "1")), 0644))
	_, st := load(t, dir)
	ast.Equal([]string{"set old 1"}, st.cmds)
}

func TestRemoveUnlisted(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, _ := load(t, dir)
	ast.Nil(a.Close())
	for _, f := range []string{name + ".7.base.rdb", name + ".9.incr.aof", name + ".manifest.tmp", "unrelated.txt"} {
		ast.Nil(os.WriteFile(filepath.Join(dir, f), nil, 0644))
	}
	a, _ = load(t, dir)
	ast.Nil(a.Close())
	ast.Equal([]string{"appendonly.aof.1.incr.aof", "appendonly.aof.manifest", "unrelated.txt"}, files(t, dir))
}

func TestBigCommand(t *testing.T) {
	ast := assert.New(t)
	dir := t.TempDir()
	a, _ := load(t, dir)
	a.Append(cmd("set", "k", strings.Repeat("x", 1<<20))) // larger than the read buffer
	ast.Nil(a.Close())
	_, st := load(t, dir)
	ast.Len(st.cmds, 1)
	ast.Len(st.cmds[0], len("set k ")+1<<20)
}

func TestParseFsyncPolicy(t *testing.T) {
	ast := assert.New(t)
	for s, want := range map[string]FsyncPolicy{"always": FsyncAlways, "everysec": FsyncEverySec, "": FsyncEverySec, "no": FsyncNo} {
		p, err := ParseFsyncPolicy(s)
		ast.Nil(err)
		ast.Equal(want, p)
	}
	_, err := ParseFsyncPolicy("sometimes")
	ast.NotNil(err)
}
