// Package aof implements the append only file as a redis 7 style
// multi-part AOF: a directory with a base snapshot, incr files holding the
// commands written since, and a manifest naming the current set.
//
//	appendonlydir/
//	  appendonly.aof.manifest
//	  appendonly.aof.3.base.rdb   snapshot taken by the last rewrite
//	  appendonly.aof.7.incr.aof   commands since that snapshot
//
// A rewrite needs no fork: the server encodes the snapshot synchronously and,
// at the same instant, switches writes to a new incr file. The snapshot is
// then written to disk in the background; once it is durable the manifest
// switches to it and the files it replaces are deleted.
package aof

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/xgzlucario/rotom/internal/fileutil"
	"github.com/xgzlucario/rotom/internal/resp"
)

// FsyncPolicy mirrors redis `appendfsync`.
type FsyncPolicy int

const (
	FsyncEverySec FsyncPolicy = iota
	FsyncAlways
	FsyncNo
)

func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "", "everysec":
		return FsyncEverySec, nil
	case "always":
		return FsyncAlways, nil
	case "no":
		return FsyncNo, nil
	}
	return 0, fmt.Errorf("invalid appendfsync %q, want always|everysec|no", s)
}

var ErrRewriteInProgress = errors.New("ERR Background append only file rewriting already in progress")

// writeBase writes a base file; tests replace it to simulate failures.
var (
	defaultWriteBase = fileutil.WriteAtomic
	writeBase        = defaultWriteBase
)

// AOF is the append only file of one server. All methods except the
// background base write run on the event loop goroutine.
type AOF struct {
	dir    string
	name   string // file name prefix, e.g. "appendonly.aof"
	policy FsyncPolicy

	m        *manifest
	file     *os.File // last incr file, opened for append
	buf      bytes.Buffer
	baseSize int64
	incrSize int64 // all incr files in the manifest

	// sizeAfterRewrite is the AOF size after the last rewrite (or load),
	// the reference for automatic rewrites.
	sizeAfterRewrite int64

	rewrite *rewriteState // non-nil while a rewrite is in progress
}

type rewriteState struct {
	base     fileInfo
	baseSize int64
	keepFrom int // incrs[keepFrom:] were written after the snapshot
	done     chan error
}

// Open opens (or creates) the AOF in dir. Call Load before appending.
func Open(dir, name string, policy FsyncPolicy) (*AOF, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	a := &AOF{dir: dir, name: name, policy: policy}

	m, err := loadManifest(a.path(name + ".manifest"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		if m, err = a.create(); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}
	a.m = m

	for _, f := range m.files() {
		st, err := os.Stat(a.path(f.name))
		if err != nil {
			return nil, fmt.Errorf("aof file in manifest: %w", err)
		}
		if f.typ == typeBase {
			a.baseSize = st.Size()
		} else {
			a.incrSize += st.Size()
		}
	}
	a.removeUnlisted()
	return a, nil
}

// create writes the first manifest. A legacy single-file AOF next to dir
// (the pre multi-part layout) is moved in as the base, like redis 7 does.
func (a *AOF) create() (*manifest, error) {
	m := &manifest{incrs: []fileInfo{{name: a.incrName(1), seq: 1, typ: typeIncr}}}
	base := fileInfo{name: a.name + ".1.base.aof", seq: 1, typ: typeBase}
	legacy := filepath.Join(filepath.Dir(a.dir), a.name)
	if st, err := os.Stat(legacy); err == nil && st.Mode().IsRegular() {
		if err := os.Rename(legacy, a.path(base.name)); err != nil {
			return nil, err
		}
		log.Warn().Msgf("aof: moved %s into %s as the base of a multi-part AOF", legacy, a.dir)
	}
	// also adopt a base moved in by a previous run that crashed before
	// writing the manifest, which would otherwise be lost.
	if _, err := os.Stat(a.path(base.name)); err == nil {
		m.base = &base
	}
	f, err := os.OpenFile(a.path(m.incrs[0].name), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	return m, m.save(a.path(a.name + ".manifest"))
}

// removeUnlisted deletes our files that the manifest does not reference:
// leftovers of a crash between a manifest switch and the cleanup.
func (a *AOF) removeUnlisted() {
	listed := map[string]bool{a.name + ".manifest": true}
	for _, f := range a.m.files() {
		listed[f.name] = true
	}
	entries, _ := os.ReadDir(a.dir)
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, a.name+".") && !listed[name] {
			_ = os.Remove(a.path(name))
		}
	}
}

func (a *AOF) path(name string) string { return filepath.Join(a.dir, name) }

func (a *AOF) incrName(seq int64) string {
	return a.name + "." + strconv.FormatInt(seq, 10) + ".incr.aof"
}

func (a *AOF) baseName(seq int64) string {
	return a.name + "." + strconv.FormatInt(seq, 10) + ".base.rdb"
}

// Load loads the base with loadRDB (or replays it, for a legacy aof-format
// base), replays every incr file, then opens the last one for appending.
//
// A command cut short at the end of the last incr file, which a crash in
// the middle of a write leaves behind, is truncated with a warning, like
// redis' aof-load-truncated. Corruption anywhere else is an error.
func (a *AOF) Load(loadRDB func(path string) error, replay func(argv [][]byte)) error {
	if b := a.m.base; b != nil {
		var err error
		if b.isRDB() {
			err = loadRDB(a.path(b.name))
		} else {
			_, err = readFile(a.path(b.name), replay)
		}
		if err != nil {
			return fmt.Errorf("load aof base %s: %w", b.name, err)
		}
	}
	for i, f := range a.m.incrs {
		valid, err := readFile(a.path(f.name), replay)
		if errors.Is(err, errTruncated) && i == len(a.m.incrs)-1 {
			log.Warn().Msgf("aof: %s ends with an incomplete command, truncating to %d bytes", f.name, valid)
			var st os.FileInfo
			if st, err = os.Stat(a.path(f.name)); err == nil {
				if err = os.Truncate(a.path(f.name), valid); err == nil {
					a.incrSize -= st.Size() - valid
				}
			}
		}
		if err != nil {
			return fmt.Errorf("load aof %s: %w", f.name, err)
		}
	}
	last := a.m.incrs[len(a.m.incrs)-1]
	file, err := os.OpenFile(a.path(last.name), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	a.file = file
	a.sizeAfterRewrite = a.Size()
	return nil
}

// Size returns the total size of the AOF files in bytes.
func (a *AOF) Size() int64 { return a.baseSize + a.incrSize + int64(a.buf.Len()) }

// SizeAfterRewrite returns the size right after the last rewrite or load.
func (a *AOF) SizeAfterRewrite() int64 { return a.sizeAfterRewrite }

func (a *AOF) Policy() FsyncPolicy { return a.policy }

// AppendRaw buffers an already RESP-encoded command.
func (a *AOF) AppendRaw(raw []byte) { a.buf.Write(raw) }

// Append buffers a command encoded as a RESP array.
func (a *AOF) Append(argv [][]byte) { a.buf.Write(Encode(a.buf.AvailableBuffer(), argv)) }

// Encode appends argv to b as a RESP array.
func Encode(b []byte, argv [][]byte) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(argv)), 10)
	b = append(b, '\r', '\n')
	for _, arg := range argv {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(arg)), 10)
		b = append(b, '\r', '\n')
		b = append(b, arg...)
		b = append(b, '\r', '\n')
	}
	return b
}

// Write moves buffered commands to the file, and fsyncs if the policy is always.
func (a *AOF) Write() error {
	if a.buf.Len() == 0 {
		return nil
	}
	n, err := a.buf.WriteTo(a.file)
	a.incrSize += n
	if err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.file.Sync()
	}
	return nil
}

// Sync fsyncs the current incr file.
func (a *AOF) Sync() error { return a.file.Sync() }

// Rewriting reports whether a rewrite is in progress.
func (a *AOF) Rewriting() bool { return a.rewrite != nil }

// StartRewrite starts a rewrite whose new base is snapshot, the rdb encoding
// of the dataset at this instant. Commands appended from now on go to a new
// incr file. The snapshot is written in the background; call PollRewrite
// periodically to finish the rewrite.
func (a *AOF) StartRewrite(snapshot []byte) error {
	if a.rewrite != nil {
		return ErrRewriteInProgress
	}
	// everything before the snapshot goes to the current incr file
	if err := a.Write(); err != nil {
		return err
	}
	if err := a.file.Sync(); err != nil {
		return err
	}

	last := a.m.incrs[len(a.m.incrs)-1]
	next := fileInfo{name: a.incrName(last.seq + 1), seq: last.seq + 1, typ: typeIncr}
	file, err := os.OpenFile(a.path(next.name), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	// The manifest keeps the old base and incr files until the new base is
	// durable, so a crash at any point loads a complete dataset.
	m := &manifest{base: a.m.base, incrs: append(a.m.incrs[:len(a.m.incrs):len(a.m.incrs)], next)}
	if err := m.save(a.path(a.name + ".manifest")); err != nil {
		_ = file.Close()
		_ = os.Remove(a.path(next.name))
		return err
	}
	_ = a.file.Close()
	a.file, a.m = file, m

	baseSeq := int64(1)
	if a.m.base != nil {
		baseSeq = a.m.base.seq + 1
	}
	rw := &rewriteState{
		base:     fileInfo{name: a.baseName(baseSeq), seq: baseSeq, typ: typeBase},
		baseSize: int64(len(snapshot)),
		keepFrom: len(m.incrs) - 1,
		done:     make(chan error, 1),
	}
	a.rewrite = rw
	path := a.path(rw.base.name)
	go func() { rw.done <- writeBase(path, snapshot) }()
	return nil
}

// PollRewrite finishes the rewrite if the background write is done. It
// returns done == false while the rewrite is still running (or none is).
func (a *AOF) PollRewrite() (done bool, err error) {
	if a.rewrite == nil {
		return false, nil
	}
	select {
	case err = <-a.rewrite.done:
		return true, a.finishRewrite(err)
	default:
		return false, nil
	}
}

func (a *AOF) finishRewrite(writeErr error) error {
	rw := a.rewrite
	a.rewrite = nil
	if writeErr != nil {
		// keep the current manifest: the old base and all incr files are
		// still complete. The next rewrite will retry.
		return fmt.Errorf("aof rewrite: write base: %w", writeErr)
	}
	old := a.m
	m := &manifest{base: &rw.base, incrs: old.incrs[rw.keepFrom:]}
	if err := m.save(a.path(a.name + ".manifest")); err != nil {
		_ = os.Remove(a.path(rw.base.name))
		return fmt.Errorf("aof rewrite: save manifest: %w", err)
	}
	a.m = m

	// the manifest no longer references the replaced files
	if old.base != nil {
		_ = os.Remove(a.path(old.base.name))
	}
	for _, f := range old.incrs[:rw.keepFrom] {
		_ = os.Remove(a.path(f.name))
	}
	a.baseSize = rw.baseSize
	a.incrSize = 0
	for _, f := range a.m.incrs {
		if st, err := os.Stat(a.path(f.name)); err == nil {
			a.incrSize += st.Size()
		}
	}
	a.sizeAfterRewrite = a.Size()
	return nil
}

// Close waits for a running rewrite, then flushes and closes the AOF.
func (a *AOF) Close() error {
	var rwErr error
	if a.rewrite != nil {
		rwErr = a.finishRewrite(<-a.rewrite.done)
	}
	if a.file == nil {
		return rwErr
	}
	err := a.Write()
	if err == nil {
		err = a.file.Sync()
	}
	return errors.Join(rwErr, err, a.file.Close())
}

// errTruncated means a file ends in the middle of a command.
var errTruncated = errors.New("aof truncated")

// readFile calls fn for every command in path. The arguments are only valid
// during the call. It returns the length of the valid prefix of the file.
func readFile(path string, fn func(argv [][]byte)) (valid int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, 0, 64<<10)
	var argv [][]byte
	pos := 0 // buf[pos:] is not parsed yet
	for {
		for {
			var n int
			argv, n, err = resp.ReadCommand(buf[pos:], argv)
			if err == resp.ErrIncomplete {
				break
			}
			if err != nil {
				return valid, fmt.Errorf("corrupted at offset %d: %w", valid, err)
			}
			pos += n
			valid += int64(n)
			if len(argv) > 0 {
				fn(argv)
			}
		}
		// compact, grow if a single command does not fit, then read more.
		buf = buf[:copy(buf, buf[pos:])]
		pos = 0
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := f.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			if len(buf) > 0 {
				return valid, fmt.Errorf("%w: %d trailing bytes", errTruncated, len(buf))
			}
			return valid, nil
		}
		if err != nil {
			return valid, err
		}
	}
}
