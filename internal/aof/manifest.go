package aof

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/xgzlucario/rotom/internal/fileutil"
)

// The manifest lists the files that make up the AOF, in the same text
// format as redis 7:
//
//	file appendonly.aof.2.base.rdb seq 2 type b
//	file appendonly.aof.5.incr.aof seq 5 type i
//	file appendonly.aof.6.incr.aof seq 6 type i
//
// Loading the AOF means loading the base (a snapshot), then replaying the
// incr files (commands written after the snapshot) in order. The manifest is
// only ever replaced atomically, so it always names a consistent set.

type fileType byte

const (
	typeBase fileType = 'b'
	typeIncr fileType = 'i'
)

type fileInfo struct {
	name string
	seq  int64
	typ  fileType
}

type manifest struct {
	base  *fileInfo // nil before the first rewrite
	incrs []fileInfo
}

// isRDB reports the format of a base file from its name: rewrites write rdb
// snapshots, while a legacy single-file AOF becomes an aof-format base.
func (f fileInfo) isRDB() bool { return strings.HasSuffix(f.name, ".rdb") }

func (m *manifest) files() []fileInfo {
	var res []fileInfo
	if m.base != nil {
		res = append(res, *m.base)
	}
	return append(res, m.incrs...)
}

func (m *manifest) encode() []byte {
	var b bytes.Buffer
	for _, f := range m.files() {
		fmt.Fprintf(&b, "file %s seq %d type %c\n", f.name, f.seq, f.typ)
	}
	return b.Bytes()
}

func (m *manifest) save(path string) error {
	return fileutil.WriteAtomic(path, m.encode())
}

var errBadManifest = errors.New("invalid aof manifest")

func loadManifest(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &manifest{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %v", errBadManifest, line, err)
		}
		switch {
		case f.typ == typeBase && m.base == nil && len(m.incrs) == 0:
			m.base = &f
		case f.typ == typeIncr && (len(m.incrs) == 0 || f.seq > m.incrs[len(m.incrs)-1].seq):
			m.incrs = append(m.incrs, f)
		default:
			return nil, fmt.Errorf("%w: %q: out of order", errBadManifest, line)
		}
	}
	if len(m.incrs) == 0 {
		return nil, fmt.Errorf("%w: no incr file", errBadManifest)
	}
	return m, nil
}

// parseLine parses "file <name> seq <n> type <b|i>"; keys may come in any order.
func parseLine(line string) (fileInfo, error) {
	fields := strings.Fields(line)
	if len(fields)%2 != 0 {
		return fileInfo{}, errors.New("odd number of fields")
	}
	var f fileInfo
	for i := 0; i < len(fields); i += 2 {
		key, val := fields[i], fields[i+1]
		switch key {
		case "file":
			if strings.ContainsAny(val, `/\`) {
				return f, errors.New("file name must not contain a path")
			}
			f.name = val
		case "seq":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n <= 0 {
				return f, errors.New("bad seq")
			}
			f.seq = n
		case "type":
			if val != "b" && val != "i" {
				return f, errors.New("bad type")
			}
			f.typ = fileType(val[0])
		}
	}
	if f.name == "" || f.seq == 0 || f.typ == 0 {
		return f, errors.New("missing field")
	}
	return f, nil
}
