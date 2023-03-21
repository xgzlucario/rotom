package store

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/base"
	"github.com/xgzlucario/rotom/structx"
)

const (
	OP_SET          = '1'
	OP_SET_WITH_TTL = '2'
	OP_REMOVE       = '3'
	OP_PERSIST      = '4'

	spr = '|'
)

var (
	// seperate char
	lineSpr = []byte("\n")
	blkSpr  = []byte("[BLK]")
)

func (s *storeShard) load() {
	s.Lock()
	defer s.Unlock()

	// open file
	fs, err := os.ReadFile(s.storePath)
	if err != nil {
		return
	}

	// reset filter
	s.filter = structx.NewBloom()

	blks := bytes.Split(fs, blkSpr)

	// read block from tail
	for i := len(blks) - 1; i >= 0; i-- {
		// decompress
		blks[i], _ = base.ZstdDecode(blks[i])

		lines := bytes.Split(blks[i], lineSpr)

		// read line from tail
		for j := len(lines) - 1; j >= 0; j-- {
			s.readLine(lines[j])
		}
	}
}

func newWriter(path string) *os.File {
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	return writer
}

// write data
func (s *storeShard) write(format string, data ...any) {
	str := fmt.Sprintf(format, data...)
	s.Lock()
	defer s.Unlock()

	s.buffer = append(s.buffer, base.S2B(&str)...)
}

// write buffer block
func (s *storeShard) writeBufferBlock() {
	s.Lock()
	defer s.Unlock()

	if len(s.buffer) == 0 {
		return
	}
	// write
	s.buffer = append(base.ZstdEncode(s.buffer), blkSpr...)
	_, err := s.rw.Write(s.buffer)
	if err != nil {
		panic(err)
	}

	// reset
	s.buffer = s.buffer[0:0]
}

// read line
func (s *storeShard) readLine(line []byte) {
	if len(line) == 0 {
		return
	}

	switch line[0] {
	// SET: {op}{key}|{value}
	case OP_SET:
		i := bytes.IndexByte(line, spr)

		// test
		if s.filter.TestAndAdd(line[:i]) {
			goto END
		}
		for _, b := range []byte{OP_SET_WITH_TTL, OP_REMOVE} {
			line[0] = b
			if s.filter.Test(line[:i]) {
				goto END
			}
		}

		s.Set(*base.B2S(line[1:i]), line[i+1:])

	// SET_WITH_TTL: {op}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		sp1 := bytes.IndexByte(line, spr)
		sp2 := bytes.IndexByte(line[sp1:], spr)

		// test
		if s.filter.TestAndAdd(line[:sp1]) {
			goto END
		}
		for _, b := range []byte{OP_SET, OP_REMOVE} {
			line[0] = b
			if s.filter.Test(line[:sp1]) {
				goto END
			}
		}

		ttl, _ := strconv.Atoi(*base.B2S(line[sp1+1 : sp2]))
		s.SetWithTTL(*base.B2S(line[1:sp1]), line[sp1+1:], time.Duration(ttl))

	// REMOVE: {op}{key}
	case OP_REMOVE:
		// test
		if s.filter.TestAndAdd(line) {
			goto END
		}
		for _, b := range []byte{OP_SET, OP_SET_WITH_TTL} {
			line[0] = b
			if s.filter.Test(line) {
				goto END
			}
		}

		s.Remove(*base.B2S(line[1:]))

	// PERSIST: {op}{key}
	case OP_PERSIST:
		// test
		if s.filter.TestAndAdd(line) {
			goto END
		}
		for _, b := range []byte{OP_SET, OP_REMOVE} {
			line[0] = b
			if s.filter.Test(line) {
				goto END
			}
		}

		s.Persist(*base.B2S(line[1:]))
	}

END:
}
