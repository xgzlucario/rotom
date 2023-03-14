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
	// open file
	fs, err := os.ReadFile(s.storePath)
	if err != nil {
		return
	}

	// read block
	for _, blk := range bytes.Split(fs, blkSpr) {
		// decompress
		blk, _ = base.ZstdDecode(blk)

		for _, line := range bytes.Split(blk, lineSpr) {
			s.readLine(line)
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
		for i, c := range line {
			if c == spr {
				s.Set(*base.B2S(line[1:i]), line[i+1:])
				break
			}
		}

	// SET_WITH_TTL: {op}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		var sp1 int
		for i, c := range line {
			if c == spr {
				if sp1 == 0 {
					sp1 = i

				} else {
					ttl, _ := strconv.Atoi(*base.B2S(line[sp1+1 : i]))
					s.SetWithTTL(*base.B2S(line[1:sp1]), line[i+1:], time.Duration(ttl))
					break
				}
			}
		}

	// REMOVE: {op}{key}
	case OP_REMOVE:
		s.Remove(*base.B2S(line[1:]))

	// PERSIST: {op}{key}
	case OP_PERSIST:
		s.Persist(*base.B2S(line[1:]))
	}
}

// rewrite
func (s *storeShard) rewrite() {
	s.Lock()
	defer s.Unlock()

	// open file
	fs, err := os.ReadFile(s.storePath)
	if err != nil {
		return
	}

	// bloom filter
	filter := structx.NewBloom()

	// read from tail
	blks := bytes.Split(fs, blkSpr)
	for i := len(blks) - 1; i >= 0; i-- {
		blk := blks[i]

		for _, line := range bytes.Split(blk, lineSpr) {
			for i, r := range line {
				// seperate op and key
				if r == spr {
					if filter.TestAndAdd(line[:i]) {
						// TODO: rewrite
						filter.Cap()
					}
				}
			}
		}
	}
}
