package store

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/base"
)

const (
	OP_SET          = '1'
	OP_SET_WITH_TTL = '2'
	OP_REMOVE       = '3'
	OP_PERSIST      = '4'

	separate = '|'
)

var (
	blkSeperate = []byte("|||")
)

func (s *storeShard) load(storePath string) {
	// open file
	fs, err := os.ReadFile(storePath)
	if err != nil {
		return
	}

	// read block
	for _, cpBlk := range bytes.Split(fs, blkSeperate) {
		// decompress
		blk, _ := base.ZstdDecode(cpBlk)
		if len(blk) == 0 {
			continue
		}

		for _, line := range bytes.Split(blk, []byte{'\n'}) {
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
	s.buffer = append(base.ZstdEncode(s.buffer), blkSeperate...)
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
			if c == separate {
				s.Set(*base.B2S(line[1:i]), line[i+1:])
				break
			}
		}

	// SET_WITH_TTL: {op}{key}|{ttl}|{value}
	case OP_SET_WITH_TTL:
		var sep1 int
		for i, c := range line {
			if c == separate {
				if sep1 == 0 {
					sep1 = i

				} else {
					ttl, _ := strconv.Atoi(*base.B2S(line[sep1+1 : i]))
					s.SetWithTTL(*base.B2S(line[1:sep1]), line[i+1:], time.Duration(ttl))
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
