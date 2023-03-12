package store

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/xgzlucario/rotom/base"
)

const (
	OP_SET byte = iota + 1
	OP_SET_WITH_TTL
	OP_REMOVE
	OP_PERSIST

	separate = '|'
)

func (s *storeShard) load(storePath string) {
	// open file
	fs, err := os.Open(storePath)
	if err != nil {
		return
	}

	// read line
	for buf := bufio.NewScanner(fs); buf.Scan(); {
		bt := buf.Bytes()

		switch bt[0] {
		// SET: {op}{key}|{value}
		case OP_SET:
			for i, c := range bt {
				if c == separate {
					s.Set(*base.B2S(bt[1:i]), bt[i+1:])
					break
				}
			}

		// SET_WITH_TTL: {op}{key}|{ttl}|{value}
		case OP_SET_WITH_TTL:
			var sep1 int
			for i, c := range bt {
				if c == separate {
					if sep1 == 0 {
						sep1 = i

					} else {
						ttl, _ := strconv.Atoi(*base.B2S(bt[sep1+1 : i]))
						s.SetWithTTL(*base.B2S(bt[1:sep1]), bt[i+1:], time.Duration(ttl))
						break
					}
				}
			}

		// REMOVE: {op}{key}
		case OP_REMOVE:
			s.Remove(*base.B2S(bt[1:]))

		// PERSIST: {op}{key}
		case OP_PERSIST:
			s.Persist(*base.B2S(bt[1:]))
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

// write buffer to log
func (s *storeShard) writeBuffer() {
	s.Lock()
	defer s.Unlock()

	if len(s.buffer) == 0 {
		return
	}
	s.rw.Write(s.buffer)
	s.buffer = s.buffer[0:0]
}
