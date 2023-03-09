package store

import (
	"bufio"
	"os"
	"strconv"
	"time"
)

const (
	OP_SET          = '1'
	OP_SET_WITH_TTL = '2'
	OP_REMOVE       = '3'
	OP_PERSIST      = '4'

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
					s.Set(string(bt[1:i]), bt[i+1:])
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
						ttl, _ := strconv.Atoi(string(bt[sep1+1 : i]))
						s.SetWithTTL(string(bt[1:sep1]), bt[i+1:], time.Duration(ttl))
						break
					}
				}
			}

		// REMOVE: {op}{key}
		case OP_REMOVE:
			s.Remove(string(bt[1:]))

		// PERSIST: {op}{key}
		case OP_PERSIST:
			s.Persist(string(bt[1:]))
		}
	}
}
