package store

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"
)

func (s *storeShard) load() {
	// open file
	fs, err := os.Open(s.storePath)
	if err != nil {
		return
	}

	// read line
	for buf := bufio.NewScanner(fs); buf.Scan(); {
		args := strings.Split(buf.Text(), "||")

		switch args[0] {
		case OP_SET:
			if len(args) == 3 {
				s.Set(args[1], args[2])
			}

		case OP_SET_WITH_TTL:
			if len(args) == 4 {
				ttl, _ := strconv.Atoi(args[3])
				s.SetWithTTL(args[1], args[2], time.Duration(ttl))
			}

		case OP_REMOVE:
			if len(args) == 2 {
				s.Remove(args[1])
			}
		}
	}
}
