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
		args := strings.Split(buf.Text(), "|")

		switch args[0] {
		case OP_SET:
			s.Cache.Set(args[1], args[2])

		case OP_SET_WITH_TTL:
			ttl, _ := strconv.Atoi(args[3])
			s.Cache.SetWithTTL(args[1], args[2], time.Duration(ttl))

		case OP_REMOVE:
			s.Cache.Remove(args[1])
		}
	}
}
