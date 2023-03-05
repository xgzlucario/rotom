package store

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func (s *Store) load() {
	// open file
	fs, err := os.Open(s.storePath)
	if err != nil {
		return
	}

	ch := make(chan string, 1024)
	var finished uint32

	// read line
	go func() {
		for buf := bufio.NewScanner(fs); buf.Scan(); {
			ch <- buf.Text()
		}
		atomic.StoreUint32(&finished, 1)
	}()

	// operation
	for {
		select {
		case text := <-ch:
			args := strings.Split(text, "|")

			switch args[0] {
			case OP_SET:
				s.m.Set(args[1], args[2])

			case OP_SET_WITH_TTL:
				ttl, _ := strconv.Atoi(args[3])
				s.m.SetWithTTL(args[1], args[2], time.Duration(ttl))

			case OP_REMOVE:
				if len(args) == 2 {
					s.m.Remove(args[1])
				}
			}

		default:
			if atomic.LoadUint32(&finished) == 1 {
				return
			}
		}
	}
}
