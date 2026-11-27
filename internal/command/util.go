package command

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/xgzlucario/rotom/internal/db"
)

var (
	ErrWrongType       = db.ErrWrongType
	ErrParseInteger    = errors.New("ERR value is not an integer or out of range")
	ErrParseFloat      = errors.New("ERR value is not a valid float")
	ErrValueOutOfRange = errors.New("ERR value is out of range, must be positive")
	ErrOverflow        = errors.New("ERR increment or decrement would overflow")
	ErrInvalidCursor   = errors.New("ERR invalid cursor")
	ErrWrongArguments  = errors.New("ERR wrong number of arguments")
	ErrUnknownCommand  = errors.New("ERR unknown command")
	ErrSyntax          = errors.New("ERR syntax error")
)

func errInvalidExpire(cmd string) error {
	return fmt.Errorf("ERR invalid expire time in '%s' command", cmd)
}

// UnknownCommandError formats the reply for an unknown command name.
func UnknownCommandError(name []byte) string {
	return fmt.Sprintf("%s '%s'", ErrUnknownCommand, name)
}

// b2s converts without copying. The result must not outlive b's contents
// unless it is cloned (the db package clones keys it retains).
func b2s(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

func equalFold(a, b string) bool {
	return len(a) == len(b) && strings.EqualFold(a, b)
}

// parseInt parses an integer argument the way redis does.
func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(b2s(b), 10, 64)
	if err != nil {
		return 0, ErrParseInteger
	}
	return n, nil
}

// parseFloat parses a float argument, rejecting NaN like redis.
func parseFloat(b []byte) (float64, error) {
	s := b2s(b)
	switch strings.ToLower(s) {
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return 0, ErrParseFloat
	}
	return f, nil
}

// normalizeRange converts redis-style inclusive [start, stop] indexes
// (negative counts from the end) into a valid range. ok is false if empty.
func normalizeRange(start, stop, length int) (int, int, bool) {
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	start = max(start, 0)
	stop = min(stop, length-1)
	if start > stop || start >= length {
		return 0, 0, false
	}
	return start, stop, true
}
