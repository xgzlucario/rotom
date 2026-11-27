package resp

import (
	"bytes"
	"errors"
	"strconv"
)

// Limits mirror redis' defaults (proto-max-bulk-len, max multibulk length).
const (
	MaxBulkLen      = 512 << 20
	MaxMultiBulkLen = 1 << 20
	maxInlineLen    = 64 << 10
)

var (
	// ErrIncomplete means buf does not hold a whole command yet.
	ErrIncomplete = errors.New("incomplete command")

	ErrInvalidMultiBulkLen = errors.New("invalid multibulk length")
	ErrInvalidBulkLen      = errors.New("invalid bulk length")
	ErrExpectedBulk        = errors.New("expected '$'")
	ErrInlineTooLong       = errors.New("too big inline request")
	ErrUnbalancedQuotes    = errors.New("unbalanced quotes in request")
)

// ReadCommand parses one command from the front of buf.
//
// It accepts both the RESP multibulk form (*2\r\n$3\r\nGET\r\n$1\r\nk\r\n),
// sent by every client library, and the inline form (GET k\r\n) typed in
// telnet. The returned arguments alias buf: no data is copied. argv is
// reused as the result's backing array.
//
// It returns the number of bytes consumed, or ErrIncomplete if more data is
// needed. Any other error is a protocol error.
func ReadCommand(buf []byte, argv [][]byte) ([][]byte, int, error) {
	argv = argv[:0]
	if len(buf) == 0 {
		return argv, 0, ErrIncomplete
	}
	if buf[0] != '*' {
		return readInline(buf, argv)
	}

	n, pos, err := readLength(buf, 1, MaxMultiBulkLen, ErrInvalidMultiBulkLen)
	if err != nil {
		return argv, 0, err
	}
	for range n {
		if pos >= len(buf) {
			return argv, 0, ErrIncomplete
		}
		if buf[pos] != '$' {
			return argv, 0, ErrExpectedBulk
		}
		var size int
		size, pos, err = readLength(buf, pos+1, MaxBulkLen, ErrInvalidBulkLen)
		if err != nil {
			return argv, 0, err
		}
		end := pos + size
		if end+2 > len(buf) {
			return argv, 0, ErrIncomplete
		}
		if buf[end] != '\r' || buf[end+1] != '\n' {
			return argv, 0, ErrInvalidBulkLen
		}
		argv = append(argv, buf[pos:end:end])
		pos = end + 2
	}
	return argv, pos, nil
}

// readLength parses the decimal number in buf[pos:] up to \r\n, returning the
// number and the position after the line.
func readLength(buf []byte, pos, limit int, errInvalid error) (int, int, error) {
	end := bytes.IndexByte(buf[pos:], '\n')
	if end < 0 {
		if len(buf)-pos > 32 {
			return 0, 0, errInvalid // no sane length line is this long
		}
		return 0, 0, ErrIncomplete
	}
	end += pos
	if end == pos || buf[end-1] != '\r' {
		return 0, 0, errInvalid
	}
	n, err := strconv.Atoi(string(buf[pos : end-1])) // does not allocate
	if err != nil || n < 0 || n > limit {
		return 0, 0, errInvalid
	}
	return n, end + 1, nil
}

// readInline splits a line on whitespace, honouring "double" and 'single'
// quotes like redis-cli.
func readInline(buf []byte, argv [][]byte) ([][]byte, int, error) {
	end := bytes.IndexByte(buf, '\n')
	if end < 0 {
		if len(buf) > maxInlineLen {
			return argv, 0, ErrInlineTooLong
		}
		return argv, 0, ErrIncomplete
	}
	line := buf[:end]
	line = bytes.TrimSuffix(line, []byte{'\r'})

	for i := 0; i < len(line); {
		switch c := line[i]; {
		case c == ' ' || c == '\t':
			i++
		case c == '"' || c == '\'':
			closing := bytes.IndexByte(line[i+1:], c)
			if closing < 0 {
				return argv, 0, ErrUnbalancedQuotes
			}
			argv = append(argv, line[i+1:i+1+closing])
			i += closing + 2
		default:
			j := i
			for j < len(line) && line[j] != ' ' && line[j] != '\t' {
				j++
			}
			argv = append(argv, line[i:j])
			i = j
		}
	}
	return argv, end + 1, nil
}
