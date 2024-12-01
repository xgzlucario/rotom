package resp

import (
	"bytes"
	"github.com/tidwall/redcon"
	"io"
	"strconv"
)

type Writer struct {
	*redcon.Writer
}

type Reader struct {
	*redcon.Reader
}

func (w *Writer) WriteFloat(f float64) {
	w.WriteBulkString(strconv.FormatFloat(f, 'f', -1, 64))
}

func NewWriter() *Writer {
	return &Writer{
		Writer: redcon.NewWriter(bytes.NewBuffer(nil)),
	}
}

func NewWriterWith(w io.Writer) *Writer {
	return &Writer{
		Writer: redcon.NewWriter(w),
	}
}

func (w *Writer) Reset() {
	w.Writer.SetBuffer(nil)
}

func NewReader(b []byte) *Reader {
	return &Reader{redcon.NewReader(bytes.NewReader(b))}
}
