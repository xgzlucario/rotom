package resp

import (
	"bytes"
	"github.com/tidwall/redcon"
)

type Writer struct {
	*redcon.Writer
}

type Reader struct {
	*redcon.Reader
}

func NewWriter() *Writer {
	return &Writer{
		Writer: redcon.NewWriter(bytes.NewBuffer(nil)),
	}
}

func (w *Writer) Reset() {
	w.Writer.SetBuffer(nil)
}

func NewReader(b []byte) *Reader {
	return &Reader{redcon.NewReader(bytes.NewReader(b))}
}
