package main

import (
	"bytes"
	"github.com/tidwall/redcon"
	"io"
	"os"
)

// Aof manages an append-only file system for storing data.
type Aof struct {
	file *os.File
	buf  *bytes.Buffer
}

func NewAof(path string) (*Aof, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Aof{
		file: fd,
		buf:  bytes.NewBuffer(make([]byte, 0, KB)),
	}, nil
}

func (a *Aof) Close() error {
	return a.file.Close()
}

func (a *Aof) Write(buf []byte) (int, error) {
	return a.buf.Write(buf)
}

func (a *Aof) Flush() error {
	_, _ = a.buf.WriteTo(a.file)
	return a.file.Sync()
}

func (a *Aof) Read(fn func(args []redcon.RESP)) error {
	rd := redcon.NewReader(a.file)
	cmds, err := rd.ReadCommands()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	respBuf := make([]redcon.RESP, 8)

	// Iterate over the records in the file, applying the function to each.
	for _, cmd := range cmds {
		respBuf = respBuf[:0]
		for _, arg := range cmd.Args {
			respBuf = append(respBuf, redcon.RESP{Data: arg})
		}
		fn(respBuf)
	}
	return nil
}
