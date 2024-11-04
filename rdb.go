package main

import (
	"bytes"
	"github.com/bytedance/sonic"
	"os"
)

const (
	saveBatchSize = 100
)

type Rdb struct {
	file   *os.File
	writer *RESPWriter
}

func NewRdb(path string) (*Rdb, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Rdb{
		file:   fd,
		writer: NewWriter(MB),
	}, nil
}

type RdbEntry struct {
	Type ObjectType `json:"o"`
	Key  string     `json:"k"`
	Data any        `json:"v"`
	Ttl  int64      `json:"t,omitempty"`
}

func (r *Rdb) SaveDB() error {
	buf := bytes.NewBuffer(nil)
	r.writer.WriteArrayHead(len(db.dict.data)/saveBatchSize + 1)

	var entries []RdbEntry
	for k, v := range db.dict.data {
		entries = append(entries, RdbEntry{
			Type: getObjectType(v),
			Key:  k,
			Data: v,
			Ttl:  db.dict.expire[k],
		})
		if len(entries) == saveBatchSize {
			err := sonic.ConfigDefault.NewEncoder(buf).Encode(entries)
			if err != nil {
				return err
			}
			r.writer.WriteBulk(buf.Bytes())
			buf.Reset()
			entries = entries[:0]
		}
	}
	err := sonic.ConfigDefault.NewEncoder(buf).Encode(entries)
	if err != nil {
		return err
	}
	r.writer.WriteBulk(buf.Bytes())

	_, err = r.writer.FlushTo(r.file)
	if err != nil {
		return err
	}
	return r.file.Sync()
}

func (r *Rdb) Close() error {
	return r.file.Close()
}
