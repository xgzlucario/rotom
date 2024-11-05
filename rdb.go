package main

import (
	"bytes"
	"errors"
	"github.com/bytedance/sonic"
	"github.com/tidwall/mmap"
	hash2 "github.com/xgzlucario/rotom/internal/hash"
	"os"
	"time"
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

type rdbEntry struct {
	Type  ObjectType `json:"o,omitempty"`
	Ttl   int64      `json:"t,omitempty"`
	Key   string     `json:"k"`
	Value any        `json:"v"`
}

func (r *Rdb) SaveDB() error {
	buf := bytes.NewBuffer(nil)
	r.writer.WriteArrayHead(len(db.dict.data)/saveBatchSize + 1)

	var entries []rdbEntry
	for k, v := range db.dict.data {
		entries = append(entries, rdbEntry{
			Type:  getObjectType(v),
			Ttl:   db.dict.expire[k],
			Key:   k,
			Value: v,
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

func (r *Rdb) LoadDB() error {
	// Read file data by mmap.
	data, err := mmap.MapFile(r.file, false)
	if len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	reader := NewReader(data)
	batchCount, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	var entries []rdbEntry
	for range batchCount {
		src, err := reader.ReadBulk()
		if err != nil {
			return err
		}
		err = sonic.Unmarshal(src, &entries)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := r.loadEntry(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Rdb) loadEntry(e rdbEntry) error {
	// check expired
	if e.Ttl > 0 && e.Ttl < time.Now().UnixNano() {
		return nil
	}

	src, ok := e.Value.([]byte)
	if !ok {
		return errors.New("invalid entry value type")
	}
	switch e.Type {
	case TypeString:
		db.dict.data[e.Key] = src
		db.dict.expire[e.Key] = e.Ttl

	case TypeMap:
		hash := hash2.NewMap()
		if err := hash.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = hash

	default:
	}
	return errors.New("[rdb] error objectType")
}

func (r *Rdb) Close() error {
	return r.file.Close()
}
