package main

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/tidwall/mmap"
	"github.com/xgzlucario/rotom/internal/hash"
	"github.com/xgzlucario/rotom/internal/list"
	"github.com/xgzlucario/rotom/internal/zset"
	"os"
	"time"
)

const (
	saveBatchSize = 100
)

type Rdb struct {
	path string
}

func NewRdb(path string) (*Rdb, error) {
	return &Rdb{
		path: path,
	}, nil
}

type rdbBatch []rdbEntry

type rdbEntry struct {
	Type  ObjectType `json:"o,omitempty"`
	Ttl   int64      `json:"t,omitempty"`
	Key   string     `json:"k"`
	Value any        `json:"v"`
}

func (r *Rdb) SaveDB() error {
	// create tmp file
	fname := fmt.Sprintf("%s.rdb", time.Now().Format(time.RFC3339))
	fs, err := os.Create(fname)
	if err != nil {
		return err
	}

	writer := NewWriter(MB)
	writer.WriteArrayHead(len(db.dict.data)/saveBatchSize + 1)
	var batch rdbBatch
	for k, v := range db.dict.data {
		batch = append(batch, rdbEntry{
			Type:  getObjectType(v),
			Ttl:   db.dict.expire[k],
			Key:   k,
			Value: v,
		})
		if len(batch) == saveBatchSize {
			err = r.dumps(batch, writer, fs)
			if err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	err = r.dumps(batch, writer, fs)
	if err != nil {
		return err
	}
	err = fs.Close()
	if err != nil {
		return err
	}
	return os.Rename(fname, r.path)
}

func (r *Rdb) dumps(batch rdbBatch, writer *RESPWriter, fs *os.File) error {
	src, err := sonic.Marshal(batch)
	if err != nil {
		return err
	}
	writer.WriteBulk(src)
	_, err = writer.FlushTo(fs)
	return err
}

func (r *Rdb) LoadDB() error {
	fs, err := os.OpenFile(r.path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	// Read file data by mmap.
	data, err := mmap.MapFile(fs, false)
	if len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	reader := NewReader(data)
	batches, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}
	var entries []rdbEntry
	for range batches {
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

	case TypeInteger:

	case TypeMap:
		hm := hash.NewMap()
		if err := hm.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = hm

	case TypeZipMap:
		zm := hash.NewZipMap()
		if err := zm.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = zm

	case TypeSet:
		hs := hash.NewSet()
		if err := hs.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = hs

	case TypeZipSet:
		zs := hash.NewZipSet()
		if err := zs.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = zs

	case TypeList:
		ls := list.New()
		if err := ls.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = ls

	case TypeZSet:
		zs := zset.New()
		if err := zs.Unmarshal(src); err != nil {
			return err
		}
		db.dict.data[e.Key] = zs

	default:
	}
	return errors.New("rdb read error objectType")
}
