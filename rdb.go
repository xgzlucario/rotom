package main

import (
	"errors"
	"fmt"
	"github.com/tidwall/mmap"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
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

func (r *Rdb) SaveDB() error {
	// create tmp file
	fname := fmt.Sprintf("%s.rdb", time.Now().Format(time.RFC3339))
	fs, err := os.Create(fname)
	if err != nil {
		return err
	}

	writer := resp.NewWriter(MB)
	writer.WriteArrayHead(len(db.dict.data))

	for k, v := range db.dict.data {
		// format: {objectType,ttl,key,value}
		objectType := getObjectType(v)
		writer.WriteInteger(int(objectType))
		writer.WriteInteger(int(db.dict.expire[k]))
		writer.WriteBulkString(k)

		switch objectType {
		case TypeString:
			raw, ok := v.([]byte)
			if !ok {
				return errors.New("invalid data typeString")
			}
			writer.WriteBulk(raw)
		case TypeInteger:
			raw, ok := v.(int)
			if !ok {
				return errors.New("invalid data typeInteger")
			}
			writer.WriteInteger(raw)
		default:
			val, ok := v.(iface.Encoder)
			if !ok {
				return errors.New("invalid data type")
			}
			if err := val.Encode(writer); err != nil {
				return err
			}
		}
	}

	// flush
	_, err = writer.FlushTo(fs)
	if err != nil {
		return err
	}
	err = fs.Close()
	if err != nil {
		return err
	}
	return os.Rename(fname, r.path)
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

	reader := resp.NewReader(data)
	count, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}

	for range count {
		// format: {objectType,ttl,key,value}
		objectType, err := reader.ReadInteger()
		if err != nil {
			return err
		}
		ttl, err := reader.ReadInteger()
		if err != nil {
			return err
		}
		key, err := reader.ReadBulk()
		if err != nil {
			return err
		}

		switch ObjectType(objectType) {
		case TypeString:
			val, err := reader.ReadBulk()
			if err != nil {
				return err
			}
			db.dict.SetWithTTL(string(key), val, int64(ttl))

		case TypeInteger:
			n, err := reader.ReadInteger()
			if err != nil {
				return err
			}
			db.dict.SetWithTTL(string(key), n, int64(ttl))

		default:
			val := type2c[ObjectType(objectType)]()
			if val == nil {
				return errors.New("invalid data type")
			}
			if err := val.Decode(reader); err != nil {
				return err
			}
			db.dict.Set(string(key), val)
		}
	}
	return nil
}
