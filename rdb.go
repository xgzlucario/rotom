package main

import (
	"fmt"
	"github.com/tidwall/mmap"
	"github.com/xgzlucario/rotom/internal/iface"
	"github.com/xgzlucario/rotom/internal/resp"
	"os"
	"time"
)

type Rdb struct {
	path string
}

func NewRdb(path string) *Rdb {
	return &Rdb{path: path}
}

func (r *Rdb) SaveDB() (err error) {
	// create tmp file
	fname := fmt.Sprintf("%s.rdb", time.Now().Format(time.RFC3339))
	fs, err := os.Create(fname)
	if err != nil {
		return err
	}

	writer := resp.NewWriter(MB)
	writer.WriteArrayHead(db.dict.data.Len())

	db.dict.data.All(func(k string, v any) bool {
		// format: {objectType,ttl,key,value}
		objectType := getObjectType(v)
		writer.WriteInteger(int(objectType))
		ttl, _ := db.dict.expire.Get(k)
		writer.WriteInteger(int(ttl))
		writer.WriteBulkString(k)

		switch objectType {
		case TypeString:
			writer.WriteBulk(v.([]byte))
		case TypeInteger:
			writer.WriteInteger(v.(int))
		default:
			if err = v.(iface.Encoder).Encode(writer); err != nil {
				log.Error().Msgf("[rdb] encode error: %v, %v", objectType, err)
				return false
			}
		}
		return true
	})

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
	// Read file data by mmap.
	data, err := mmap.Open(r.path, false)
	if len(data) == 0 {
		return nil
	}
	if err != nil {
		return err
	}

	reader := resp.NewReader(data)
	n, err := reader.ReadArrayHead()
	if err != nil {
		return err
	}

	for range n {
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
			if err = val.Decode(reader); err != nil {
				return err
			}
			db.dict.Set(string(key), val)
		}
	}
	return nil
}
