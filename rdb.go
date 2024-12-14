package main

import (
	"fmt"
	"github.com/tidwall/mmap"
	"github.com/xgzlucario/rotom/internal/iface"
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

	writer := iface.NewWriter(make([]byte, 0, KB))
	writer.WriteUint64(uint64(db.dict.data.Len()))
	db.dict.data.All(func(k string, v any) bool {
		// format: {objectType, ttl, key, value}
		objectType := getObjectType(v)
		writer.WriteUint8(uint8(objectType))
		ttl, _ := db.dict.expire.Get(k)
		writer.WriteVarint(int(ttl))
		writer.WriteString(k)

		switch objectType {
		case TypeString:
			writer.WriteBytes(v.([]byte))
		case TypeInteger:
			writer.WriteVarint(v.(int))
		default:
			v.(iface.Encoder).WriteTo(writer)
		}
		return true
	})

	// flush
	_, err = fs.Write(writer.Bytes())
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

	rd := iface.NewReader(data)
	n := rd.ReadUint64()
	for range n {
		// format: {objectType, ttl, key, value}
		objectType := rd.ReadUint8()
		ttl := rd.ReadVarint()
		key := rd.ReadString()

		switch ObjectType(objectType) {
		case TypeString:
			db.dict.SetWithTTL(key, rd.ReadBytes(), ttl)
		case TypeInteger:
			db.dict.SetWithTTL(key, int(rd.ReadVarint()), ttl)
		default:
			val := type2c[ObjectType(objectType)]()
			if val == nil {
				panic(fmt.Sprintf("unknown object type: %v", objectType))
			}
			val.ReadFrom(rd)
			db.dict.SetWithTTL(key, val, ttl)
		}
	}
	return nil
}
