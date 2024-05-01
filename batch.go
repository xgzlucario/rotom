package rotom

import "github.com/xgzlucario/rotom/codeman"

type Batch struct {
	Key       string
	Val       []byte
	Timestamp int64
}

// BatchSet
func (db *DB) BatchSet(batches ...*Batch) {
	codec := codeman.NewCodec()
	for _, b := range batches {
		if b.Timestamp < 0 {
			continue
		}
		codec = codec.Byte(byte(OpSetTx)).Str(b.Key).Int(b.Timestamp).Bytes(b.Val)
		db.m.SetTx(b.Key, b.Val, b.Timestamp)
	}
	db.encode(codec)
}

// BatchHSet
func (db *DB) BatchHSet(key string, batches ...*Batch) error {
	m, err := db.fetchMap(key, true)
	if err != nil {
		return err
	}
	codec := codeman.NewCodec()
	for _, b := range batches {
		if b.Timestamp < 0 {
			continue
		}
		codec = codec.Byte(byte(OpHSet)).Str(key).Str(b.Key).Bytes(b.Val).Int(b.Timestamp)
		m.Set(b.Key, b.Val, b.Timestamp)
	}
	db.encode(codec)
	return nil
}
