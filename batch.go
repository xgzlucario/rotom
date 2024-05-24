package rotom

import (
	"fmt"

	"github.com/xgzlucario/rotom/codeman"
)

type (
	// For Set, HSet
	Batch struct {
		Key       string
		Val       []byte
		Timestamp int64
	}

	// For ZSet
	ZSBatch struct {
		Key   string
		Score int64
	}
)

func checkts(batches []*Batch) {
	for _, b := range batches {
		if b.Timestamp < 0 {
			panic(fmt.Sprintf("error: batch key `%s` timestamp is negetive", b.Key))
		}
	}
}

// BatchSet
func (db *DB) BatchSet(batches ...*Batch) {
	checkts(batches)
	codec := codeman.NewCodec()

	for _, b := range batches {
		codec = codec.Byte(byte(OpSetTx)).Str(b.Key).Int(b.Timestamp).Bytes(b.Val)
		db.m.SetTx(b.Key, b.Val, b.Timestamp)
	}
	db.encode(codec)
}

// BatchHSet
func (db *DB) BatchHSet(key string, batches ...*Batch) error {
	checkts(batches)

	m, err := db.fetchMap(key, true)
	if err != nil {
		return err
	}
	codec := newCodec(OpHSetTx).Str(key).Int(int64(len(batches)))

	for _, b := range batches {
		codec = codec.Str(b.Key).Bytes(b.Val).Int(b.Timestamp)
		m.Set(b.Key, b.Val)
	}
	db.encode(codec)

	return nil
}

// BatchZSet
func (db *DB) BatchZSet(key string, batches ...*ZSBatch) error {
	m, err := db.fetchZSet(key, true)
	if err != nil {
		return err
	}
	codec := newCodec(OpZSet).Str(key).Int(int64(len(batches)))

	for _, b := range batches {
		codec = codec.Str(b.Key).Int(b.Score)
		m.Set(b.Key, b.Score)
	}
	db.encode(codec)

	return nil
}
