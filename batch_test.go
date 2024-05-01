package rotom

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genKV(i int) (string, []byte) {
	k := fmt.Sprintf("k-%08x", i)
	v := fmt.Sprintf("v-%08x", i)
	return k, []byte(v)
}

func TestBatchSet(t *testing.T) {
	assert := assert.New(t)
	db, _ := createDB()

	batch := make([]*Batch, 0)
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		batch = append(batch, &Batch{
			Key: k,
			Val: []byte(v),
		})
	}
	db.BatchSet(batch...)

	// get
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		val, ts, err := db.Get(k)
		assert.Equal(val, v)
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		val, ts, err := db2.Get(k)
		assert.Equal(val, v)
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}
}

func TestBatchHSet(t *testing.T) {
	assert := assert.New(t)
	db, _ := createDB()

	batches := make([]*Batch, 0)
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		batches = append(batches, &Batch{
			Key: k,
			Val: []byte(v),
		})
	}
	db.BatchHSet("map", batches...)

	// get
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		val, err := db.HGet("map", k)
		assert.Equal(val, v)
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < 1000; i++ {
		k, v := genKV(i)
		val, err := db2.HGet("map", k)
		assert.Equal(val, v)
		assert.Nil(err)
	}
}
