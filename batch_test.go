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
	const N = 1000
	db, _ := createDB()

	batch := make([]*Batch, 0)
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		batch = append(batch, &Batch{
			Key: k,
			Val: []byte(v),
		})
	}
	db.BatchSet(batch...)

	// get
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		val, ts, err := db.Get(k)
		assert.Equal(val, v)
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}

	db.Sync()
	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		val, ts, err := db2.Get(k)
		assert.Equal(val, v)
		assert.Equal(ts, int64(0))
		assert.Nil(err)
	}
}

func TestBatchHSet(t *testing.T) {
	assert := assert.New(t)
	const N = 1000
	db, _ := createDB()

	batches := make([]*Batch, 0, N)
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		batches = append(batches, &Batch{
			Key: k,
			Val: []byte(v),
		})
	}
	db.BatchHSet("map", batches...)

	// get
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		val, err := db.HGet("map", k)
		assert.Equal(val, v)
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < N; i++ {
		k, v := genKV(i)
		val, err := db2.HGet("map", k)
		assert.Equal(val, v)
		assert.Nil(err)
	}
}

func TestBatchZSet(t *testing.T) {
	assert := assert.New(t)
	const N = 1000
	db, _ := createDB()

	batches := make([]*ZSBatch, 0, N)
	for i := 0; i < N; i++ {
		k, _ := genKV(i)
		batches = append(batches, &ZSBatch{
			Key:   k,
			Score: int64(i),
		})
	}
	db.BatchZSet("zs", batches...)

	// get
	for i := 0; i < N; i++ {
		k, _ := genKV(i)
		score, err := db.ZGet("zs", k)
		assert.Equal(score, int64(i))
		assert.Nil(err)
	}

	db.Shrink()
	db.Close()

	// reopen
	db2, _ := Open(db.GetOptions())
	for i := 0; i < N; i++ {
		k, _ := genKV(i)
		score, err := db2.ZGet("zs", k)
		assert.Equal(score, int64(i))
		assert.Nil(err)
	}
}
