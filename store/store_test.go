package store

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkSet(b *testing.B) {
	db := DB(2)
	for i := 0; i < b.N; i++ {
		db.Set("xgz"+strconv.Itoa(i), i)
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	db := DB(3)
	for i := 0; i < b.N; i++ {
		db.SetWithTTL("xgz"+strconv.Itoa(i), i, time.Second)
	}
}
