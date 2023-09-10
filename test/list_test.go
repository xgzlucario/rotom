package test

import (
	"math/rand"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestList(t *testing.T) {
	ls := structx.NewList[int]()
	valid := make([]int, 0, 1024)

	// test empty pop
	_, ok := ls.LPop()
	if ok {
		t.Fatal("list should be empty")
	}
	_, ok = ls.RPop()
	if ok {
		t.Fatal("list should be empty")
	}

	// random insert
	for i := 0; i < 1000; i++ {
		elem := rand.Int()
		if i%2 == 0 {
			ls.RPush(elem)
			valid = append(valid, elem)

		} else {
			ls.LPush(elem)
			valid = append([]int{elem}, valid...)
		}
	}

	// test len
	if ls.Len() != 1000 {
		t.Fatal("list len error")
	}

	// test range
	i := 0
	ls.Range(func(elem int) bool {
		if elem != valid[i] {
			t.Fatal("list range error")
		}
		i++
		return true
	})

	// test index
	for i := 0; i < 1000; i++ {
		index := rand.Intn(ls.Len())

		elem, ok := ls.Index(index)
		if !ok {
			t.Fatal("index error")
		}
		if elem != valid[index] {
			t.Fatal("index error")
		}
	}

	// test pop
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			elem, ok := ls.LPop()
			if !ok || elem != valid[0] {
				t.Fatalf("list lpop: %d %d", elem, valid[0])
			}
			valid = valid[1:]

		} else {
			elem, ok := ls.RPop()
			if !ok || elem != valid[len(valid)-1] {
				t.Fatalf("list rpop: %d %d", elem, valid[len(valid)-1])
			}
			valid = valid[:len(valid)-1]
		}
	}
}

func BenchmarkList(b *testing.B) {
	b.Run("ziplist/RPush", func(b *testing.B) {
		ls := structx.NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.RPush(i)
		}
	})

	b.Run("ziplist/LPush", func(b *testing.B) {
		ls := structx.NewList[int]()
		for i := 0; i < b.N; i++ {
			ls.LPush(i)
		}
	})
}
