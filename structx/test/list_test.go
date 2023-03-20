package test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestList(t *testing.T) {
	l := structx.NewList(1, 2, 3)

	// Test LPush
	l.LPush(0)
	if !reflect.DeepEqual(l.Values(), []int{0, 1, 2, 3}) {
		t.Errorf("LPush error: expected [0 1 2 3], got %v", l.Values())
	}

	// Test RPush
	l.RPush(4)
	if !reflect.DeepEqual(l.Values(), []int{0, 1, 2, 3, 4}) {
		t.Errorf("RPush error: expected [0 1 2 3 4], got %v", l.Values())
	}

	// Test Insert
	l.Insert(2, 5, 6)
	if !reflect.DeepEqual(l.Values(), []int{0, 1, 5, 6, 2, 3, 4}) {
		t.Errorf("Insert error: expected [0 1 5 6 2 3  4], got %v", l.Values())
	}

	// Test LPop
	val, ok := l.LPop()
	if !ok || val != 0 || !reflect.DeepEqual(l.Values(), []int{1, 5, 6, 2, 3, 4}) {
		t.Errorf("LPop error: expected 0,true,[1 5 6 2 3 4], got %v,%v,%v", val, ok, l.Values())
	}

	// Test RPop
	val, ok = l.RPop()
	if !ok || val != 4 || !reflect.DeepEqual(l.Values(), []int{1, 5, 6, 2, 3}) {
		t.Errorf("RPop error: expected 4,true,[1 5 6 2 3], got %v,%v,%v", val, ok, l.Values())
	}

	// Test RemoveFirst
	if !l.RemoveFirst(5) || !reflect.DeepEqual(l.Values(), []int{1, 6, 2, 3}) {
		t.Errorf("RemoveFirst error: expected true,[1 6 2 3], got %v,%v", l.RemoveFirst(5), l.Values())
	}

	// Test RemoveIndex
	if !l.RemoveIndex(2) || !reflect.DeepEqual(l.Values(), []int{1, 6, 3}) {
		t.Errorf("RemoveIndex error: expected true,[1 6 3], got %v,%v", l.RemoveIndex(2), l.Values())
	}

	// Test Max
	max := l.Max(func(t1, t2 int) bool {
		return t1 < t2
	})
	if max != 6 {
		t.Errorf("Max error: expected 6, got %v", max)
	}

	// Test Min
	min := l.Min(func(t1, t2 int) bool {
		return t1 < t2
	})
	if min != 1 {
		t.Errorf("Min error: expected 1, got %v", min)
	}

	// Test Sort
	l.Sort(func(t1, t2 int) bool {
		return t1 < t2
	})
	if !reflect.DeepEqual(l.Values(), []int{1, 3, 6}) {
		t.Errorf("Sort error: expected [1 3 6], got %v", l.Values())
	}

	// Test IsSorted
	if !l.IsSorted(func(t1, t2 int) bool {
		return t1 < t2
	}) {
		t.Error("IsSorted error: expected true, got false")
	}

	// Test JSON marshal/unmarshal
	data, err := json.Marshal(l)
	if err != nil {
		t.Errorf("Marshal error: %v", err)
	}
	var l2 structx.List[int]
	err = json.Unmarshal(data, &l2)
	if err != nil || !reflect.DeepEqual(l.Values(), l2.Values()) {
		t.Errorf("Unmarshal error: %v, expected %v, got %v", err, l.Values(), l2.Values())
	}
}
