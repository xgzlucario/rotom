package test

import (
	"encoding/json"
	"testing"

	"github.com/xgzlucario/rotom/structx"
	"golang.org/x/exp/slices"
)

func TestList(t *testing.T) {
	t.Parallel()

	list := structx.NewList(1, 2, 3, 4, 5)

	// Test LPush
	list.LPush(0)
	if list.Len() != 6 || list.Index(0) != 0 {
		t.Errorf("LPush error: %v\n", list.Values())
	}

	// Test RPush
	list.RPush(6)
	if list.Len() != 7 || list.Index(6) != 6 {
		t.Errorf("RPush error: %v\n", list.Values())
	}

	// Test Insert
	list.Insert(3, 99)
	if list.Len() != 8 || list.Index(3) != 99 {
		t.Errorf("Insert error: %v\n", list.Values())
	}

	// Test LPop
	val, ok := list.LPop()
	if !ok || val != 0 || list.Len() != 7 {
		t.Errorf("LPop error: %v\n", list.Values())
	}

	// Test RPop
	val, ok = list.RPop()
	if !ok || val != 6 || list.Len() != 6 {
		t.Errorf("RPop error: %v\n", list.Values())
	}

	// Test RemoveFirst
	if !list.RemoveFirst(3) || list.Len() != 5 {
		t.Errorf("RemoveFirst error: %v\n", list.Values())
	}
	if list.RemoveFirst(7) || list.Len() != 5 {
		t.Errorf("RemoveFirst error: %v\n", list.Values())
	}

	// Test RemoveIndex
	if !list.RemoveIndex(2) || list.Len() != 4 {
		t.Errorf("RemoveIndex error: %v\n", list.Values())
	}
	if list.RemoveIndex(7) || list.Len() != 4 {
		t.Errorf("RemoveIndex error: %v\n", list.Values())
	}

	// Test Max and Min
	max := list.Max(func(a, b int) bool { return a < b })
	if max != 5 {
		t.Errorf("Max error: %v\n", max)
	}
	min := list.Min(func(a, b int) bool { return a < b })
	if min != 1 {
		t.Errorf("Min error: %v\n", min)
	}

	// Test Sort
	list.Sort(func(a, b int) bool { return a > b })
	if !list.IsSorted(func(a, b int) bool { return a > b }) {
		t.Errorf("Sort error: %v\n", list.Values())
	}

	// Test JSON
	bytes, err := json.Marshal(list)
	if err != nil {
		t.Errorf("MarshalJSON error: %v", err)
	}
	unmarshaledList := structx.NewList(0)
	err = json.Unmarshal(bytes, unmarshaledList)
	if err != nil || !slices.Equal(list.Values(), unmarshaledList.Values()) {
		t.Errorf("UnmarshalJSON error: %v\n%v\n%v", err, list.Values(), unmarshaledList.Values())
	}
}
