package structx

import (
	"encoding/json"
	"testing"
)

// CREATE BY CHATGPT

func TestLSet(t *testing.T) {
	// Create an LSet with some initial values
	lset := NewLSet(1, 2, 3)

	// Test Add and Exist methods
	lset.Add(4)
	if !lset.Exist(4) {
		t.Errorf("Expected 4 to exist in LSet, but it does not")
	}
	if lset.Add(4) {
		t.Errorf("Expected Add(4) to return false, but it did not")
	}

	// Test Remove method
	lset.Remove(4)
	if lset.Exist(4) {
		t.Errorf("Expected 4 to be removed from LSet, but it still exists")
	}
	if !lset.Remove(4) {
		t.Errorf("Expected Remove(4) to return true, but it did not")
	}

	// Test Copy method
	lset2 := lset.Copy()
	if !lset.Equal(lset2) {
		t.Errorf("Expected lset and lset2 to be equal, but they are not")
	}
	lset2.Add(5)
	if lset.Equal(lset2) {
		t.Errorf("Expected lset and lset2 to be different after adding 5 to lset2, but they are equal")
	}

	// Test Union method
	lset3 := NewLSet(3, 4, 5)
	lset4 := lset.Union(lset3)
	if lset4.Len() != 5 || !lset4.Exist(1) || !lset4.Exist(2) || !lset4.Exist(3) || !lset4.Exist(4) || !lset4.Exist(5) {
		t.Errorf("Expected lset4 to contain elements 1-5, but it does not")
	}

	// Test Intersect method
	lset5 := NewLSet(2, 3, 4)
	lset6 := lset.Intersect(lset5)
	if lset6.Len() != 2 || !lset6.Exist(2) || !lset6.Exist(3) {
		t.Errorf("Expected lset6 to contain elements 2 and 3, but it does not")
	}

	// Test Difference method
	lset7 := NewLSet(3, 4, 5)
	lset8 := lset.Difference(lset7)
	if lset8.Len() != 2 || !lset8.Exist(1) || !lset8.Exist(2) {
		t.Errorf("Expected lset8 to contain elements 1 and 2, but it does not")
	}

	// Test IsSubSet method
	lset9 := NewLSet(2, 3)
	if !lset9.IsSubSet(lset) {
		t.Errorf("Expected lset9 to be a subset of lset, but it is not")
	}
	if lset.IsSubSet(lset9) {
		t.Errorf("Expected lset to not be a subset of lset9, but it is")
	}

	// Test Pop methods
	key, ok := lset.LPop()
	if !ok || key != 1 || lset.Len() != 2 || lset.Exist(1) {
		t.Errorf("Expected LPop to remove the first element and return it, but it did not")
	}
}

// TestLSetDifference tests the Difference method of LSet
func TestLSetDifference(t *testing.T) {
	ls1 := NewLSet("a", "b", "c")
	ls2 := NewLSet("b", "c", "d")
	expected := NewLSet("a", "d")
	result := ls1.Difference(ls2)

	if !result.Equal(expected) {
		t.Errorf("Difference of %v and %v should be %v, but got %v", ls1, ls2, expected, result)
	}

	result2 := ls2.Difference(ls1)

	if !result2.Equal(NewLSet("d")) {
		t.Errorf("Difference of %v and %v should be %v, but got %v", ls2, ls1, NewLSet("d"), result2)
	}

	result3 := NewLSet("a").Difference(NewLSet[string]())

	if !result3.Equal(NewLSet("a")) {
		t.Errorf("Difference of %v and %v should be %v, but got %v", NewLSet("a"), NewLSet[string](), NewLSet("a"), result3)
	}
}

// TestLSetIsSubSet tests the IsSubSet method of LSet
func TestLSetIsSubSet(t *testing.T) {
	ls1 := NewLSet("a", "b", "c")
	ls2 := NewLSet("a", "b", "c", "d")
	ls3 := NewLSet("a", "b")
	ls4 := NewLSet("a", "b", "c", "d", "e")

	if !ls1.IsSubSet(ls2) {
		t.Errorf("%v should be subset of %v", ls1, ls2)
	}

	if ls2.IsSubSet(ls1) {
		t.Errorf("%v should not be subset of %v", ls2, ls1)
	}

	if !ls3.IsSubSet(ls1) {
		t.Errorf("%v should be subset of %v", ls3, ls1)
	}

	if !ls2.IsSubSet(ls4) {
		t.Errorf("%v should be subset of %v", ls2, ls4)
	}
}

// TestLSetRandomPop tests the RandomPop method of LSet
func TestLSetRandomPop(t *testing.T) {
	ls := NewLSet("a", "b", "c")
	count := make(map[string]int)
	for i := 0; i < 3; i++ {
		v, ok := ls.RandomPop()
		if !ok {
			t.Errorf("Unexpected empty set")
		}
		count[v]++
	}
	for _, c := range count {
		if c != 1 {
			t.Errorf("Expected each element to be popped exactly once, got count=%d", c)
		}
	}
	if _, ok := ls.RandomPop(); ok {
		t.Errorf("Expected empty set, but got non-empty set")
	}
}

// TestLSetMarshalUnmarshalJSON tests the MarshalJSON and UnmarshalJSON methods of LSet
func TestLSetMarshalUnmarshalJSON(t *testing.T) {
	ls := NewLSet("a", "b", "c")
	bytes, err := json.Marshal(ls)
	if err != nil {
		t.Errorf("Unexpected error while marshaling: %v", err)
	}
	var ls2 LSet[string]
	err = json.Unmarshal(bytes, &ls2)
	if err != nil {
		t.Errorf("Unexpected error while unmarshaling: %v", err)
	}
}
