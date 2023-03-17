package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestSkiplist(t *testing.T) {
	sl := structx.NewSkipList[int, string]()
	sl.Insert(1, "one")
	sl.Insert(2, "two")
	sl.Insert(3, "three")

	// Test Len()
	if sl.Len() != 3 {
		t.Errorf("Expected len() to be 3, but got %v", sl.Len())
	}

	// Test Iter() and Next()
	iter := sl.Iter()
	if iter.Key() != 1 || iter.Value() != "one" {
		t.Errorf("Expected first element to be (1, \"one\"), but got (%v, %v)", iter.Key(), iter.Value())
	}
	iter = iter.Next()
	if iter.Key() != 2 || iter.Value() != "two" {
		t.Errorf("Expected second element to be (2, \"two\"), but got (%v, %v)", iter.Key(), iter.Value())
	}
	iter = iter.Next()
	if iter.Key() != 3 || iter.Value() != "three" {
		t.Errorf("Expected third element to be (3, \"three\"), but got (%v, %v)", iter.Key(), iter.Value())
	}

	// Test Delete()
	if !sl.Delete(2) {
		t.Errorf("Expected Delete(2) to return true")
	}
	if sl.Len() != 2 {
		t.Errorf("Expected len() to be 2 after deleting one element, but got %v", sl.Len())
	}
	iter = sl.Iter()
	if iter.Key() != 1 || iter.Value() != "one" {
		t.Errorf("Expected first element after deleting to be (1, \"one\"), but got (%v, %v)", iter.Key(), iter.Value())
	}
	iter = iter.Next()
	if iter.Key() != 3 || iter.Value() != "three" {
		t.Errorf("Expected second element after deleting to be (3, \"three\"), but got (%v, %v)", iter.Key(), iter.Value())
	}

	// Test Insert with same key
	if sl.Insert(3, "new three").Value() != "new three" {
		t.Errorf("Expected Insert with same key to update the value")
	}
}
