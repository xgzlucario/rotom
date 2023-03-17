package structx_test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestSkiplist(t *testing.T) {
	sl := structx.NewSkipList[int, string]()

	// Insert a new node and check its existence
	key1 := 7
	value1 := "value1"
	sl.Insert(key1, value1)
	if sl.Len() != 1 {
		t.Fatalf("unexpected len %d, expected 1", sl.Len())
	}
	if n := sl.Iter(); n == nil || n.Key() != key1 || n.Value() != value1 {
		t.Fatalf("unexpected node %v, expected key=%d value=%s", n, key1, value1)
	}

	// Insert a node with the same key and check that the value is updated
	value2 := "value2"
	sl.Insert(key1, value2)
	if sl.Len() != 1 {
		t.Fatalf("unexpected len %d, expected 1", sl.Len())
	}
	if n := sl.Iter(); n == nil || n.Key() != key1 || n.Value() != value2 {
		t.Fatalf("unexpected node %v, expected key=%d value=%s", n, key1, value2)
	}

	// Insert another node with a different key and check that it exists
	key2 := 9
	value3 := "value3"
	sl.Insert(key2, value3)
	if sl.Len() != 2 {
		t.Fatalf("unexpected len %d, expected 2", sl.Len())
	}
	if n := sl.Iter(); n == nil || n.Key() != key1 || n.Value() != value2 {
		t.Fatalf("unexpected node %v, expected key=%d value=%s", n, key1, value2)
	}
	n := sl.Iter()
	if n = n.Next(); n == nil || n.Key() != key2 || n.Value() != value3 {
		t.Fatalf("unexpected node %v, expected key=%d value=%s", n, key2, value3)
	}

	// Delete a node and check its absence
	if !sl.Delete(key1) {
		t.Fatalf("failed to delete key %d", key1)
	}
	if sl.Len() != 1 {
		t.Fatalf("unexpected len %d, expected 1", sl.Len())
	}
	if n := sl.Iter(); n == nil || n.Key() != key2 || n.Value() != value3 {
		t.Fatalf("unexpected node %v, expected key=%d value=%s", n, key2, value3)
	}

	// Try to delete a non-existent node and check that it fails
	if sl.Delete(key1) {
		t.Fatalf("deleted non-existent key %d", key1)
	}
}
