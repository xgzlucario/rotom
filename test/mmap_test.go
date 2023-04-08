package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestMMap(t *testing.T) {
	// create a new MMap
	m := structx.NewMMap()

	// set some values
	m.Set("value1", "key1")
	m.Set("value2", "key2", "key3")
	m.Set("value3", "key4", "key5", "key6")

	// test getting values
	val, ok := m.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Error getting value for key1")
	}
	val, ok = m.Get("key2", "key3")
	if !ok || val != "value2" {
		t.Errorf("Error getting value for key2, key3")
	}
	val, ok = m.Get("key4", "key5", "key6")
	if !ok || val != "value3" {
		t.Errorf("Error getting value for key4, key5, key6")
	}

	// test removing values
	m.Remove("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Errorf("Error removing value for key1")
	}
	m.Remove("key2", "key3")
	_, ok = m.Get("key2", "key3")
	if ok {
		t.Errorf("Error removing value for key2, key3")
	}
	m.Remove("key4", "key5", "key6")
	_, ok = m.Get("key4", "key5", "key6")
	if ok {
		t.Errorf("Error removing value for key4, key5, key6")
	}
}
