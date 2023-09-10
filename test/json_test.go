package test

import (
	"testing"

	"github.com/xgzlucario/rotom/structx"
)

func TestJson(t *testing.T) {
	j := structx.NewJson()
	j.SetBytes("xgz", []byte("23"))
	j.SetBytes("xgz1", []byte("30"))
	j.SetBytes("xgz", []byte("24"))

	// Get xgz
	s, ok := j.Get("xgz").(string)
	if !ok {
		t.Fatalf("get xgz failed: %v", s)
	}
	if s != "24" {
		t.Fatalf("string is not 24: %s", s)
	}

	// Get xgz1
	s, ok = j.Get("xgz1").(string)
	if !ok {
		t.Fatalf("get xgz failed: %v", s)
	}
	if s != "30" {
		t.Fatalf("string is not 30: %s", s)
	}

	// Get xgz2
	_, ok = j.Get("xgz2").(string)
	if ok {
		t.Fatalf("get xgz2 should not exist")
	}

	// Delete
	err := j.Delete("xgz")
	if err != nil {
		t.Fatalf("delete xgz failed: %v", err)
	}
}
