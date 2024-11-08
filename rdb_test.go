package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRdb(t *testing.T) {
	ast := assert.New(t)
	rdb := NewRdb("main.go")
	ast.NotNil(rdb.LoadDB())
}
