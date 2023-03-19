package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/structx"
)

func TestZSet(t *testing.T) {
	zs := structx.NewZSet[string, int, string]()

	// Test Set and Get
	zs.Set("a", "val1")
	val, score, ok := zs.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "val1", val)
	assert.Equal(t, 0, score)

	// Test SetScore and Get
	zs.SetScore("a", 10)
	val, score, ok = zs.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "val1", val)
	assert.Equal(t, 10, score)

	// Test SetWithScore and Get
	zs.SetWithScore("b", 5, "val2")
	val, score, ok = zs.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "val2", val)
	assert.Equal(t, 5, score)

	// Test Incr
	newScore := zs.Incr("a", 5)
	val, score, ok = zs.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 15, newScore)
	assert.Equal(t, 15, score)
}
