package zset

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ZipZSet(t *testing.T) {
	ast := assert.New(t)

	zs := NewZipZSet()
	zs.Set("xgz", 100)
	zs.Set("abc", 100)
	zs.Set("xxx", 300)
	zs.Set("www", 200)

	score, ok := zs.Get("xgz")
	ast.Equal(score, float64(100))
	ast.True(ok)
}
