package zset

import (
	"github.com/stretchr/testify/assert"
	"github.com/xgzlucario/rotom/internal/iface"
	"testing"
)

func TestZSet(t *testing.T) {
	t.Run("zset", func(t *testing.T) {
		testZSet(t, New())
	})
	t.Run("zipzset", func(t *testing.T) {
		testZSet(t, NewZipZSet())
	})
}

func testZSet(t *testing.T, zs iface.ZSetI) {
	ast := assert.New(t)

	zs.Set("xgz", 100)
	zs.Set("abc", 100)
	zs.Set("xxx", -300)
	zs.Set("www", 200)
	zs.Set("xgz", 400)

	//zs.Scan(func(key string, score float64) {
	//	t.Error(key, score)
	//})

	/*
	   zset_test.go:28: xxx -300
	   zset_test.go:28: abc 100
	   zset_test.go:28: www 200
	   zset_test.go:28: xgz 400
	*/

	score, ok := zs.Get("xgz")
	ast.Equal(score, float64(400))
	ast.True(ok)

	ast.Equal(0, zs.Rank("xxx"))
	ast.Equal(1, zs.Rank("abc"))
	ast.Equal(2, zs.Rank("www"))
	ast.Equal(3, zs.Rank("xgz"))
}
