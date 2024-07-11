package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func genListPack(start, end int) *ListPack {
	lp := NewListPack()
	for i := start; i < end; i++ {
		lp.RPush(genKey(i))
	}
	return lp
}

func genKey(i int) string {
	return fmt.Sprintf("%06x", i)
}

func lp2list(lp *ListPack) (res []string) {
	it := lp.NewIterator()
	for !it.IsEnd() {
		res = append(res, string(it.Next()))
	}
	return
}

func TestIterator(t *testing.T) {
	assert := assert.New(t)
	lp := NewListPack()
	it := lp.NewIterator()

	lp.RPush("001", "002", "003")
	assert.Equal(lp2list(lp), []string{"001", "002", "003"})

	// lpush
	lp.LPush("004")
	assert.Equal(lp2list(lp), []string{"004", "001", "002", "003"})

	// next
	data := it.Next()
	assert.Equal(string(data), "004")

	// remove
	removed := it.RemoveNext()
	assert.Equal(string(removed), "001")
	assert.Equal(lp2list(lp), []string{"004", "002", "003"})

	removed = it.RemovePrev()
	assert.Equal(string(removed), "004")
	assert.Equal(lp2list(lp), []string{"002", "003"})
}
