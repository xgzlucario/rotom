package hash

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Regression: Migrate used a pooled buffer without truncating it, so the
// "compacted" data kept len(old) bytes of garbage in front and grew instead.
func TestZipMapMigrateShrinks(t *testing.T) {
	ast := assert.New(t)
	zm := New()
	for i := range 100 {
		zm.Set(fmt.Sprintf("k%d", i), []byte("v"))
	}
	// rewrite every value with a different length many times
	for round := range 200 {
		val := make([]byte, 1+round%2)
		for i := range 100 {
			zm.Set(fmt.Sprintf("k%d", i), val)
		}
	}
	live := 0
	zm.Scan(func(_ string, val []byte) { live += 1 + len(val) })
	ast.LessOrEqual(len(zm.data), 2*live+migrateThresholdSize*2)
	ast.Equal(100, zm.Len())
}
