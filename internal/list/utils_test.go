package list

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtils(t *testing.T) {
	assert := assert.New(t)

	t.Run("appendVarint", func(t *testing.T) {
		for i := 0; i < math.MaxUint16; i++ {
			// append varint
			b1 := binary.AppendUvarint(nil, uint64(i))
			b2 := appendUvarint(nil, i, false)
			b3 := appendUvarint(nil, i, true)
			b4 := slices.Clone(b3)
			slices.Reverse(b4)

			assert.Equal(b1, b2)
			assert.Equal(b1, b2)
			assert.Equal(b1, b4)

			// read uvarint
			x1, s1 := binary.Uvarint(b1)
			x2, s2 := uvarintReverse(b3)
			x3, s3 := uvarintReverse(append([]byte("something"), b3...))

			assert.Equal(x1, x2)
			assert.Equal(x1, x3)

			assert.Equal(s1, s2)
			assert.Equal(s1, s3)
		}
	})
}
