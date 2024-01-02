package rotom

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTicker(t *testing.T) {
	assert := assert.New(t)

	assert.Panics(func() {
		NewTicker(context.TODO(), -1, func() {})
	})

	ctx, cancel := context.WithCancel(context.Background())
	ticker := NewTicker(ctx, time.Second, func() {})
	ticker.Reset(time.Second)

	cancel()
	err := ticker.Do()
	assert.Equal(err, ErrTickerClosed)
}
