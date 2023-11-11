package base

import (
	"context"
	"time"
)

// Ticker
type Ticker struct {
	ticker *time.Ticker
	ctx    context.Context
	f      func()
	reset  chan struct{}
}

// NewTicker return a ticker.
func NewTicker(ctx context.Context, interval time.Duration, f func()) *Ticker {
	if interval <= 0 {
		panic("invalid interval")
	}

	t := &Ticker{
		ticker: time.NewTicker(interval),
		ctx:    ctx,
		f:      f,
		reset:  make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-t.ticker.C:
				f()

			case <-t.reset:
				t.ticker.Reset(interval)

			case <-ctx.Done():
				return
			}
		}
	}()

	return t
}

func (t *Ticker) Do() error {
	select {
	case <-t.ctx.Done():
		return ErrDatabaseClosed

	default:
		t.f()
		return nil
	}
}
