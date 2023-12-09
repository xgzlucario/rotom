package base

import (
	"context"
	"errors"
	"time"
)

var (
	ErrTickerClosed = errors.New("ticker closed")
)

// Ticker
type Ticker struct {
	ticker *time.Ticker
	ctx    context.Context
	f      func()
	reset  chan time.Duration
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
		reset:  make(chan time.Duration),
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

// Do
func (t *Ticker) Do() error {
	select {
	case <-t.ctx.Done():
		return ErrTickerClosed

	default:
		t.f()
		return nil
	}
}

// Reset
func (t *Ticker) Reset(interval time.Duration) {
	t.reset <- interval
}
