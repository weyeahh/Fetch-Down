package limiter

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	rate       float64 // >0: rate-limited, 0: unlimited
	lastRefill time.Time
	stopped    bool
}

func NewTokenBucket(rate float64, burstSize float64) *TokenBucket {
	if rate <= 0 {
		return &TokenBucket{
			tokens:     0,
			maxTokens:  0,
			rate:       0,
			lastRefill: time.Now(),
		}
	}

	if burstSize <= 0 {
		burstSize = rate * 0.5
	}

	return &TokenBucket{
		tokens:     burstSize,
		maxTokens:  burstSize,
		rate:       rate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) SetRate(rate float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if rate <= 0 {
		tb.rate = 0
		tb.stopped = false
		return
	}

	tb.stopped = false
	tb.rate = rate
	tb.maxTokens = rate * 0.5
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
}

func (tb *TokenBucket) Stop() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.stopped = true
	tb.rate = 0
	tb.maxTokens = 0
	tb.tokens = 0
}

func (tb *TokenBucket) IsStopped() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.stopped
}

func (tb *TokenBucket) GetRate() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

func (tb *TokenBucket) Allow(n int) int {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if tb.stopped {
		return 0
	}
	if tb.rate <= 0 {
		return n
	}

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return n
	}

	available := int(tb.tokens)
	if available <= 0 {
		return 0
	}
	tb.tokens = 0
	return available
}

func (tb *TokenBucket) Wait(ctx context.Context, n int) {
	tb.mu.Lock()

	for {
		if tb.stopped {
			tb.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			tb.mu.Lock()
			continue
		}

		if tb.rate <= 0 {
			tb.mu.Unlock()
			return
		}

		tb.refill()

		if tb.tokens >= float64(n) {
			tb.tokens -= float64(n)
			tb.mu.Unlock()
			return
		}

		deficit := float64(n) - tb.tokens
		waitTime := time.Duration(deficit/tb.rate*float64(time.Second)) + time.Millisecond
		if waitTime > 500*time.Millisecond {
			waitTime = 500 * time.Millisecond
		}
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(waitTime):
		}

		tb.mu.Lock()
	}
}

func (tb *TokenBucket) WaitN(ctx context.Context, n int) time.Duration {
	tb.mu.Lock()

	start := time.Now()

	for {
		if tb.stopped {
			tb.mu.Unlock()
			return time.Since(start)
		}
		if tb.rate <= 0 {
			tb.mu.Unlock()
			return time.Since(start)
		}

		tb.refill()

		if tb.tokens >= float64(n) {
			tb.tokens -= float64(n)
			tb.mu.Unlock()
			return time.Since(start)
		}

		deficit := float64(n) - tb.tokens
		waitTime := time.Duration(deficit/tb.rate*float64(time.Second)) + time.Millisecond
		if waitTime > 500*time.Millisecond {
			waitTime = 500 * time.Millisecond
		}
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return time.Since(start)
		case <-time.After(waitTime):
		}

		tb.mu.Lock()
	}
}

func (tb *TokenBucket) refill() {
	if tb.rate <= 0 {
		return
	}
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
}
