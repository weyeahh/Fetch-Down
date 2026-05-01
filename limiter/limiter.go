package limiter

import (
	"sync"
	"time"
)

// TokenBucket implements a token bucket rate limiter for controlling download speed.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64   // current available tokens (bytes)
	maxTokens  float64   // max tokens (burst size in bytes)
	rate       float64   // tokens per second (bytes/sec), 0 = unlimited
	lastRefill time.Time
}

// NewTokenBucket creates a new rate limiter.
// rate is in bytes per second, 0 means unlimited.
// burstSize is the max burst in bytes (defaults to 1 second of rate if <= 0).
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
		burstSize = rate // 1 second burst
	}

	return &TokenBucket{
		tokens:     burstSize,
		maxTokens:  burstSize,
		rate:       rate,
		lastRefill: time.Now(),
	}
}

// SetRate updates the rate limit dynamically (bytes per second).
// 0 means unlimited.
func (tb *TokenBucket) SetRate(rate float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if rate <= 0 {
		tb.rate = 0
		return
	}

	tb.rate = rate
	tb.maxTokens = rate // 1 second burst
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
}

// GetRate returns the current rate in bytes per second.
func (tb *TokenBucket) GetRate() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

// Allow checks if n bytes can be consumed. If not, it waits until enough tokens
// are available or returns false if the context is done.
// Returns the actual amount allowed (may be less than n if partial is available).
func (tb *TokenBucket) Allow(n int) int {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// unlimited
	if tb.rate <= 0 {
		return n
	}

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return n
	}

	// return what's available
	available := int(tb.tokens)
	if available <= 0 {
		return 0
	}
	tb.tokens = 0
	return available
}

// Wait blocks until n bytes worth of tokens are available.
func (tb *TokenBucket) Wait(n int) {
	tb.mu.Lock()

	if tb.rate <= 0 {
		tb.mu.Unlock()
		return
	}

	tb.refill()

	for tb.tokens < float64(n) {
		deficit := float64(n) - tb.tokens
		waitTime := time.Duration(deficit/tb.rate*float64(time.Second)) + time.Millisecond
		tb.mu.Unlock()
		time.Sleep(waitTime)
		tb.mu.Lock()
		tb.refill()
	}

	tb.tokens -= float64(n)
	tb.mu.Unlock()
}

// WaitN blocks until n bytes worth of tokens are available, returning the wait duration.
func (tb *TokenBucket) WaitN(n int) time.Duration {
	tb.mu.Lock()

	if tb.rate <= 0 {
		tb.mu.Unlock()
		return 0
	}

	tb.refill()

	start := time.Now()
	for tb.tokens < float64(n) {
		deficit := float64(n) - tb.tokens
		waitTime := time.Duration(deficit/tb.rate*float64(time.Second)) + time.Millisecond
		tb.mu.Unlock()
		time.Sleep(waitTime)
		tb.mu.Lock()
		tb.refill()
	}

	tb.tokens -= float64(n)
	tb.mu.Unlock()
	return time.Since(start)
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
}
