package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveRateLimiter implements rate limiting that adapts to target QPS changes.
type AdaptiveRateLimiter struct {
	controller *LoadController

	// Token bucket for rate limiting
	tokens       chan struct{}
	currentLimit atomic.Int64

	// Refill goroutine control
	done   chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool

	// Metrics
	acquired atomic.Int64
	rejected atomic.Int64
}

// NewAdaptiveRateLimiter creates a new rate limiter that adapts to controller's target QPS.
func NewAdaptiveRateLimiter(lc *LoadController) *AdaptiveRateLimiter {
	targetQPS := lc.GetTargetQPS()
	if targetQPS < 1 {
		targetQPS = 1
	}

	// Buffer size based on initial target (allow burst)
	bufferSize := targetQPS
	if bufferSize > 10000 {
		bufferSize = 10000
	}

	r := &AdaptiveRateLimiter{
		controller: lc,
		tokens:     make(chan struct{}, bufferSize),
		done:       make(chan struct{}),
	}
	r.currentLimit.Store(int64(targetQPS))

	return r
}

// Start begins the token refill goroutine.
func (r *AdaptiveRateLimiter) Start(ctx context.Context) {
	r.wg.Add(1)
	go r.refillLoop(ctx)
}

// Stop stops the rate limiter.
func (r *AdaptiveRateLimiter) Stop() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	close(r.done)
	r.mu.Unlock()

	r.wg.Wait()
}

// refillLoop adds tokens at the rate specified by the controller.
func (r *AdaptiveRateLimiter) refillLoop(ctx context.Context) {
	defer r.wg.Done()

	// Use high-resolution ticker for smooth rate limiting
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	tokensPerTick := 0.0
	accumulatedTokens := 0.0

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
			// Get current target and adapt
			targetQPS := r.controller.GetTargetQPS()
			if targetQPS < 1 {
				targetQPS = 1
			}
			r.currentLimit.Store(int64(targetQPS))

			// Calculate tokens per 10ms tick: targetQPS / 100
			tokensPerTick = float64(targetQPS) / 100.0
			accumulatedTokens += tokensPerTick

			// Add whole tokens
			for accumulatedTokens >= 1.0 {
				select {
				case r.tokens <- struct{}{}:
					accumulatedTokens -= 1.0
				default:
					// Buffer full, discard excess
					accumulatedTokens = 0
				}
			}
		}
	}
}

// Wait blocks until a token is available or context is cancelled.
func (r *AdaptiveRateLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return context.Canceled
	case <-r.tokens:
		r.acquired.Add(1)
		return nil
	}
}

// WaitWithTimeout waits for a token with a timeout.
func (r *AdaptiveRateLimiter) WaitWithTimeout(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return context.Canceled
	case <-timer.C:
		return context.DeadlineExceeded
	case <-r.tokens:
		r.acquired.Add(1)
		return nil
	}
}

// Acquire attempts to get a token without blocking.
// Returns true if a token was acquired, false otherwise.
func (r *AdaptiveRateLimiter) Acquire() bool {
	select {
	case <-r.tokens:
		r.acquired.Add(1)
		return true
	default:
		r.rejected.Add(1)
		return false
	}
}

// TryAcquire is an alias for Acquire for API compatibility.
func (r *AdaptiveRateLimiter) TryAcquire() bool {
	return r.Acquire()
}

// GetCurrentLimit returns the current rate limit (QPS).
func (r *AdaptiveRateLimiter) GetCurrentLimit() int {
	return int(r.currentLimit.Load())
}

// GetAvailableTokens returns the number of tokens currently available.
func (r *AdaptiveRateLimiter) GetAvailableTokens() int {
	return len(r.tokens)
}

// GetStats returns rate limiter statistics.
func (r *AdaptiveRateLimiter) GetStats() RateLimiterStats {
	return RateLimiterStats{
		Acquired:        r.acquired.Load(),
		Rejected:        r.rejected.Load(),
		CurrentLimit:    r.GetCurrentLimit(),
		AvailableTokens: r.GetAvailableTokens(),
	}
}

// RateLimiterStats contains rate limiter statistics.
type RateLimiterStats struct {
	Acquired        int64
	Rejected        int64
	CurrentLimit    int
	AvailableTokens int
}

// SimpleRateLimiter provides a basic fixed-rate limiter without dynamic adjustment.
type SimpleRateLimiter struct {
	qps    int
	ticker *time.Ticker
	done   chan struct{}
	mu     sync.Mutex
}

// NewSimpleRateLimiter creates a simple fixed-rate limiter.
func NewSimpleRateLimiter(qps int) *SimpleRateLimiter {
	if qps < 1 {
		qps = 1
	}
	interval := time.Second / time.Duration(qps)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	return &SimpleRateLimiter{
		qps:    qps,
		ticker: time.NewTicker(interval),
		done:   make(chan struct{}),
	}
}

// Wait blocks until the next tick.
func (r *SimpleRateLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return context.Canceled
	case <-r.ticker.C:
		return nil
	}
}

// Stop stops the rate limiter.
func (r *SimpleRateLimiter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.done:
	default:
		close(r.done)
	}
	r.ticker.Stop()
}
