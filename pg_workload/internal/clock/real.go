package clock

import "time"

// RealClock implements Clock using actual system time.
type RealClock struct {
	done chan struct{}
}

// NewRealClock creates a new RealClock.
func NewRealClock() *RealClock {
	return &RealClock{
		done: make(chan struct{}),
	}
}

// Now returns the current system time.
func (c *RealClock) Now() time.Time {
	return time.Now()
}

// Since returns the duration elapsed since t.
func (c *RealClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

// Sleep pauses execution for the specified duration.
func (c *RealClock) Sleep(d time.Duration) {
	select {
	case <-time.After(d):
	case <-c.done:
	}
}

// Ticker returns a ticker that emits at the specified interval.
func (c *RealClock) Ticker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{
		C:          t.C,
		realTicker: t,
		done:       c.done,
	}
}

// After returns a channel that receives the current time after duration d.
func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Done returns a channel that is closed when the clock is stopped.
func (c *RealClock) Done() <-chan struct{} {
	return c.done
}

// Stop stops the clock.
func (c *RealClock) Stop() {
	select {
	case <-c.done:
		// Already stopped
	default:
		close(c.done)
	}
}

// TimeScale returns 1 for real clock.
func (c *RealClock) TimeScale() int {
	return 1
}

// IsSimulated returns false for real clock.
func (c *RealClock) IsSimulated() bool {
	return false
}
