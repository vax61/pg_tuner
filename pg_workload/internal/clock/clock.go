package clock

import "time"

// Clock provides time-related operations that can be real or simulated.
type Clock interface {
	// Now returns the current time according to this clock.
	Now() time.Time

	// Since returns the duration elapsed since t according to this clock.
	Since(t time.Time) time.Duration

	// Sleep pauses execution for the specified duration according to this clock.
	// For simulated clocks with timeScale > 1, actual sleep time is d/timeScale.
	Sleep(d time.Duration)

	// Ticker returns a ticker that emits at the specified interval according to this clock.
	// For simulated clocks with timeScale > 1, actual interval is d/timeScale.
	Ticker(d time.Duration) *Ticker

	// After returns a channel that receives the current time after duration d.
	After(d time.Duration) <-chan time.Time

	// Done returns a channel that is closed when the clock is stopped.
	Done() <-chan struct{}

	// Stop stops the clock and releases resources.
	Stop()

	// TimeScale returns the time scale factor (1 for real clock).
	TimeScale() int

	// IsSimulated returns true if this is a simulated clock.
	IsSimulated() bool
}

// New creates a new Clock based on the mode and parameters.
func New(simulated bool, startTime time.Time, timeScale int) Clock {
	if simulated {
		return NewSimulatedClock(startTime, timeScale)
	}
	return NewRealClock()
}
