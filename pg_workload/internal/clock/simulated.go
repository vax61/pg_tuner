package clock

import (
	"sync"
	"time"
)

// SimulatedClock implements Clock with time acceleration.
// For timeScale > 1, time passes faster: 1 real second = timeScale simulated seconds.
type SimulatedClock struct {
	startRealTime time.Time
	startSimTime  time.Time
	timeScale     int
	done          chan struct{}
	mu            sync.Mutex
}

// NewSimulatedClock creates a new SimulatedClock.
// startTime is the initial simulated time.
// timeScale determines how fast time passes (1 = real-time, 24 = 24x faster).
func NewSimulatedClock(startTime time.Time, timeScale int) *SimulatedClock {
	if timeScale < 1 {
		timeScale = 1
	}
	return &SimulatedClock{
		startRealTime: time.Now(),
		startSimTime:  startTime,
		timeScale:     timeScale,
		done:          make(chan struct{}),
	}
}

// Now returns the current simulated time.
// Simulated time = startSimTime + (elapsed real time * timeScale)
func (c *SimulatedClock) Now() time.Time {
	elapsed := time.Since(c.startRealTime)
	simulatedElapsed := time.Duration(int64(elapsed) * int64(c.timeScale))
	return c.startSimTime.Add(simulatedElapsed)
}

// Since returns the simulated duration elapsed since t.
func (c *SimulatedClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Sleep pauses execution for the specified simulated duration.
// Actual sleep time = d / timeScale
func (c *SimulatedClock) Sleep(d time.Duration) {
	realDuration := time.Duration(int64(d) / int64(c.timeScale))
	if realDuration < time.Millisecond {
		realDuration = time.Millisecond // Minimum sleep to avoid busy-waiting
	}
	select {
	case <-time.After(realDuration):
	case <-c.done:
	}
}

// Ticker returns a ticker that emits at the specified simulated interval.
// Actual interval = d / timeScale
func (c *SimulatedClock) Ticker(d time.Duration) *Ticker {
	realInterval := time.Duration(int64(d) / int64(c.timeScale))
	if realInterval < time.Millisecond {
		realInterval = time.Millisecond // Minimum interval
	}

	// Create a channel that will emit simulated times
	ch := make(chan time.Time, 1)
	stopCh := make(chan struct{})

	go func() {
		ticker := time.NewTicker(realInterval)
		defer ticker.Stop()
		defer close(ch)

		for {
			select {
			case <-ticker.C:
				select {
				case ch <- c.Now():
				default:
					// Drop if consumer is slow
				}
			case <-stopCh:
				return
			case <-c.done:
				return
			}
		}
	}()

	return &Ticker{
		C:      ch,
		stopCh: stopCh,
		done:   c.done,
	}
}

// After returns a channel that receives the simulated time after duration d.
// Actual wait time = d / timeScale
func (c *SimulatedClock) After(d time.Duration) <-chan time.Time {
	realDuration := time.Duration(int64(d) / int64(c.timeScale))
	if realDuration < time.Millisecond {
		realDuration = time.Millisecond
	}

	ch := make(chan time.Time, 1)
	go func() {
		select {
		case <-time.After(realDuration):
			ch <- c.Now()
		case <-c.done:
		}
	}()
	return ch
}

// Done returns a channel that is closed when the clock is stopped.
func (c *SimulatedClock) Done() <-chan struct{} {
	return c.done
}

// Stop stops the clock and signals all waiting operations.
func (c *SimulatedClock) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.done:
		// Already stopped
	default:
		close(c.done)
	}
}

// TimeScale returns the time scale factor.
func (c *SimulatedClock) TimeScale() int {
	return c.timeScale
}

// IsSimulated returns true for simulated clock.
func (c *SimulatedClock) IsSimulated() bool {
	return true
}

// SimulatedDuration converts a real duration to simulated duration.
func (c *SimulatedClock) SimulatedDuration(realDuration time.Duration) time.Duration {
	return time.Duration(int64(realDuration) * int64(c.timeScale))
}

// RealDuration converts a simulated duration to real duration.
func (c *SimulatedClock) RealDuration(simDuration time.Duration) time.Duration {
	return time.Duration(int64(simDuration) / int64(c.timeScale))
}
