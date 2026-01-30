package clock

import (
	"testing"
	"time"
)

func TestRealClock_Now(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	before := time.Now()
	now := c.Now()
	after := time.Now()

	if now.Before(before) || now.After(after) {
		t.Errorf("RealClock.Now() returned time outside expected range")
	}
}

func TestRealClock_Since(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	start := c.Now()
	time.Sleep(50 * time.Millisecond)
	elapsed := c.Since(start)

	if elapsed < 50*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("RealClock.Since() = %v, expected ~50ms", elapsed)
	}
}

func TestRealClock_Sleep(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	start := time.Now()
	c.Sleep(50 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("RealClock.Sleep() took %v, expected ~50ms", elapsed)
	}
}

func TestRealClock_SleepInterrupted(t *testing.T) {
	c := NewRealClock()

	done := make(chan struct{})
	go func() {
		c.Sleep(1 * time.Second)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	c.Stop()

	select {
	case <-done:
		// Good, sleep was interrupted
	case <-time.After(200 * time.Millisecond):
		t.Error("RealClock.Sleep() was not interrupted by Stop()")
	}
}

func TestRealClock_Ticker(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	ticker := c.Ticker(50 * time.Millisecond)
	defer ticker.Stop()

	count := 0
	timeout := time.After(300 * time.Millisecond)

loop:
	for {
		select {
		case <-ticker.C:
			count++
			if count >= 3 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if count < 3 {
		t.Errorf("RealClock.Ticker() only ticked %d times, expected at least 3", count)
	}
}

func TestRealClock_TimeScale(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	if scale := c.TimeScale(); scale != 1 {
		t.Errorf("RealClock.TimeScale() = %d, expected 1", scale)
	}
}

func TestRealClock_IsSimulated(t *testing.T) {
	c := NewRealClock()
	defer c.Stop()

	if c.IsSimulated() {
		t.Error("RealClock.IsSimulated() = true, expected false")
	}
}

func TestSimulatedClock_TimeScale1(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 1)
	defer c.Stop()

	if scale := c.TimeScale(); scale != 1 {
		t.Errorf("SimulatedClock.TimeScale() = %d, expected 1", scale)
	}

	// With timeScale=1, simulated time should advance at same rate as real time
	simStart := c.Now()
	time.Sleep(100 * time.Millisecond)
	elapsed := c.Since(simStart)

	// Allow some tolerance
	if elapsed < 80*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("SimulatedClock with timeScale=1: elapsed = %v, expected ~100ms", elapsed)
	}
}

func TestSimulatedClock_TimeScale4(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 4)
	defer c.Stop()

	if scale := c.TimeScale(); scale != 4 {
		t.Errorf("SimulatedClock.TimeScale() = %d, expected 4", scale)
	}

	// With timeScale=4, 100ms real = 400ms simulated
	simStart := c.Now()
	time.Sleep(100 * time.Millisecond)
	elapsed := c.Since(simStart)

	// Allow some tolerance: expect ~400ms, accept 300-600ms
	if elapsed < 300*time.Millisecond || elapsed > 600*time.Millisecond {
		t.Errorf("SimulatedClock with timeScale=4: elapsed = %v, expected ~400ms", elapsed)
	}
}

func TestSimulatedClock_TimeScale24(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 24)
	defer c.Stop()

	if scale := c.TimeScale(); scale != 24 {
		t.Errorf("SimulatedClock.TimeScale() = %d, expected 24", scale)
	}

	// With timeScale=24, 100ms real = 2400ms (2.4s) simulated
	simStart := c.Now()
	time.Sleep(100 * time.Millisecond)
	elapsed := c.Since(simStart)

	// Allow some tolerance: expect ~2.4s, accept 2-3s
	if elapsed < 2*time.Second || elapsed > 3*time.Second {
		t.Errorf("SimulatedClock with timeScale=24: elapsed = %v, expected ~2.4s", elapsed)
	}
}

func TestSimulatedClock_Sleep(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 4)
	defer c.Stop()

	// Sleep for 200ms simulated = 50ms real
	realStart := time.Now()
	c.Sleep(200 * time.Millisecond)
	realElapsed := time.Since(realStart)

	// Should sleep ~50ms real time (200ms / 4)
	if realElapsed < 40*time.Millisecond || realElapsed > 150*time.Millisecond {
		t.Errorf("SimulatedClock.Sleep(200ms) with timeScale=4 took %v real, expected ~50ms", realElapsed)
	}
}

func TestSimulatedClock_Ticker(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 4)
	defer c.Stop()

	// Ticker every 200ms simulated = 50ms real
	ticker := c.Ticker(200 * time.Millisecond)
	defer ticker.Stop()

	count := 0
	realStart := time.Now()
	timeout := time.After(300 * time.Millisecond) // Should get ~6 ticks in 300ms

loop:
	for {
		select {
		case <-ticker.C:
			count++
			if count >= 4 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	realElapsed := time.Since(realStart)

	// Should have gotten at least 4 ticks in ~200ms real time
	if count < 4 {
		t.Errorf("SimulatedClock.Ticker() only ticked %d times in %v, expected at least 4", count, realElapsed)
	}
}

func TestSimulatedClock_After(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 4)
	defer c.Stop()

	// After 200ms simulated = 50ms real
	realStart := time.Now()
	<-c.After(200 * time.Millisecond)
	realElapsed := time.Since(realStart)

	// Should have waited ~50ms real
	if realElapsed < 40*time.Millisecond || realElapsed > 150*time.Millisecond {
		t.Errorf("SimulatedClock.After(200ms) with timeScale=4 took %v real, expected ~50ms", realElapsed)
	}
}

func TestSimulatedClock_IsSimulated(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 1)
	defer c.Stop()

	if !c.IsSimulated() {
		t.Error("SimulatedClock.IsSimulated() = false, expected true")
	}
}

func TestSimulatedClock_StartTime(t *testing.T) {
	startTime := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 1)
	defer c.Stop()

	now := c.Now()
	diff := now.Sub(startTime)

	// Should be very close to start time (within a few ms of creation)
	if diff < 0 || diff > 50*time.Millisecond {
		t.Errorf("SimulatedClock.Now() = %v, expected close to %v (diff: %v)", now, startTime, diff)
	}
}

func TestSimulatedClock_DurationConversion(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewSimulatedClock(startTime, 4)
	defer c.Stop()

	// Test SimulatedDuration
	realDur := 1 * time.Hour
	simDur := c.SimulatedDuration(realDur)
	if simDur != 4*time.Hour {
		t.Errorf("SimulatedDuration(1h) with timeScale=4 = %v, expected 4h", simDur)
	}

	// Test RealDuration
	simDur = 4 * time.Hour
	realDur = c.RealDuration(simDur)
	if realDur != 1*time.Hour {
		t.Errorf("RealDuration(4h) with timeScale=4 = %v, expected 1h", realDur)
	}
}

func TestNew_RealClock(t *testing.T) {
	c := New(false, time.Time{}, 1)
	defer c.Stop()

	if c.IsSimulated() {
		t.Error("New(false, ...) returned simulated clock")
	}
	if c.TimeScale() != 1 {
		t.Errorf("RealClock.TimeScale() = %d, expected 1", c.TimeScale())
	}
}

func TestNew_SimulatedClock(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := New(true, startTime, 12)
	defer c.Stop()

	if !c.IsSimulated() {
		t.Error("New(true, ...) returned real clock")
	}
	if c.TimeScale() != 12 {
		t.Errorf("SimulatedClock.TimeScale() = %d, expected 12", c.TimeScale())
	}
}

func TestSimulatedClock_InvalidTimeScale(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// timeScale < 1 should default to 1
	c := NewSimulatedClock(startTime, 0)
	defer c.Stop()

	if c.TimeScale() != 1 {
		t.Errorf("SimulatedClock with timeScale=0 should default to 1, got %d", c.TimeScale())
	}

	c2 := NewSimulatedClock(startTime, -5)
	defer c2.Stop()

	if c2.TimeScale() != 1 {
		t.Errorf("SimulatedClock with timeScale=-5 should default to 1, got %d", c2.TimeScale())
	}
}
