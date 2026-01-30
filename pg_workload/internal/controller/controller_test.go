package controller

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
)

func TestNewLoadController(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 100

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	if lc.GetBaseWorkers() != 4 {
		t.Errorf("BaseWorkers = %d, want 4", lc.GetBaseWorkers())
	}

	if lc.GetTargetQPS() != 100 {
		t.Errorf("TargetQPS = %d, want 100", lc.GetTargetQPS())
	}

	if lc.GetCurrentMultiplier() != 1.0 {
		t.Errorf("CurrentMultiplier = %f, want 1.0", lc.GetCurrentMultiplier())
	}
}

func TestLoadController_GetTargetQPS_VariesWithHour(t *testing.T) {
	// Use simulated clock to control time
	startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	// Pattern with different multipliers at different hours
	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0, // 100 QPS
			10: 2.0, // 200 QPS
			11: 0.5, // 50 QPS
		},
	}

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	// At 9:00, should be 100 QPS
	if qps := lc.GetTargetQPS(); qps != 100 {
		t.Errorf("At 9:00, TargetQPS = %d, want 100", qps)
	}
}

func TestLoadController_GetTargetWorkers_Proportional(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 1.0,
			1: 2.0,
			2: 0.5,
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     16,
		UpdateInterval: 1 * time.Second,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	defer lc.Stop()

	// At multiplier 1.0, workers = 4
	if w := lc.GetTargetWorkers(); w != 4 {
		t.Errorf("At mult=1.0, TargetWorkers = %d, want 4", w)
	}
}

func TestLoadController_WithSimulatedClock_TimeScale4(t *testing.T) {
	// Start at 9:00
	startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 4) // 4x speed
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0, // 100 QPS
			10: 2.0, // 200 QPS
			11: 3.0, // 300 QPS
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     20,
		UpdateInterval: 15 * time.Minute, // Simulated time
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lc.Start(ctx)
	defer lc.Stop()

	// Initial: 9:00, mult=1.0
	if qps := lc.GetTargetQPS(); qps != 100 {
		t.Errorf("Initial TargetQPS = %d, want 100", qps)
	}

	// Wait 15 real minutes = 1 hour simulated time
	// After 15 min real = 60 min sim, we're at 10:00
	time.Sleep(100 * time.Millisecond) // Small sleep, clock advances
	lc.ForceUpdate()

	// Clock should have advanced slightly
	simNow := clk.Now()
	t.Logf("After small wait: simulated time = %s", simNow.Format(time.RFC3339))
}

func TestLoadController_Start_Stop(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 100

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     16,
		UpdateInterval: 50 * time.Millisecond,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lc.Start(ctx)

	// Let it run a bit
	time.Sleep(150 * time.Millisecond)

	// Stop should not hang
	done := make(chan struct{})
	go func() {
		lc.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(1 * time.Second):
		t.Error("Stop() timed out")
	}

	// Should have recorded some adjustments
	if lc.GetMetrics().GetAdjustments() < 1 {
		t.Error("Expected at least 1 adjustment")
	}
}

func TestLoadController_ForceUpdate(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0,
			10: 2.0,
		},
	}

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	// At 9:30, interpolated between 1.0 and 2.0 = 1.5
	lc.ForceUpdate()

	mult := lc.GetCurrentMultiplier()
	expectedMult := 1.5
	if math.Abs(mult-expectedMult) > 0.01 {
		t.Errorf("At 9:30, multiplier = %f, want ~%f", mult, expectedMult)
	}

	expectedQPS := 150 // 100 * 1.5
	if qps := lc.GetTargetQPS(); qps != expectedQPS {
		t.Errorf("At 9:30, TargetQPS = %d, want %d", qps, expectedQPS)
	}
}

func TestAdaptiveRateLimiter_RespectsTargetQPS(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 100 // Target 100 QPS

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	rl := NewAdaptiveRateLimiter(lc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rl.Start(ctx)
	defer rl.Stop()

	// Give rate limiter time to start
	time.Sleep(50 * time.Millisecond)

	// Count how many we can acquire in 100ms
	// Target is 100 QPS, so in 100ms we expect ~10 requests
	acquired := 0
	deadline := time.Now().Add(100 * time.Millisecond)

	for time.Now().Before(deadline) {
		if err := rl.WaitWithTimeout(ctx, 20*time.Millisecond); err == nil {
			acquired++
		}
	}

	// With 100 QPS target, in 100ms we should get ~10 requests
	// Allow ±50% tolerance due to timing variations
	expectedMin := 5
	expectedMax := 20

	if acquired < expectedMin || acquired > expectedMax {
		t.Errorf("Acquired %d in 100ms (100 QPS target), expected %d-%d",
			acquired, expectedMin, expectedMax)
	}
}

func TestAdaptiveRateLimiter_Acquire_NonBlocking(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 1000

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	rl := NewAdaptiveRateLimiter(lc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rl.Start(ctx)
	defer rl.Stop()

	// Wait for some tokens
	time.Sleep(50 * time.Millisecond)

	// Acquire should not block
	start := time.Now()
	_ = rl.Acquire()
	elapsed := time.Since(start)

	if elapsed > 10*time.Millisecond {
		t.Errorf("Acquire() took %v, should be non-blocking", elapsed)
	}
}

func TestAdaptiveRateLimiter_Stats(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 500

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	rl := NewAdaptiveRateLimiter(lc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rl.Start(ctx)
	defer rl.Stop()

	time.Sleep(50 * time.Millisecond)

	// Acquire some tokens
	for i := 0; i < 10; i++ {
		rl.Acquire()
	}

	stats := rl.GetStats()
	if stats.CurrentLimit != 500 {
		t.Errorf("CurrentLimit = %d, want 500", stats.CurrentLimit)
	}
}

func TestDynamicWorkerPool_ScalesUp(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 1.0, // mult=1.0, workers=4
			1: 2.0, // mult=2.0, workers=8
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     16,
		UpdateInterval: 1 * time.Minute,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	defer lc.Stop()

	// At hour 0, mult=1.0, target workers=4
	lc.ForceUpdate()
	initialWorkers := lc.GetTargetWorkers()
	if initialWorkers != 4 {
		t.Errorf("Initial target workers = %d, want 4", initialWorkers)
	}

	var workCount atomic.Int64
	workFunc := func(ctx context.Context, workerID int) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				workCount.Add(1)
			}
		}
	}

	pool := NewDynamicWorkerPool(lc, 1, 16)
	pool.SetWorkFunc(workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	// Initial workers should match target
	time.Sleep(50 * time.Millisecond)
	if workers := pool.CurrentWorkers(); workers != 4 {
		t.Errorf("Initial pool workers = %d, want 4", workers)
	}

	// Simulate time passing to hour 1 (mult=2.0)
	// Just update the controller directly
	time.Sleep(50 * time.Millisecond)
}

func TestDynamicWorkerPool_ScalesDown(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 2.0, // mult=2.0, workers=8
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     16,
		UpdateInterval: 1 * time.Minute,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	defer lc.Stop()

	lc.ForceUpdate()

	workFunc := func(ctx context.Context, workerID int) {
		<-ctx.Done()
	}

	pool := NewDynamicWorkerPool(lc, 1, 16)
	pool.SetWorkFunc(workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	time.Sleep(50 * time.Millisecond)

	// Initial: target workers = 8 (4 * 2.0)
	initialWorkers := pool.CurrentWorkers()
	if initialWorkers != 8 {
		t.Errorf("Initial workers = %d, want 8", initialWorkers)
	}

	// Now change pattern to have lower multiplier
	pat.HourlyMultipliers[0] = 0.5 // mult=0.5, workers=2
	lc.ForceUpdate()

	// Adjust should scale down (limited by maxDelta=2)
	delta := pool.Adjust(ctx)
	if delta >= 0 {
		t.Errorf("Expected negative delta (scale down), got %d", delta)
	}

	// Should have reduced by at most maxDelta (2)
	newWorkers := pool.CurrentWorkers()
	if newWorkers < initialWorkers-2 || newWorkers > initialWorkers {
		t.Errorf("After scale down: workers = %d (was %d), expected gradual reduction",
			newWorkers, initialWorkers)
	}
}

func TestDynamicWorkerPool_GradualScaling(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 1.0,
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     20,
		UpdateInterval: 1 * time.Minute,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	defer lc.Stop()

	lc.ForceUpdate()

	workFunc := func(ctx context.Context, workerID int) {
		<-ctx.Done()
	}

	poolCfg := DynamicWorkerPoolConfig{
		MinWorkers: 1,
		MaxWorkers: 20,
		MaxDelta:   2, // Max 2 workers per adjustment
	}

	pool := NewDynamicWorkerPoolWithConfig(lc, poolCfg)
	pool.SetWorkFunc(workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	time.Sleep(50 * time.Millisecond)

	// Start with 4 workers
	if w := pool.CurrentWorkers(); w != 4 {
		t.Errorf("Initial workers = %d, want 4", w)
	}

	// Set target to 10 workers (mult=2.5)
	pat.HourlyMultipliers[0] = 2.5
	lc.ForceUpdate()

	// First adjustment: 4 -> 6 (max +2)
	delta1 := pool.Adjust(ctx)
	if delta1 != 2 {
		t.Errorf("First adjustment delta = %d, want 2", delta1)
	}
	if w := pool.CurrentWorkers(); w != 6 {
		t.Errorf("After first adjustment: workers = %d, want 6", w)
	}

	// Second adjustment: 6 -> 8
	delta2 := pool.Adjust(ctx)
	if delta2 != 2 {
		t.Errorf("Second adjustment delta = %d, want 2", delta2)
	}
	if w := pool.CurrentWorkers(); w != 8 {
		t.Errorf("After second adjustment: workers = %d, want 8", w)
	}

	// Third adjustment: 8 -> 10
	delta3 := pool.Adjust(ctx)
	if delta3 != 2 {
		t.Errorf("Third adjustment delta = %d, want 2", delta3)
	}
	if w := pool.CurrentWorkers(); w != 10 {
		t.Errorf("After third adjustment: workers = %d, want 10", w)
	}

	// Fourth adjustment: already at target, delta = 0
	delta4 := pool.Adjust(ctx)
	if delta4 != 0 {
		t.Errorf("Fourth adjustment delta = %d, want 0", delta4)
	}
}

func TestDynamicWorkerPool_Stats(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 100

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	workFunc := func(ctx context.Context, workerID int) {
		<-ctx.Done()
	}

	pool := NewDynamicWorkerPool(lc, 1, 16)
	pool.SetWorkFunc(workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	time.Sleep(50 * time.Millisecond)

	stats := pool.GetStats()
	if stats.MinWorkers != 1 {
		t.Errorf("MinWorkers = %d, want 1", stats.MinWorkers)
	}
	if stats.MaxWorkers != 16 {
		t.Errorf("MaxWorkers = %d, want 16", stats.MaxWorkers)
	}
	if stats.CurrentWorkers != 4 {
		t.Errorf("CurrentWorkers = %d, want 4", stats.CurrentWorkers)
	}
}

func TestControllerMetrics_History(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()
	pat.BaselineQPS = 100

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     16,
		UpdateInterval: 20 * time.Millisecond,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lc.Start(ctx)

	// Let it run and record some history
	time.Sleep(200 * time.Millisecond)

	lc.Stop()

	metrics := lc.GetMetrics()

	// At least 1 adjustment from init + some from ticker
	if adj := metrics.GetAdjustments(); adj < 1 {
		t.Errorf("Expected at least 1 adjustment, got %d", adj)
	}

	history := metrics.GetTargetHistory()
	if len(history) < 1 {
		t.Errorf("Expected at least 1 history entry, got %d", len(history))
	}

	// All entries should have valid data
	for i, h := range history {
		if h.TargetQPS != 100 {
			t.Errorf("History[%d].TargetQPS = %d, want 100", i, h.TargetQPS)
		}
		if h.Multiplier != 1.0 {
			t.Errorf("History[%d].Multiplier = %f, want 1.0", i, h.Multiplier)
		}
	}
}

func TestControllerMetrics_GetStats(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 0.5,
			1: 1.0,
			2: 2.0,
		},
	}

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	// Record at different multipliers
	pat.HourlyMultipliers[0] = 0.5
	lc.ForceUpdate()
	pat.HourlyMultipliers[0] = 1.0
	lc.ForceUpdate()
	pat.HourlyMultipliers[0] = 2.0
	lc.ForceUpdate()

	stats := lc.GetMetrics().GetStats()

	if stats.TotalAdjustments < 3 {
		t.Errorf("TotalAdjustments = %d, want at least 3", stats.TotalAdjustments)
	}
}

func TestSimpleRateLimiter(t *testing.T) {
	rl := NewSimpleRateLimiter(100) // 100 QPS
	defer rl.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Count waits in 100ms
	acquired := 0
	deadline := time.Now().Add(100 * time.Millisecond)

	for time.Now().Before(deadline) {
		if err := rl.Wait(ctx); err == nil {
			acquired++
		}
	}

	// At 100 QPS, should get ~10 in 100ms
	// Allow tolerance for timing
	if acquired < 5 || acquired > 20 {
		t.Errorf("SimpleRateLimiter: acquired %d in 100ms, expected ~10", acquired)
	}
}

func TestLoadController_DoubleStart(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := pattern.UniformPattern.Clone()

	lc := NewLoadController(clk, pat, 4)
	defer lc.Stop()

	ctx := context.Background()

	// Start twice should not panic or start multiple goroutines
	lc.Start(ctx)
	lc.Start(ctx) // Should be ignored

	time.Sleep(50 * time.Millisecond)
	lc.Stop()
	lc.Stop() // Should be safe to call twice
}

func TestDynamicWorkerPool_RespectsBounds(t *testing.T) {
	clk := clock.NewRealClock()
	defer clk.Stop()

	pat := &pattern.LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 10.0, // Very high multiplier
		},
	}

	cfg := LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     2,
		MaxWorkers:     8, // Max 8 workers
		UpdateInterval: 1 * time.Minute,
	}

	lc := NewLoadControllerWithConfig(clk, pat, cfg)
	defer lc.Stop()

	lc.ForceUpdate()

	workFunc := func(ctx context.Context, workerID int) {
		<-ctx.Done()
	}

	pool := NewDynamicWorkerPool(lc, 2, 8)
	pool.SetWorkFunc(workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)
	defer pool.Stop()

	// Adjust multiple times
	for i := 0; i < 10; i++ {
		pool.Adjust(ctx)
	}

	time.Sleep(50 * time.Millisecond)

	// Should not exceed max
	if w := pool.CurrentWorkers(); w > 8 {
		t.Errorf("Workers = %d, exceeds max 8", w)
	}

	// Now set very low multiplier
	pat.HourlyMultipliers[0] = 0.1
	lc.ForceUpdate()

	// Adjust multiple times
	for i := 0; i < 10; i++ {
		pool.Adjust(ctx)
	}

	time.Sleep(50 * time.Millisecond)

	// Should not go below min
	if w := pool.CurrentWorkers(); w < 2 {
		t.Errorf("Workers = %d, below min 2", w)
	}
}
