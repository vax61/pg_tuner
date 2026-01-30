package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
)

// LoadController manages dynamic load adjustment based on time patterns.
type LoadController struct {
	clock   clock.Clock
	pattern *pattern.LoadPattern

	baseWorkers int
	minWorkers  int
	maxWorkers  int

	currentQPS atomic.Int64
	targetQPS  atomic.Int64

	currentMultiplier atomic.Value // float64

	metrics *ControllerMetrics

	updateInterval time.Duration
	done           chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	started        bool
}

// LoadControllerConfig contains configuration for LoadController.
type LoadControllerConfig struct {
	BaseWorkers    int
	MinWorkers     int
	MaxWorkers     int
	UpdateInterval time.Duration
}

// DefaultLoadControllerConfig returns default configuration.
func DefaultLoadControllerConfig() LoadControllerConfig {
	return LoadControllerConfig{
		BaseWorkers:    4,
		MinWorkers:     1,
		MaxWorkers:     32,
		UpdateInterval: 1 * time.Minute,
	}
}

// NewLoadController creates a new LoadController.
func NewLoadController(clk clock.Clock, pat *pattern.LoadPattern, baseWorkers int) *LoadController {
	cfg := DefaultLoadControllerConfig()
	cfg.BaseWorkers = baseWorkers
	return NewLoadControllerWithConfig(clk, pat, cfg)
}

// NewLoadControllerWithConfig creates a new LoadController with full configuration.
func NewLoadControllerWithConfig(clk clock.Clock, pat *pattern.LoadPattern, cfg LoadControllerConfig) *LoadController {
	if cfg.MinWorkers < 1 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers < cfg.MinWorkers {
		cfg.MaxWorkers = cfg.MinWorkers
	}
	if cfg.BaseWorkers < cfg.MinWorkers {
		cfg.BaseWorkers = cfg.MinWorkers
	}
	if cfg.BaseWorkers > cfg.MaxWorkers {
		cfg.BaseWorkers = cfg.MaxWorkers
	}
	if cfg.UpdateInterval < 1*time.Second {
		cfg.UpdateInterval = 1 * time.Second
	}

	lc := &LoadController{
		clock:          clk,
		pattern:        pat,
		baseWorkers:    cfg.BaseWorkers,
		minWorkers:     cfg.MinWorkers,
		maxWorkers:     cfg.MaxWorkers,
		updateInterval: cfg.UpdateInterval,
		done:           make(chan struct{}),
		metrics:        newControllerMetrics(),
	}

	// Initialize current values
	lc.currentMultiplier.Store(1.0)
	lc.updateTarget()

	return lc
}

// Start begins the background goroutine that updates target values.
func (lc *LoadController) Start(ctx context.Context) {
	lc.mu.Lock()
	if lc.started {
		lc.mu.Unlock()
		return
	}
	lc.started = true
	lc.mu.Unlock()

	lc.wg.Add(1)
	go lc.runUpdateLoop(ctx)
}

// Stop stops the controller and waits for cleanup.
func (lc *LoadController) Stop() {
	lc.mu.Lock()
	if !lc.started {
		lc.mu.Unlock()
		return
	}
	lc.mu.Unlock()

	select {
	case <-lc.done:
		// Already stopped
	default:
		close(lc.done)
	}
	lc.wg.Wait()
}

// runUpdateLoop periodically updates target values based on the clock.
func (lc *LoadController) runUpdateLoop(ctx context.Context) {
	defer lc.wg.Done()

	ticker := lc.clock.Ticker(lc.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-lc.done:
			return
		case <-ticker.C:
			lc.updateTarget()
		}
	}
}

// updateTarget recalculates target QPS and records metrics.
func (lc *LoadController) updateTarget() {
	now := lc.clock.Now()
	mult := lc.pattern.GetMultiplierSmooth(now)
	targetQPS := lc.pattern.GetTargetQPSSmooth(now)

	lc.currentMultiplier.Store(mult)
	lc.targetQPS.Store(int64(targetQPS))

	// Record in metrics
	lc.metrics.recordTarget(now, mult, targetQPS, lc.GetTargetWorkers())
}

// GetTargetQPS returns the current target QPS.
func (lc *LoadController) GetTargetQPS() int {
	return int(lc.targetQPS.Load())
}

// GetCurrentQPS returns the actual measured QPS.
func (lc *LoadController) GetCurrentQPS() int {
	return int(lc.currentQPS.Load())
}

// SetCurrentQPS updates the measured QPS (called by rate limiter or executor).
func (lc *LoadController) SetCurrentQPS(qps int) {
	lc.currentQPS.Store(int64(qps))
}

// GetTargetWorkers returns the recommended number of workers.
// Workers scale proportionally with the multiplier.
func (lc *LoadController) GetTargetWorkers() int {
	mult := lc.GetCurrentMultiplier()

	// Scale workers: baseWorkers * multiplier
	workers := int(float64(lc.baseWorkers) * mult)

	// Clamp to min/max bounds
	if workers < lc.minWorkers {
		workers = lc.minWorkers
	}
	if workers > lc.maxWorkers {
		workers = lc.maxWorkers
	}

	return workers
}

// GetCurrentMultiplier returns the current load multiplier.
func (lc *LoadController) GetCurrentMultiplier() float64 {
	v := lc.currentMultiplier.Load()
	if v == nil {
		return 1.0
	}
	return v.(float64)
}

// GetMinWorkers returns the minimum workers.
func (lc *LoadController) GetMinWorkers() int {
	return lc.minWorkers
}

// GetMaxWorkers returns the maximum workers.
func (lc *LoadController) GetMaxWorkers() int {
	return lc.maxWorkers
}

// GetBaseWorkers returns the base workers.
func (lc *LoadController) GetBaseWorkers() int {
	return lc.baseWorkers
}

// GetPattern returns the load pattern.
func (lc *LoadController) GetPattern() *pattern.LoadPattern {
	return lc.pattern
}

// GetClock returns the clock.
func (lc *LoadController) GetClock() clock.Clock {
	return lc.clock
}

// GetMetrics returns controller metrics.
func (lc *LoadController) GetMetrics() *ControllerMetrics {
	return lc.metrics
}

// ForceUpdate forces an immediate target update.
func (lc *LoadController) ForceUpdate() {
	lc.updateTarget()
}
