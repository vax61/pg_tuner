package executor

import (
	"context"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/controller"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/timeline"
)

// runSimulation executes the main simulation loop.
func (se *SimulationExecutor) runSimulation(ctx context.Context) error {
	se.phase.Store(SimPhaseStarting)
	se.startRealTime = time.Now()

	cfg := se.config

	// Start controller (updates target QPS based on time pattern)
	se.controller.Start(ctx)

	// Start rate limiter
	se.rateLimiter.Start(ctx)

	// Start event scheduler
	se.scheduler.Start(ctx)

	// Start storage manager
	if err := se.storage.Start(ctx); err != nil {
		return err
	}

	// Set up interval collector with current config
	multiplier := se.controller.GetCurrentMultiplier()
	targetQPS := se.controller.GetTargetQPS()
	se.intervalCollector.SetConfig(multiplier, targetQPS, cfg.Workers)

	// Create worker function with explicit entity counts
	readRatio := cfg.Profile.WorkloadDistribution.Read
	writeRatio := cfg.Profile.WorkloadDistribution.Write

	// Calculate default customer/branch counts if not specified
	maxCustomers := cfg.MaxCustomers
	if maxCustomers < 1 {
		maxCustomers = cfg.MaxAccounts / 2
		if maxCustomers < 1 {
			maxCustomers = 1
		}
	}
	maxBranches := cfg.MaxBranches
	if maxBranches < 1 {
		maxBranches = cfg.MaxAccounts / 200
		if maxBranches < 1 {
			maxBranches = 1
		}
	}

	workerFunc := CreateSimulationWorkerFuncWithConfig(SimulationWorkerFuncConfig{
		Pool:           se.pool,
		Controller:     se.controller,
		Metrics:        se.metrics,
		RateLimiter:    se.rateLimiter,
		Queries:        profile.OLTPQueries,
		BaseSeed:       cfg.Seed,
		MaxAccounts:    cfg.MaxAccounts,
		MaxCustomers:   maxCustomers,
		MaxBranches:    maxBranches,
		ReadRatio:      readRatio,
		WriteRatio:     writeRatio,
		RecordCallback: se.recordQuery,
	})

	// Create dynamic worker pool
	se.workerPool = controller.NewDynamicWorkerPool(se.controller, 1, cfg.Workers*4)
	se.workerPool.SetWorkFunc(workerFunc)

	// Start worker pool
	se.workerPool.Start(ctx)

	se.phase.Store(SimPhaseRunning)

	// Start interval snapshot goroutine
	se.wg.Add(1)
	go se.intervalLoop(ctx)

	// Start storage monitor goroutine
	se.wg.Add(1)
	go se.storageMonitorLoop(ctx)

	// Main loop: wait for duration or context cancellation
	err := se.mainLoop(ctx)

	// Graceful shutdown
	se.phase.Store(SimPhaseStopping)

	// Stop worker pool first
	se.workerPool.Stop()

	// Signal done to other goroutines
	select {
	case <-se.done:
	default:
		close(se.done)
	}

	// Wait for goroutines
	se.wg.Wait()

	// Final interval snapshot
	se.takeIntervalSnapshot()

	// Stop other components (order matters)
	se.rateLimiter.Stop()
	se.controller.Stop()
	se.scheduler.Stop()
	se.storage.Stop()
	se.clock.Stop()

	// Close timeline
	if se.timeline != nil {
		se.timeline.Flush()
	}

	se.phase.Store(SimPhaseDone)
	return err
}

// mainLoop waits for duration or context cancellation.
func (se *SimulationExecutor) mainLoop(ctx context.Context) error {
	cfg := se.config

	// Duration is REAL time, not simulated time
	// With --duration 60m and --time-scale 12, we run for 60 real minutes
	// which produces 720 simulated minutes (12 hours)
	endRealTime := se.startRealTime.Add(cfg.Duration)

	// Check every 10 seconds real time
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-se.done:
			return nil
		case <-ticker.C:
			// Check if real duration has elapsed
			if time.Now().After(endRealTime) || time.Now().Equal(endRealTime) {
				return nil
			}

			// Check storage limit
			if se.storage.IsAtLimit() {
				return nil
			}
		}
	}
}

// intervalLoop periodically takes snapshots for the timeline.
func (se *SimulationExecutor) intervalLoop(ctx context.Context) {
	defer se.wg.Done()

	// Use clock's ticker for proper simulation timing
	ticker := se.clock.Ticker(se.config.AggregateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-se.done:
			return
		case <-ticker.C:
			se.takeIntervalSnapshot()
		}
	}
}

// takeIntervalSnapshot captures current metrics for the timeline.
func (se *SimulationExecutor) takeIntervalSnapshot() {
	if se.intervalCollector == nil {
		return
	}

	// Get current status
	multiplier := se.controller.GetCurrentMultiplier()
	targetQPS := se.controller.GetTargetQPS()
	activeWorkers := se.workerPool.CurrentWorkers()

	// Check for event effects
	effects := se.scheduler.GetCurrentEffects()
	if effects.MultiplierOverride != nil {
		multiplier = *effects.MultiplierOverride
	}

	// Update collector config
	se.intervalCollector.SetConfig(multiplier, targetQPS, activeWorkers)

	// Create timeline entry
	entry := se.intervalCollector.ToEntry()

	// Write to timeline if configured
	if se.timeline != nil {
		se.timeline.Record(entry)
	}

	// Reset collector for next interval
	se.intervalCollector.Reset(se.clock.Now())
}

// storageMonitorLoop monitors storage usage.
func (se *SimulationExecutor) storageMonitorLoop(ctx context.Context) {
	defer se.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-se.done:
			return
		case <-ticker.C:
			// Storage is auto-monitored by StorageManager
			// This is for additional actions if needed
			if se.storage.IsAtLimit() {
				// Signal to stop if storage limit reached
				select {
				case <-se.done:
				default:
					close(se.done)
				}
				return
			}
		}
	}
}

// GetTimelineSummary returns the timeline summary.
func (se *SimulationExecutor) GetTimelineSummary() *timeline.TimelineSummary {
	if se.timeline == nil {
		return nil
	}
	return se.timeline.GetSummary()
}
