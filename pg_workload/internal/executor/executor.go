package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

// Phase represents the current execution phase.
type Phase string

const (
	PhaseIdle     Phase = "idle"
	PhaseWarmup   Phase = "warmup"
	PhaseRunning  Phase = "running"
	PhaseCooldown Phase = "cooldown"
	PhaseDone     Phase = "done"
)

// Config holds executor configuration.
type Config struct {
	Duration     time.Duration
	Warmup       time.Duration
	Cooldown     time.Duration
	Workers      int
	Seed         int64
	Queries      []profile.QueryTemplate
	MaxAccounts  int // For parameter generation
	MaxCustomers int // For JOIN query parameter generation
	MaxBranches  int // For JOIN query parameter generation
}

// Executor orchestrates workload execution.
type Executor struct {
	pool      *database.Pool
	collector *metrics.Collector
	config    Config

	mu      sync.RWMutex
	phase   Phase
	workers []*Worker
	stopCh  chan struct{}
}

// NewExecutor creates a new Executor.
func NewExecutor(pool *database.Pool, collector *metrics.Collector, cfg Config) *Executor {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.MaxAccounts < 1 {
		cfg.MaxAccounts = 10000
	}
	// Set default customer/branch counts based on scale ratios
	if cfg.MaxCustomers < 1 {
		cfg.MaxCustomers = cfg.MaxAccounts / 2
		if cfg.MaxCustomers < 1 {
			cfg.MaxCustomers = 1
		}
	}
	if cfg.MaxBranches < 1 {
		cfg.MaxBranches = cfg.MaxAccounts / 200
		if cfg.MaxBranches < 1 {
			cfg.MaxBranches = 1
		}
	}

	return &Executor{
		pool:      pool,
		collector: collector,
		config:    cfg,
		phase:     PhaseIdle,
		stopCh:    make(chan struct{}),
	}
}

// Run executes the workload with warmup, main execution, and cooldown phases.
func (e *Executor) Run(ctx context.Context) error {
	// Create cancellable context for workers
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Start workers
	var wg sync.WaitGroup
	e.workers = make([]*Worker, e.config.Workers)

	for i := 0; i < e.config.Workers; i++ {
		workerSeed := e.config.Seed + int64(i)
		workerCfg := WorkerConfig{
			MaxAccounts:  e.config.MaxAccounts,
			MaxCustomers: e.config.MaxCustomers,
			MaxBranches:  e.config.MaxBranches,
		}
		w := NewWorkerWithConfig(i, e.pool, e.collector, e.config.Queries, workerSeed, workerCfg)
		e.workers[i] = w

		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			worker.Run(workerCtx)
		}(w)
	}

	// Phase: Warmup
	if e.config.Warmup > 0 {
		e.setPhase(PhaseWarmup)
		if err := e.waitPhase(ctx, e.config.Warmup); err != nil {
			cancelWorkers()
			wg.Wait()
			return err
		}
		// Reset metrics after warmup
		e.collector.Reset()
	}

	// Phase: Main execution
	e.setPhase(PhaseRunning)
	if err := e.waitPhase(ctx, e.config.Duration); err != nil {
		cancelWorkers()
		wg.Wait()
		return err
	}

	// Phase: Cooldown
	if e.config.Cooldown > 0 {
		e.setPhase(PhaseCooldown)
		if err := e.waitPhase(ctx, e.config.Cooldown); err != nil {
			cancelWorkers()
			wg.Wait()
			return err
		}
	}

	// Stop workers gracefully
	e.setPhase(PhaseDone)
	cancelWorkers()
	wg.Wait()

	return nil
}

// waitPhase waits for a duration or context cancellation.
func (e *Executor) waitPhase(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.stopCh:
		return fmt.Errorf("executor stopped")
	case <-timer.C:
		return nil
	}
}

// Stop signals the executor to stop.
func (e *Executor) Stop() {
	close(e.stopCh)
}

// Phase returns the current execution phase.
func (e *Executor) Phase() Phase {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.phase
}

func (e *Executor) setPhase(p Phase) {
	e.mu.Lock()
	e.phase = p
	e.mu.Unlock()
}

// Stats returns current worker statistics.
func (e *Executor) Stats() ExecutorStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := ExecutorStats{
		Phase:       e.phase,
		WorkerCount: len(e.workers),
	}

	for _, w := range e.workers {
		ws := w.Stats()
		stats.TotalQueries += ws.Queries
		stats.TotalErrors += ws.Errors
	}

	return stats
}

// ExecutorStats holds executor statistics.
type ExecutorStats struct {
	Phase        Phase
	WorkerCount  int
	TotalQueries int64
	TotalErrors  int64
}
