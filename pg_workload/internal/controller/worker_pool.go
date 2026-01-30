package controller

import (
	"context"
	"sync"
	"sync/atomic"
)

// WorkerFunc is the function signature for worker tasks.
type WorkerFunc func(ctx context.Context, workerID int)

// Worker represents a single worker in the pool.
type Worker struct {
	ID       int
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	running  atomic.Bool
	workFunc WorkerFunc
}

// newWorker creates a new worker.
func newWorker(id int, workFunc WorkerFunc) *Worker {
	return &Worker{
		ID:       id,
		done:     make(chan struct{}),
		workFunc: workFunc,
	}
}

// Start starts the worker.
func (w *Worker) Start(parentCtx context.Context) {
	if w.running.Load() {
		return
	}

	w.ctx, w.cancel = context.WithCancel(parentCtx)
	w.running.Store(true)

	go func() {
		defer func() {
			w.running.Store(false)
			close(w.done)
		}()
		w.workFunc(w.ctx, w.ID)
	}()
}

// Stop stops the worker.
func (w *Worker) Stop() {
	if !w.running.Load() {
		return
	}
	if w.cancel != nil {
		w.cancel()
	}
}

// Wait waits for the worker to finish.
func (w *Worker) Wait() {
	<-w.done
}

// IsRunning returns whether the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// DynamicWorkerPool manages a pool of workers that can scale dynamically.
type DynamicWorkerPool struct {
	controller *LoadController
	workFunc   WorkerFunc

	workers    []*Worker
	minWorkers int
	maxWorkers int

	// Maximum workers to add/remove per adjustment cycle
	maxDelta int

	nextWorkerID atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc

	adjustments atomic.Int64
	scaleUps    atomic.Int64
	scaleDowns  atomic.Int64

	mu      sync.RWMutex
	started bool
	stopped bool
}

// DynamicWorkerPoolConfig contains configuration for DynamicWorkerPool.
type DynamicWorkerPoolConfig struct {
	MinWorkers int
	MaxWorkers int
	MaxDelta   int // Maximum workers to add/remove per cycle
}

// DefaultDynamicWorkerPoolConfig returns default configuration.
func DefaultDynamicWorkerPoolConfig() DynamicWorkerPoolConfig {
	return DynamicWorkerPoolConfig{
		MinWorkers: 1,
		MaxWorkers: 32,
		MaxDelta:   2,
	}
}

// NewDynamicWorkerPool creates a new dynamic worker pool.
func NewDynamicWorkerPool(lc *LoadController, min, max int) *DynamicWorkerPool {
	cfg := DynamicWorkerPoolConfig{
		MinWorkers: min,
		MaxWorkers: max,
		MaxDelta:   2,
	}
	return NewDynamicWorkerPoolWithConfig(lc, cfg)
}

// NewDynamicWorkerPoolWithConfig creates a new dynamic worker pool with full config.
func NewDynamicWorkerPoolWithConfig(lc *LoadController, cfg DynamicWorkerPoolConfig) *DynamicWorkerPool {
	if cfg.MinWorkers < 1 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers < cfg.MinWorkers {
		cfg.MaxWorkers = cfg.MinWorkers
	}
	if cfg.MaxDelta < 1 {
		cfg.MaxDelta = 1
	}

	return &DynamicWorkerPool{
		controller: lc,
		workers:    make([]*Worker, 0),
		minWorkers: cfg.MinWorkers,
		maxWorkers: cfg.MaxWorkers,
		maxDelta:   cfg.MaxDelta,
	}
}

// SetWorkFunc sets the work function for workers.
func (p *DynamicWorkerPool) SetWorkFunc(fn WorkerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workFunc = fn
}

// Start starts the worker pool with the initial number of workers.
func (p *DynamicWorkerPool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started || p.stopped {
		return
	}

	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true

	// Start initial workers based on controller target
	initialWorkers := p.controller.GetTargetWorkers()
	if initialWorkers < p.minWorkers {
		initialWorkers = p.minWorkers
	}
	if initialWorkers > p.maxWorkers {
		initialWorkers = p.maxWorkers
	}

	for i := 0; i < initialWorkers; i++ {
		p.addWorkerLocked()
	}
}

// Stop stops all workers and the pool.
func (p *DynamicWorkerPool) Stop() {
	p.mu.Lock()
	if p.stopped || !p.started {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()

	// Cancel context to stop all workers
	if p.cancel != nil {
		p.cancel()
	}

	// Wait for all workers to finish
	p.mu.RLock()
	workers := make([]*Worker, len(p.workers))
	copy(workers, p.workers)
	p.mu.RUnlock()

	for _, w := range workers {
		w.Wait()
	}
}

// Adjust scales workers up or down based on controller target.
// Returns the change in worker count.
func (p *DynamicWorkerPool) Adjust(ctx context.Context) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return 0
	}

	p.adjustments.Add(1)

	current := len(p.workers)
	target := p.controller.GetTargetWorkers()

	// Clamp target to pool bounds
	if target < p.minWorkers {
		target = p.minWorkers
	}
	if target > p.maxWorkers {
		target = p.maxWorkers
	}

	delta := target - current

	// Limit the change to avoid oscillations
	if delta > p.maxDelta {
		delta = p.maxDelta
	}
	if delta < -p.maxDelta {
		delta = -p.maxDelta
	}

	if delta > 0 {
		// Scale up
		p.scaleUps.Add(1)
		for i := 0; i < delta; i++ {
			p.addWorkerLocked()
		}
	} else if delta < 0 {
		// Scale down
		p.scaleDowns.Add(1)
		for i := 0; i < -delta; i++ {
			p.removeWorkerLocked()
		}
	}

	return delta
}

// addWorkerLocked adds a new worker (must hold lock).
func (p *DynamicWorkerPool) addWorkerLocked() {
	if len(p.workers) >= p.maxWorkers {
		return
	}

	id := int(p.nextWorkerID.Add(1))
	worker := newWorker(id, p.workFunc)

	if p.ctx != nil {
		worker.Start(p.ctx)
	}

	p.workers = append(p.workers, worker)
}

// removeWorkerLocked removes a worker (must hold lock).
func (p *DynamicWorkerPool) removeWorkerLocked() {
	if len(p.workers) <= p.minWorkers {
		return
	}

	// Remove the last worker
	idx := len(p.workers) - 1
	worker := p.workers[idx]
	p.workers = p.workers[:idx]

	// Stop the worker in background to not block
	go func() {
		worker.Stop()
		worker.Wait()
	}()
}

// CurrentWorkers returns the current number of workers.
func (p *DynamicWorkerPool) CurrentWorkers() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.workers)
}

// RunningWorkers returns the number of actually running workers.
func (p *DynamicWorkerPool) RunningWorkers() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := 0
	for _, w := range p.workers {
		if w.IsRunning() {
			count++
		}
	}
	return count
}

// GetStats returns pool statistics.
func (p *DynamicWorkerPool) GetStats() WorkerPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return WorkerPoolStats{
		CurrentWorkers: len(p.workers),
		MinWorkers:     p.minWorkers,
		MaxWorkers:     p.maxWorkers,
		TargetWorkers:  p.controller.GetTargetWorkers(),
		Adjustments:    p.adjustments.Load(),
		ScaleUps:       p.scaleUps.Load(),
		ScaleDowns:     p.scaleDowns.Load(),
	}
}

// WorkerPoolStats contains worker pool statistics.
type WorkerPoolStats struct {
	CurrentWorkers int
	MinWorkers     int
	MaxWorkers     int
	TargetWorkers  int
	Adjustments    int64
	ScaleUps       int64
	ScaleDowns     int64
}
