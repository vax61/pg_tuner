package executor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
	"github.com/myorg/pg_tuner/pg_workload/internal/controller"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/events"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/storage"
	"github.com/myorg/pg_tuner/pg_workload/internal/timeline"
)

// SimulationConfig contains configuration for simulation mode.
type SimulationConfig struct {
	Profile           *profile.SimulationProfile
	Duration          time.Duration
	TimeScale         int
	StartTime         time.Time
	MaxStorage        int64
	RawRetention      time.Duration
	AggregateInterval time.Duration
	TimelineOutput    string
	Workers           int
	Seed              int64
	MaxAccounts       int
	MaxCustomers      int
	MaxBranches       int
	StoragePath       string
}

// SimulationStatus represents current simulation state.
type SimulationStatus struct {
	Phase           SimulationPhase
	SimulatedTime   time.Time
	RealTime        time.Time
	Multiplier      float64
	TargetQPS       int
	ActualQPS       float64
	ActiveWorkers   int
	ActiveEvents    []string
	StorageUsedPct  float64
	TotalQueries    int64
	TotalErrors     int64
	ElapsedReal     time.Duration
	ElapsedSimulated time.Duration
}

// SimulationPhase represents simulation execution phase.
type SimulationPhase string

const (
	SimPhaseIdle      SimulationPhase = "idle"
	SimPhaseStarting  SimulationPhase = "starting"
	SimPhaseRunning   SimulationPhase = "running"
	SimPhaseStopping  SimulationPhase = "stopping"
	SimPhaseDone      SimulationPhase = "done"
)

// SimulationExecutor orchestrates simulation mode execution.
type SimulationExecutor struct {
	pool              *database.Pool
	metrics           *metrics.Collector
	clock             clock.Clock
	controller        *controller.LoadController
	rateLimiter       *controller.AdaptiveRateLimiter
	workerPool        *controller.DynamicWorkerPool
	scheduler         *events.EventScheduler
	storage           *storage.StorageManager
	timeline          *timeline.StreamingTimeline
	intervalCollector *timeline.IntervalCollector
	config            *SimulationConfig

	phase         atomic.Value // SimulationPhase
	startRealTime time.Time
	startSimTime  time.Time

	totalQueries atomic.Int64
	totalErrors  atomic.Int64

	eventsTriggered []EventRecord
	eventsMu        sync.Mutex

	done chan struct{}
	wg   sync.WaitGroup
	mu   sync.RWMutex
}

// EventRecord tracks triggered events.
type EventRecord struct {
	Name      string    `json:"name"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Triggered bool      `json:"triggered"`
}

// NewSimulationExecutor creates a new simulation executor.
func NewSimulationExecutor(cfg *SimulationConfig, pool *database.Pool) (*SimulationExecutor, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if pool == nil {
		return nil, fmt.Errorf("pool is required")
	}
	if cfg.Profile == nil {
		return nil, fmt.Errorf("profile is required")
	}

	// Set defaults
	if cfg.TimeScale < 1 {
		cfg.TimeScale = 1
	}
	if cfg.TimeScale > 24 {
		cfg.TimeScale = 24
	}
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.AggregateInterval < time.Second {
		cfg.AggregateInterval = time.Minute
	}
	if cfg.MaxStorage <= 0 {
		cfg.MaxStorage = 500 << 20 // 500MB
	}
	if cfg.RawRetention <= 0 {
		cfg.RawRetention = 10 * time.Minute
	}
	if cfg.MaxAccounts < 1 {
		cfg.MaxAccounts = 10000
	}
	if cfg.StoragePath == "" {
		cfg.StoragePath = "/tmp/pg_workload_sim"
	}

	se := &SimulationExecutor{
		pool:            pool,
		metrics:         metrics.NewCollector(),
		config:          cfg,
		done:            make(chan struct{}),
		eventsTriggered: make([]EventRecord, 0),
	}
	se.phase.Store(SimPhaseIdle)

	return se, nil
}

// Initialize sets up all simulation components.
func (se *SimulationExecutor) Initialize(ctx context.Context) error {
	cfg := se.config

	// Create clock (simulated or real)
	startTime := cfg.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}
	se.startSimTime = startTime
	se.clock = clock.NewSimulatedClock(startTime, cfg.TimeScale)

	// Create load controller with pattern
	loadPattern := cfg.Profile.LoadPattern
	if loadPattern == nil {
		loadPattern = &pattern.LoadPattern{
			Type:        "constant",
			BaselineQPS: 100,
		}
	}

	lc := controller.NewLoadControllerWithConfig(se.clock, loadPattern, controller.LoadControllerConfig{
		BaseWorkers:    cfg.Workers,
		MinWorkers:     1,
		MaxWorkers:     cfg.Workers * 4,
		UpdateInterval: time.Minute, // Update every simulated minute
	})
	se.controller = lc

	// Create rate limiter
	se.rateLimiter = controller.NewAdaptiveRateLimiter(lc)

	// Create event scheduler
	se.scheduler = events.NewEventScheduler(se.clock)

	// Add events from profile (if any)
	// For now, we'll add events via AddEvent method

	// Create storage manager
	storageMgr, err := storage.NewStorageManager(
		cfg.StoragePath,
		cfg.MaxStorage,
		cfg.RawRetention,
		cfg.AggregateInterval,
	)
	if err != nil {
		return fmt.Errorf("creating storage manager: %w", err)
	}
	se.storage = storageMgr

	// Create timeline writer if output specified
	if cfg.TimelineOutput != "" {
		st, err := timeline.NewStreamingTimeline(cfg.TimelineOutput, cfg.AggregateInterval, 10)
		if err != nil {
			return fmt.Errorf("creating timeline writer: %w", err)
		}
		se.timeline = st
	}

	// Create interval collector
	se.intervalCollector = timeline.NewIntervalCollector(cfg.AggregateInterval, se.clock.Now())

	// Setup event listener
	se.scheduler.AddListener(se)

	return nil
}

// OnEventStart implements events.EventListener.
func (se *SimulationExecutor) OnEventStart(event *events.ActiveEvent) {
	se.eventsMu.Lock()
	defer se.eventsMu.Unlock()

	se.eventsTriggered = append(se.eventsTriggered, EventRecord{
		Name:      event.Event.Name,
		StartTime: event.StartTime,
		EndTime:   event.EndTime,
		Triggered: true,
	})
}

// OnEventEnd implements events.EventListener.
func (se *SimulationExecutor) OnEventEnd(event *events.ActiveEvent) {
	// Event end is tracked via the record's EndTime
}

// AddEvent adds a scheduled event to the simulation.
func (se *SimulationExecutor) AddEvent(event *events.ScheduledEvent) error {
	return se.scheduler.AddEvent(event)
}

// Run executes the simulation.
func (se *SimulationExecutor) Run(ctx context.Context) error {
	return se.runSimulation(ctx)
}

// Stop gracefully stops the simulation.
func (se *SimulationExecutor) Stop() error {
	se.phase.Store(SimPhaseStopping)

	// Close done channel to signal all goroutines
	select {
	case <-se.done:
		// Already closed
	default:
		close(se.done)
	}

	// Wait for all goroutines
	se.wg.Wait()

	// Cleanup components
	if se.rateLimiter != nil {
		se.rateLimiter.Stop()
	}
	if se.controller != nil {
		se.controller.Stop()
	}
	if se.scheduler != nil {
		se.scheduler.Stop()
	}
	if se.storage != nil {
		se.storage.Stop()
	}
	if se.clock != nil {
		se.clock.Stop()
	}

	// Flush and close timeline
	if se.timeline != nil {
		se.timeline.Close()
	}

	se.phase.Store(SimPhaseDone)
	return nil
}

// GetStatus returns current simulation status.
func (se *SimulationExecutor) GetStatus() *SimulationStatus {
	se.mu.RLock()
	defer se.mu.RUnlock()

	phase := se.phase.Load().(SimulationPhase)

	var simTime time.Time
	var multiplier float64
	var targetQPS int
	var activeWorkers int
	var activeEvents []string

	if se.clock != nil {
		simTime = se.clock.Now()
	}
	if se.controller != nil {
		multiplier = se.controller.GetCurrentMultiplier()
		targetQPS = se.controller.GetTargetQPS()
		activeWorkers = se.controller.GetTargetWorkers()
	}
	if se.scheduler != nil {
		for _, ae := range se.scheduler.GetActiveEvents() {
			activeEvents = append(activeEvents, ae.Event.Name)
		}
	}

	var storagePct float64
	if se.storage != nil {
		_, _, storagePct = se.storage.GetUsage()
	}

	var elapsedReal, elapsedSim time.Duration
	if !se.startRealTime.IsZero() {
		elapsedReal = time.Since(se.startRealTime)
	}
	if !se.startSimTime.IsZero() && se.clock != nil {
		elapsedSim = se.clock.Now().Sub(se.startSimTime)
	}

	totalQueries := se.totalQueries.Load()
	var actualQPS float64
	if elapsedReal > 0 {
		actualQPS = float64(totalQueries) / elapsedReal.Seconds()
	}

	return &SimulationStatus{
		Phase:            phase,
		SimulatedTime:    simTime,
		RealTime:         time.Now(),
		Multiplier:       multiplier,
		TargetQPS:        targetQPS,
		ActualQPS:        actualQPS,
		ActiveWorkers:    activeWorkers,
		ActiveEvents:     activeEvents,
		StorageUsedPct:   storagePct,
		TotalQueries:     totalQueries,
		TotalErrors:      se.totalErrors.Load(),
		ElapsedReal:      elapsedReal,
		ElapsedSimulated: elapsedSim,
	}
}

// GetMetrics returns the metrics collector.
func (se *SimulationExecutor) GetMetrics() *metrics.Collector {
	return se.metrics
}

// GetTimeline returns the timeline (if configured).
func (se *SimulationExecutor) GetTimeline() *timeline.Timeline {
	if se.timeline == nil {
		return nil
	}
	return se.timeline.GetTimeline()
}

// GetEventsTriggered returns the list of triggered events.
func (se *SimulationExecutor) GetEventsTriggered() []EventRecord {
	se.eventsMu.Lock()
	defer se.eventsMu.Unlock()

	result := make([]EventRecord, len(se.eventsTriggered))
	copy(result, se.eventsTriggered)
	return result
}

// GetStorageUsed returns bytes of storage used.
func (se *SimulationExecutor) GetStorageUsed() int64 {
	if se.storage == nil {
		return 0
	}
	current, _, _ := se.storage.GetUsage()
	return current
}

// recordQuery is called by workers to record query execution.
func (se *SimulationExecutor) recordQuery(latencyNs int64, isRead bool, isError bool) {
	se.totalQueries.Add(1)
	if isError {
		se.totalErrors.Add(1)
	}

	// Record to interval collector for timeline
	if se.intervalCollector != nil {
		se.intervalCollector.RecordQuery(latencyNs/1000, isRead, isError)
	}
}
