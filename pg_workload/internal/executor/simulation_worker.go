package executor

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/controller"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

// SimulationWorker executes queries with rate limiting and adaptive behavior.
type SimulationWorker struct {
	id            int
	pool          *database.Pool
	controller    *controller.LoadController
	metrics       *metrics.Collector
	rateLimiter   *controller.AdaptiveRateLimiter
	selector      *QuerySelector
	rng           *rand.Rand
	maxAccounts   int
	maxCustomers  int
	maxBranches   int
	readRatio     int
	writeRatio    int

	// Callback for recording to simulation executor
	recordCallback func(latencyNs int64, isRead bool, isError bool)

	queries atomic.Int64
	errors  atomic.Int64
}

// SimulationWorkerConfig contains worker configuration.
type SimulationWorkerConfig struct {
	ID             int
	Pool           *database.Pool
	Controller     *controller.LoadController
	Metrics        *metrics.Collector
	RateLimiter    *controller.AdaptiveRateLimiter
	Queries        []profile.QueryTemplate
	Seed           int64
	MaxAccounts    int
	MaxCustomers   int
	MaxBranches    int
	ReadRatio      int
	WriteRatio     int
	RecordCallback func(latencyNs int64, isRead bool, isError bool)
}

// NewSimulationWorker creates a new simulation worker.
func NewSimulationWorker(cfg SimulationWorkerConfig) *SimulationWorker {
	if cfg.ReadRatio <= 0 && cfg.WriteRatio <= 0 {
		cfg.ReadRatio = 70
		cfg.WriteRatio = 30
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

	return &SimulationWorker{
		id:             cfg.ID,
		pool:           cfg.Pool,
		controller:     cfg.Controller,
		metrics:        cfg.Metrics,
		rateLimiter:    cfg.RateLimiter,
		selector:       NewQuerySelector(cfg.Queries, cfg.Seed),
		rng:            rand.New(rand.NewSource(cfg.Seed)),
		maxAccounts:    cfg.MaxAccounts,
		maxCustomers:   cfg.MaxCustomers,
		maxBranches:    cfg.MaxBranches,
		readRatio:      cfg.ReadRatio,
		writeRatio:     cfg.WriteRatio,
		recordCallback: cfg.RecordCallback,
	}
}

// Run executes queries until context is cancelled.
func (w *SimulationWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Wait for rate limiter token
			if err := w.rateLimiter.Wait(ctx); err != nil {
				// Context cancelled or rate limiter stopped
				return
			}

			// Execute query
			w.executeQuery(ctx)
		}
	}
}

// executeQuery selects and executes a single query with rate limiting.
func (w *SimulationWorker) executeQuery(ctx context.Context) {
	// Select query based on read/write ratio
	query := w.selectQuery()
	if query == nil {
		return
	}

	isRead := query.Type == profile.QueryTypeRead

	// Generate parameters
	args := w.generateParams(query)

	// Acquire connection
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		w.recordError(query.Name, "acquire_failed", isRead)
		return
	}
	defer conn.Release()

	// Execute query and measure latency
	start := time.Now()
	_, err = conn.Exec(ctx, query.SQL, args...)
	latency := time.Since(start)

	w.queries.Add(1)

	isError := false
	if err != nil {
		isError = true
		w.recordError(query.Name, classifyError(err), isRead)
	} else {
		w.metrics.RecordLatency(query.Name, latency.Nanoseconds())
	}

	// Callback to simulation executor
	if w.recordCallback != nil {
		w.recordCallback(latency.Nanoseconds(), isRead, isError)
	}
}

// selectQuery selects a query based on current read/write ratio.
func (w *SimulationWorker) selectQuery() *profile.QueryTemplate {
	// Determine if this should be a read or write based on ratio
	totalRatio := w.readRatio + w.writeRatio
	if totalRatio <= 0 {
		totalRatio = 100
	}

	randVal := w.rng.Intn(totalRatio)
	preferRead := randVal < w.readRatio

	// Get base query from selector
	query := w.selector.Next()
	if query == nil {
		return nil
	}

	// If query type matches preference, use it
	isRead := query.Type == profile.QueryTypeRead
	if isRead == preferRead {
		return query
	}

	// Otherwise, try to find a query of the preferred type
	// Use a few attempts before falling back
	for i := 0; i < 3; i++ {
		q := w.selector.Next()
		if q != nil && (q.Type == profile.QueryTypeRead) == preferRead {
			return q
		}
	}

	// Fall back to original query
	return query
}

// recordError records an error.
func (w *SimulationWorker) recordError(opName, errType string, isRead bool) {
	w.metrics.IncrementError(opName, errType)
	w.errors.Add(1)

	if w.recordCallback != nil {
		w.recordCallback(0, isRead, true)
	}
}

// generateParams generates query parameters based on query name.
func (w *SimulationWorker) generateParams(query *profile.QueryTemplate) []any {
	switch query.Name {
	// === Point Lookups ===
	case "point_select":
		return []any{w.rng.Intn(w.maxAccounts) + 1}

	// === Range Selects ===
	case "range_select":
		minBalance := float64(w.rng.Intn(50000))
		maxBalance := minBalance + float64(w.rng.Intn(50000))
		return []any{minBalance, maxBalance}

	// === Writes ===
	case "insert_tx":
		accountID := w.rng.Intn(w.maxAccounts) + 1
		txTypes := []string{"deposit", "withdrawal", "transfer", "fee"}
		txType := txTypes[w.rng.Intn(len(txTypes))]
		amount := float64(w.rng.Intn(5000)) + w.rng.Float64()
		if txType == "withdrawal" || txType == "fee" {
			amount = -amount
		}
		desc := fmt.Sprintf("SIM-%d-%d", w.id, time.Now().UnixNano())
		return []any{accountID, txType, amount, desc}

	case "update_balance":
		amount := float64(w.rng.Intn(2000)) - 1000
		accountID := w.rng.Intn(w.maxAccounts) + 1
		return []any{amount, accountID}

	// === 2-way JOINs ===
	case "customer_accounts":
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "account_transactions":
		return []any{w.rng.Intn(w.maxAccounts) + 1}

	case "branch_accounts":
		return []any{w.rng.Intn(w.maxBranches) + 1}

	// === 3-way+ JOINs ===
	case "customer_tx_summary":
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "branch_tx_summary":
		return []any{w.rng.Intn(w.maxBranches) + 1}

	case "customer_audit_trail":
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "full_customer_report":
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "complex_join":
		statuses := []string{"active", "inactive", "suspended", "pending"}
		return []any{statuses[w.rng.Intn(len(statuses))]}

	default:
		return nil
	}
}

// SetReadWriteRatio updates the read/write ratio.
func (w *SimulationWorker) SetReadWriteRatio(read, write int) {
	w.readRatio = read
	w.writeRatio = write
}

// Stats returns worker statistics.
func (w *SimulationWorker) Stats() WorkerStats {
	return WorkerStats{
		ID:      w.id,
		Queries: w.queries.Load(),
		Errors:  w.errors.Load(),
	}
}

// SimulationWorkerPool manages a pool of simulation workers.
type SimulationWorkerPool struct {
	workers     []*SimulationWorker
	workerFunc  controller.WorkerFunc
	dynamicPool *controller.DynamicWorkerPool
}

// SimulationWorkerFuncConfig contains configuration for creating worker functions.
type SimulationWorkerFuncConfig struct {
	Pool           *database.Pool
	Controller     *controller.LoadController
	Metrics        *metrics.Collector
	RateLimiter    *controller.AdaptiveRateLimiter
	Queries        []profile.QueryTemplate
	BaseSeed       int64
	MaxAccounts    int
	MaxCustomers   int
	MaxBranches    int
	ReadRatio      int
	WriteRatio     int
	RecordCallback func(latencyNs int64, isRead bool, isError bool)
}

// CreateSimulationWorkerFunc creates a worker function for the dynamic pool.
func CreateSimulationWorkerFunc(
	pool *database.Pool,
	lc *controller.LoadController,
	mc *metrics.Collector,
	rl *controller.AdaptiveRateLimiter,
	queries []profile.QueryTemplate,
	baseSeed int64,
	maxAccounts int,
	readRatio int,
	writeRatio int,
	recordCallback func(latencyNs int64, isRead bool, isError bool),
) controller.WorkerFunc {
	// Use default customer/branch counts based on scale ratios
	maxCustomers := maxAccounts / 2
	if maxCustomers < 1 {
		maxCustomers = 1
	}
	maxBranches := maxAccounts / 200
	if maxBranches < 1 {
		maxBranches = 1
	}

	return CreateSimulationWorkerFuncWithConfig(SimulationWorkerFuncConfig{
		Pool:           pool,
		Controller:     lc,
		Metrics:        mc,
		RateLimiter:    rl,
		Queries:        queries,
		BaseSeed:       baseSeed,
		MaxAccounts:    maxAccounts,
		MaxCustomers:   maxCustomers,
		MaxBranches:    maxBranches,
		ReadRatio:      readRatio,
		WriteRatio:     writeRatio,
		RecordCallback: recordCallback,
	})
}

// CreateSimulationWorkerFuncWithConfig creates a worker function with explicit configuration.
func CreateSimulationWorkerFuncWithConfig(cfg SimulationWorkerFuncConfig) controller.WorkerFunc {
	return func(ctx context.Context, workerID int) {
		workerCfg := SimulationWorkerConfig{
			ID:             workerID,
			Pool:           cfg.Pool,
			Controller:     cfg.Controller,
			Metrics:        cfg.Metrics,
			RateLimiter:    cfg.RateLimiter,
			Queries:        cfg.Queries,
			Seed:           cfg.BaseSeed + int64(workerID),
			MaxAccounts:    cfg.MaxAccounts,
			MaxCustomers:   cfg.MaxCustomers,
			MaxBranches:    cfg.MaxBranches,
			ReadRatio:      cfg.ReadRatio,
			WriteRatio:     cfg.WriteRatio,
			RecordCallback: cfg.RecordCallback,
		}

		worker := NewSimulationWorker(workerCfg)
		worker.Run(ctx)
	}
}
