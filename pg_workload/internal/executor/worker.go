package executor

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

// Worker executes queries in a loop.
type Worker struct {
	id          int
	pool        *database.Pool
	collector   *metrics.Collector
	selector    *QuerySelector
	rng         *rand.Rand
	maxAccounts int
	maxCustomers int
	maxBranches  int

	queries atomic.Int64
	errors  atomic.Int64
}

// WorkerStats holds worker statistics.
type WorkerStats struct {
	ID      int
	Queries int64
	Errors  int64
}

// WorkerConfig holds configuration for creating a worker.
type WorkerConfig struct {
	MaxAccounts  int
	MaxCustomers int
	MaxBranches  int
}

// NewWorker creates a new Worker.
func NewWorker(id int, pool *database.Pool, collector *metrics.Collector, queries []profile.QueryTemplate, seed int64, maxAccounts int) *Worker {
	// Default customer/branch counts based on scale ratios
	maxCustomers := maxAccounts / 2
	if maxCustomers < 1 {
		maxCustomers = 1
	}
	maxBranches := maxAccounts / 200
	if maxBranches < 1 {
		maxBranches = 1
	}

	return &Worker{
		id:           id,
		pool:         pool,
		collector:    collector,
		selector:     NewQuerySelector(queries, seed),
		rng:          rand.New(rand.NewSource(seed)),
		maxAccounts:  maxAccounts,
		maxCustomers: maxCustomers,
		maxBranches:  maxBranches,
	}
}

// NewWorkerWithConfig creates a new Worker with explicit entity counts.
func NewWorkerWithConfig(id int, pool *database.Pool, collector *metrics.Collector, queries []profile.QueryTemplate, seed int64, cfg WorkerConfig) *Worker {
	maxCustomers := cfg.MaxCustomers
	if maxCustomers < 1 {
		maxCustomers = cfg.MaxAccounts / 2
	}
	maxBranches := cfg.MaxBranches
	if maxBranches < 1 {
		maxBranches = cfg.MaxAccounts / 200
	}
	if maxBranches < 1 {
		maxBranches = 1
	}

	return &Worker{
		id:           id,
		pool:         pool,
		collector:    collector,
		selector:     NewQuerySelector(queries, seed),
		rng:          rand.New(rand.NewSource(seed)),
		maxAccounts:  cfg.MaxAccounts,
		maxCustomers: maxCustomers,
		maxBranches:  maxBranches,
	}
}

// Run executes queries until context is cancelled.
func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			w.executeQuery(ctx)
		}
	}
}

// executeQuery selects and executes a single query.
func (w *Worker) executeQuery(ctx context.Context) {
	query := w.selector.Next()
	if query == nil {
		return
	}

	// Generate parameters based on query type
	args := w.generateParams(query)

	// Acquire connection
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		w.collector.IncrementError(query.Name, "acquire_failed")
		w.errors.Add(1)
		return
	}
	defer conn.Release()

	// Execute query and measure latency
	start := time.Now()
	_, err = conn.Exec(ctx, query.SQL, args...)
	latency := time.Since(start)

	w.queries.Add(1)

	if err != nil {
		w.collector.IncrementError(query.Name, classifyError(err))
		w.errors.Add(1)
		return
	}

	w.collector.RecordLatency(query.Name, latency.Nanoseconds())
}

// generateParams generates query parameters based on query name.
func (w *Worker) generateParams(query *profile.QueryTemplate) []any {
	switch query.Name {
	// === Point Lookups ===
	case "point_select":
		// Random account ID
		return []any{w.rng.Intn(w.maxAccounts) + 1}

	// === Range Selects ===
	case "range_select":
		// Random balance range
		minBalance := float64(w.rng.Intn(50000))
		maxBalance := minBalance + float64(w.rng.Intn(50000))
		return []any{minBalance, maxBalance}

	// === Writes ===
	case "insert_tx":
		// account_id, type, amount, description
		accountID := w.rng.Intn(w.maxAccounts) + 1
		txTypes := []string{"deposit", "withdrawal", "transfer", "fee"}
		txType := txTypes[w.rng.Intn(len(txTypes))]
		amount := float64(w.rng.Intn(5000)) + w.rng.Float64()
		if txType == "withdrawal" || txType == "fee" {
			amount = -amount
		}
		desc := fmt.Sprintf("TX-%d-%d", w.id, time.Now().UnixNano())
		return []any{accountID, txType, amount, desc}

	case "update_balance":
		// amount, account_id
		amount := float64(w.rng.Intn(2000)) - 1000 // -1000 to +1000
		accountID := w.rng.Intn(w.maxAccounts) + 1
		return []any{amount, accountID}

	// === 2-way JOINs ===
	case "customer_accounts":
		// customer_id
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "account_transactions":
		// account_id
		return []any{w.rng.Intn(w.maxAccounts) + 1}

	case "branch_accounts":
		// branch_id
		return []any{w.rng.Intn(w.maxBranches) + 1}

	// === 3-way+ JOINs ===
	case "customer_tx_summary":
		// customer_id
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "branch_tx_summary":
		// branch_id
		return []any{w.rng.Intn(w.maxBranches) + 1}

	case "customer_audit_trail":
		// customer_id
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "full_customer_report":
		// customer_id
		return []any{w.rng.Intn(w.maxCustomers) + 1}

	case "complex_join":
		// status
		statuses := []string{"active", "inactive", "suspended", "pending"}
		return []any{statuses[w.rng.Intn(len(statuses))]}

	default:
		return nil
	}
}

// Stats returns worker statistics.
func (w *Worker) Stats() WorkerStats {
	return WorkerStats{
		ID:      w.id,
		Queries: w.queries.Load(),
		Errors:  w.errors.Load(),
	}
}

// classifyError categorizes an error for metrics.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()

	// Common PostgreSQL error patterns
	switch {
	case contains(errStr, "connection refused"):
		return "connection_refused"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "deadlock"):
		return "deadlock"
	case contains(errStr, "serialization"):
		return "serialization"
	case contains(errStr, "unique"):
		return "unique_violation"
	case contains(errStr, "foreign key"):
		return "fk_violation"
	case contains(errStr, "context canceled"):
		return "canceled"
	default:
		return "other"
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
