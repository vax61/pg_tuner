package executor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/config"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/schema"
)

func TestNewExecutor(t *testing.T) {
	collector := metrics.NewCollector()
	cfg := Config{
		Duration: 1 * time.Second,
		Workers:  2,
		Seed:     42,
		Queries:  profile.OLTPQueries,
	}

	// Test with nil pool (shouldn't panic)
	exec := NewExecutor(nil, collector, cfg)
	if exec == nil {
		t.Fatal("NewExecutor returned nil")
	}
	if exec.Phase() != PhaseIdle {
		t.Errorf("expected phase idle, got %s", exec.Phase())
	}
}

func TestExecutorPhases(t *testing.T) {
	skipIfNoPostgres(t)

	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()

	// Setup schema
	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}
	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("SeedOLTPData failed: %v", err)
	}
	defer schema.DropOLTPSchema(ctx, pool.Pool())

	collector := metrics.NewCollector()
	cfg := Config{
		Duration:    500 * time.Millisecond,
		Warmup:      100 * time.Millisecond,
		Cooldown:    100 * time.Millisecond,
		Workers:     2,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := NewExecutor(pool, collector, cfg)

	// Track phases seen
	phases := make(map[Phase]bool)
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				phases[exec.Phase()] = true
			}
		}
	}()

	err := exec.Run(ctx)
	close(done)

	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify we went through phases
	if !phases[PhaseWarmup] {
		t.Error("never saw warmup phase")
	}
	if !phases[PhaseRunning] {
		t.Error("never saw running phase")
	}
}

func TestExecutorMetrics(t *testing.T) {
	skipIfNoPostgres(t)

	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()

	// Setup schema
	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}
	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("SeedOLTPData failed: %v", err)
	}
	defer schema.DropOLTPSchema(ctx, pool.Pool())

	collector := metrics.NewCollector()
	cfg := Config{
		Duration:    500 * time.Millisecond,
		Warmup:      0,
		Cooldown:    0,
		Workers:     4,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := NewExecutor(pool, collector, cfg)

	if err := exec.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Check metrics were recorded
	snap := collector.GetSnapshot()
	if snap.TotalQueries == 0 {
		t.Error("no queries recorded")
	}

	// Check executor stats
	stats := exec.Stats()
	if stats.TotalQueries == 0 {
		t.Error("executor stats show no queries")
	}

	t.Logf("Total queries: %d, QPS: %.2f", snap.TotalQueries, snap.QPS)
}

func TestExecutorCancellation(t *testing.T) {
	skipIfNoPostgres(t)

	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Setup schema
	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}
	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("SeedOLTPData failed: %v", err)
	}
	defer schema.DropOLTPSchema(context.Background(), pool.Pool())

	collector := metrics.NewCollector()
	cfg := Config{
		Duration:    10 * time.Second, // Long duration
		Workers:     2,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := NewExecutor(pool, collector, cfg)

	done := make(chan error)
	go func() {
		done <- exec.Run(ctx)
	}()

	// Cancel after short time
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Should return quickly
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("executor did not stop after cancellation")
	}
}

func skipIfNoPostgres(t *testing.T) {
	if os.Getenv("PGHOST") == "" && os.Getenv("PG_TEST") == "" {
		t.Skip("Skipping integration test: set PGHOST or PG_TEST=1 to run")
	}
}

func getTestPool(t *testing.T) *database.Pool {
	cfg := &config.DatabaseConfig{
		Host:    "localhost",
		Port:    5432,
		User:    "postgres",
		DBName:  "postgres",
		SSLMode: "disable",
	}

	if v := os.Getenv("PGHOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("PGPASSWORD"); v != "" {
		cfg.Password = v
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := database.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	return pool
}
