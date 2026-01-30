package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/config"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/executor"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/report"
	"github.com/myorg/pg_tuner/pg_workload/internal/schema"
)

func skipIfNoPostgres(t *testing.T) {
	if os.Getenv("PGHOST") == "" && os.Getenv("PG_TEST") == "" {
		t.Skip("Skipping integration test: set PGHOST or PG_TEST=1 to run")
	}
}

func getTestConfig() *config.DatabaseConfig {
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
	if v := os.Getenv("PGUSER"); v != "" {
		cfg.User = v
	}
	if v := os.Getenv("PGPASSWORD"); v != "" {
		cfg.Password = v
	}
	if v := os.Getenv("PGDATABASE"); v != "" {
		cfg.DBName = v
	}

	return cfg
}

func TestBurstWorkloadIntegration(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup database connection
	dbCfg := getTestConfig()
	pool, err := database.NewPool(ctx, dbCfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Setup schema
	t.Log("Creating OLTP schema...")
	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer schema.DropOLTPSchema(context.Background(), pool.Pool())

	// Seed data
	t.Log("Seeding test data (scale=1)...")
	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("Failed to seed data: %v", err)
	}

	// Verify data
	accountCount, _ := schema.GetAccountCount(ctx, pool.Pool())
	txCount, _ := schema.GetTransactionCount(ctx, pool.Pool())
	t.Logf("Seeded %d accounts, %d transactions", accountCount, txCount)

	// Create metrics collector
	collector := metrics.NewCollector()

	// Create executor
	execCfg := executor.Config{
		Duration:    30 * time.Second,
		Warmup:      5 * time.Second,
		Cooldown:    5 * time.Second,
		Workers:     4,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := executor.NewExecutor(pool, collector, execCfg)

	// Run workload
	startTime := time.Now()
	t.Log("Starting burst workload (30s + warmup/cooldown)...")

	if err := exec.Run(ctx); err != nil {
		t.Fatalf("Workload execution failed: %v", err)
	}

	endTime := time.Now()
	t.Logf("Workload completed in %s", endTime.Sub(startTime))

	// Generate report
	runInfo := report.RunInfo{
		StartTime:   startTime,
		EndTime:     endTime,
		Duration:    endTime.Sub(startTime),
		Mode:        "burst",
		Profile:     "oltp_standard",
		Seed:        42,
		Workers:     4,
		Connections: 10,
	}

	snapshot := collector.GetSnapshot()
	rpt := report.GenerateReport(runInfo, snapshot)

	// Validate report
	t.Log("Validating report...")

	if rpt.Version != "1.0" {
		t.Errorf("Expected version 1.0, got %s", rpt.Version)
	}

	if rpt.Summary.TotalQueries == 0 {
		t.Error("No queries executed")
	}

	if rpt.Summary.QPS <= 0 {
		t.Error("QPS should be positive")
	}

	if len(rpt.Latencies) == 0 {
		t.Error("No latency data recorded")
	}

	// Check all query types are represented
	expectedOps := []string{"point_select", "range_select", "insert_tx", "update_balance", "complex_join"}
	for _, op := range expectedOps {
		if _, ok := rpt.Latencies[op]; !ok {
			t.Errorf("Missing latency data for %s", op)
		}
	}

	// Validate JSON output
	jsonData, err := rpt.ToJSON()
	if err != nil {
		t.Fatalf("Failed to serialize report: %v", err)
	}

	// Verify JSON is valid
	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("Invalid JSON output: %v", err)
	}

	// Log summary
	t.Logf("Results: %d queries, %.1f QPS, %d errors (%.2f%%)",
		rpt.Summary.TotalQueries,
		rpt.Summary.QPS,
		rpt.Summary.TotalErrors,
		rpt.Summary.ErrorRate)

	// Log per-operation stats
	for name, lat := range rpt.Latencies {
		t.Logf("  %s: %d queries, p50=%s, p99=%s",
			name, lat.Count, lat.P50, lat.P99)
	}
}

func TestShortBurstWithCancellation(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup
	dbCfg := getTestConfig()
	pool, err := database.NewPool(ctx, dbCfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer schema.DropOLTPSchema(context.Background(), pool.Pool())

	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("Failed to seed data: %v", err)
	}

	collector := metrics.NewCollector()

	execCfg := executor.Config{
		Duration:    60 * time.Second, // Long duration
		Workers:     2,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := executor.NewExecutor(pool, collector, execCfg)

	// Cancel after 5 seconds
	cancelCtx, cancelFunc := context.WithCancel(ctx)

	done := make(chan error)
	go func() {
		done <- exec.Run(cancelCtx)
	}()

	time.Sleep(5 * time.Second)
	t.Log("Cancelling workload...")
	cancelFunc()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Expected context.Canceled or nil, got: %v", err)
		}
		t.Log("Workload cancelled successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Workload did not stop after cancellation")
	}

	// Should still have some data
	snapshot := collector.GetSnapshot()
	if snapshot.TotalQueries == 0 {
		t.Error("Expected some queries before cancellation")
	}
	t.Logf("Executed %d queries before cancellation", snapshot.TotalQueries)
}

func TestReportJSONValidity(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Setup
	dbCfg := getTestConfig()
	pool, err := database.NewPool(ctx, dbCfg)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	_ = schema.DropOLTPSchema(ctx, pool.Pool())
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	defer schema.DropOLTPSchema(context.Background(), pool.Pool())

	if err := schema.SeedOLTPData(ctx, pool.Pool(), 42, 1); err != nil {
		t.Fatalf("Failed to seed data: %v", err)
	}

	collector := metrics.NewCollector()

	execCfg := executor.Config{
		Duration:    15 * time.Second,
		Warmup:      5 * time.Second,
		Workers:     2,
		Seed:        42,
		Queries:     profile.OLTPQueries,
		MaxAccounts: 10000,
	}

	exec := executor.NewExecutor(pool, collector, execCfg)

	startTime := time.Now()
	if err := exec.Run(ctx); err != nil {
		t.Fatalf("Workload failed: %v", err)
	}
	endTime := time.Now()

	// Generate and validate report
	runInfo := report.RunInfo{
		StartTime:   startTime,
		EndTime:     endTime,
		Duration:    endTime.Sub(startTime),
		Mode:        "burst",
		Profile:     "oltp_standard",
		Seed:        42,
		Workers:     2,
		Connections: 10,
	}

	snapshot := collector.GetSnapshot()
	rpt := report.GenerateReport(runInfo, snapshot)

	// Get JSON
	jsonData, err := rpt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Parse and validate structure
	var parsed struct {
		Version string `json:"version"`
		RunInfo struct {
			StartTime   string  `json:"start_time"`
			Duration    string  `json:"duration"`
			DurationSec float64 `json:"duration_sec"`
			Mode        string  `json:"mode"`
		} `json:"run_info"`
		Summary struct {
			TotalQueries int64   `json:"total_queries"`
			QPS          float64 `json:"qps"`
			ErrorRate    float64 `json:"error_rate_pct"`
		} `json:"summary"`
		Latencies map[string]struct {
			Operation string `json:"operation"`
			Count     int64  `json:"count"`
			P50       string `json:"p50"`
			P99       string `json:"p99"`
			P50Us     int64  `json:"p50_us"`
			P99Us     int64  `json:"p99_us"`
		} `json:"latencies"`
	}

	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Validate required fields
	if parsed.Version == "" {
		t.Error("Missing version")
	}
	if parsed.RunInfo.StartTime == "" {
		t.Error("Missing start_time")
	}
	if parsed.RunInfo.Mode != "burst" {
		t.Errorf("Expected mode 'burst', got '%s'", parsed.RunInfo.Mode)
	}
	if parsed.Summary.TotalQueries == 0 {
		t.Error("No queries in summary")
	}

	// Validate latency data has both human-readable and numeric values
	for name, lat := range parsed.Latencies {
		if lat.P50 == "" {
			t.Errorf("%s: missing p50 string", name)
		}
		if lat.P99 == "" {
			t.Errorf("%s: missing p99 string", name)
		}
		if lat.P50Us == 0 && lat.Count > 0 {
			t.Errorf("%s: missing p50_us numeric value", name)
		}
	}

	t.Logf("JSON report validated: %d bytes", len(jsonData))
}
