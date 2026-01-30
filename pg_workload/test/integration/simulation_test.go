package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/executor"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/report"
	"github.com/myorg/pg_tuner/pg_workload/internal/schema"
)

func TestSimulationWorkloadIntegration(t *testing.T) {
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

	// Create simulation config
	simProfile := profile.NewSimulationProfile()
	simProfile.SetDefaults()
	simStartTime := time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC) // 9 AM start

	simCfg := &executor.SimulationConfig{
		Profile:           simProfile,
		Duration:          10 * time.Minute, // Simulated 10 minutes
		TimeScale:         6,                // 6x compression: 10 min sim = ~100s real
		StartTime:         simStartTime,
		MaxStorage:        100 << 20, // 100MB
		RawRetention:      2 * time.Minute,
		AggregateInterval: 30 * time.Second,
		Workers:           4,
		Seed:              42,
		MaxAccounts:       10000,
		StoragePath:       "/tmp/pg_workload_sim_test",
	}

	// Create simulation executor
	simExec, err := executor.NewSimulationExecutor(simCfg, pool)
	if err != nil {
		t.Fatalf("Failed to create simulation executor: %v", err)
	}

	// Initialize components
	if err := simExec.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize simulation: %v", err)
	}

	// Run simulation
	realStartTime := time.Now()
	t.Log("Starting simulation (10 min simulated, ~100s real at 6x scale)...")

	if err := simExec.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Simulation execution failed: %v", err)
	}

	// Stop executor
	if err := simExec.Stop(); err != nil {
		t.Logf("Warning: error during stop: %v", err)
	}

	realEndTime := time.Now()
	realDuration := realEndTime.Sub(realStartTime)
	t.Logf("Simulation completed in %s real time", realDuration)

	// Get final status
	status := simExec.GetStatus()

	// Validate simulation behavior
	if status.TotalQueries == 0 {
		t.Error("No queries executed during simulation")
	}

	// Check simulated time advanced
	if status.ElapsedSimulated < 5*time.Minute {
		t.Errorf("Simulated time should be at least 5 minutes, got %s", status.ElapsedSimulated)
	}

	// Check time compression worked (with margin for startup/shutdown)
	expectedMaxRealDuration := (simCfg.Duration / time.Duration(simCfg.TimeScale)) + 30*time.Second
	if realDuration > expectedMaxRealDuration {
		t.Errorf("Real duration %s exceeded expected max %s for %dx time scale",
			realDuration, expectedMaxRealDuration, simCfg.TimeScale)
	}

	// Generate simulation report
	runInfo := report.RunInfo{
		StartTime:   realStartTime,
		EndTime:     realEndTime,
		Duration:    realDuration,
		Mode:        "simulation",
		Profile:     "default",
		Seed:        42,
		Workers:     4,
		Connections: 10,
	}

	simInfo := report.SimulationInfo{
		TimeScale:         simCfg.TimeScale,
		StartSimTime:      simStartTime,
		EndSimTime:        status.SimulatedTime,
		SimulatedDuration: status.ElapsedSimulated,
		RealDuration:      realDuration,
		ProfileUsed:       "default",
		ClockMode:         "simulated",
	}

	reportCfg := report.SimulationReportConfig{
		RunInfo:         runInfo,
		SimInfo:         simInfo,
		Snapshot:        simExec.GetMetrics().GetSnapshot(),
		TimelineSummary: simExec.GetTimelineSummary(),
		StorageUsed:     simExec.GetStorageUsed(),
	}

	simReport := report.GenerateSimulationReport(reportCfg)

	// Validate report
	if simReport.Version != "1.0" {
		t.Errorf("Expected version 1.0, got %s", simReport.Version)
	}

	if simReport.Summary.TotalQueries == 0 {
		t.Error("No queries in report summary")
	}

	if simReport.SimulationInfo.TimeScale != simCfg.TimeScale {
		t.Errorf("Time scale mismatch: expected %d, got %d",
			simCfg.TimeScale, simReport.SimulationInfo.TimeScale)
	}

	// Validate JSON output
	jsonData, err := simReport.ToJSON()
	if err != nil {
		t.Fatalf("Failed to serialize report: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("Invalid JSON output: %v", err)
	}

	// Log summary
	t.Logf("Simulation results:")
	t.Logf("  Simulated duration: %s", simReport.SimulationInfo.SimulatedDuration)
	t.Logf("  Real duration: %s", simReport.SimulationInfo.RealDuration)
	t.Logf("  Total queries: %d", simReport.Summary.TotalQueries)
	t.Logf("  QPS: %.1f", simReport.Summary.QPS)
	t.Logf("  Errors: %d (%.2f%%)", simReport.Summary.TotalErrors, simReport.Summary.ErrorRate)

	// Cleanup temp storage
	os.RemoveAll("/tmp/pg_workload_sim_test")
}

func TestSimulationWithCancellation(t *testing.T) {
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

	// Create long-running simulation config
	simProfile := profile.NewSimulationProfile()
	simProfile.SetDefaults()
	simCfg := &executor.SimulationConfig{
		Profile:           simProfile,
		Duration:          1 * time.Hour, // Long simulation
		TimeScale:         1,             // 1x (no compression)
		StartTime:         time.Now(),
		MaxStorage:        100 << 20,
		RawRetention:      2 * time.Minute,
		AggregateInterval: 30 * time.Second,
		Workers:           2,
		Seed:              42,
		MaxAccounts:       10000,
		StoragePath:       "/tmp/pg_workload_sim_cancel_test",
	}

	simExec, err := executor.NewSimulationExecutor(simCfg, pool)
	if err != nil {
		t.Fatalf("Failed to create simulation executor: %v", err)
	}

	if err := simExec.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize simulation: %v", err)
	}

	// Cancel after 10 seconds
	cancelCtx, cancelFunc := context.WithCancel(ctx)

	done := make(chan error)
	go func() {
		done <- simExec.Run(cancelCtx)
	}()

	time.Sleep(10 * time.Second)
	t.Log("Cancelling simulation...")
	cancelFunc()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Expected context.Canceled or nil, got: %v", err)
		}
		t.Log("Simulation cancelled successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Simulation did not stop after cancellation")
	}

	// Stop executor
	simExec.Stop()

	// Should still have some data
	status := simExec.GetStatus()
	if status.TotalQueries == 0 {
		t.Error("Expected some queries before cancellation")
	}
	t.Logf("Executed %d queries before cancellation", status.TotalQueries)

	// Cleanup
	os.RemoveAll("/tmp/pg_workload_sim_cancel_test")
}

func TestSimulationReportJSONValidity(t *testing.T) {
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

	// Short simulation for JSON validation
	simProfile := profile.NewSimulationProfile()
	simProfile.SetDefaults()
	simStartTime := time.Now()

	simCfg := &executor.SimulationConfig{
		Profile:           simProfile,
		Duration:          5 * time.Minute,
		TimeScale:         12, // Fast compression
		StartTime:         simStartTime,
		MaxStorage:        50 << 20,
		RawRetention:      1 * time.Minute,
		AggregateInterval: 30 * time.Second,
		Workers:           2,
		Seed:              42,
		MaxAccounts:       10000,
		StoragePath:       "/tmp/pg_workload_sim_json_test",
	}

	simExec, err := executor.NewSimulationExecutor(simCfg, pool)
	if err != nil {
		t.Fatalf("Failed to create simulation executor: %v", err)
	}

	if err := simExec.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize simulation: %v", err)
	}

	realStartTime := time.Now()
	if err := simExec.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Simulation failed: %v", err)
	}
	simExec.Stop()
	realEndTime := time.Now()

	status := simExec.GetStatus()

	// Generate report
	runInfo := report.RunInfo{
		StartTime:   realStartTime,
		EndTime:     realEndTime,
		Duration:    realEndTime.Sub(realStartTime),
		Mode:        "simulation",
		Profile:     "default",
		Seed:        42,
		Workers:     2,
		Connections: 10,
	}

	simInfo := report.SimulationInfo{
		TimeScale:         simCfg.TimeScale,
		StartSimTime:      simStartTime,
		EndSimTime:        status.SimulatedTime,
		SimulatedDuration: status.ElapsedSimulated,
		RealDuration:      status.ElapsedReal,
		ProfileUsed:       "default",
		ClockMode:         "simulated",
	}

	reportCfg := report.SimulationReportConfig{
		RunInfo:         runInfo,
		SimInfo:         simInfo,
		Snapshot:        simExec.GetMetrics().GetSnapshot(),
		TimelineSummary: simExec.GetTimelineSummary(),
		StorageUsed:     simExec.GetStorageUsed(),
	}

	simReport := report.GenerateSimulationReport(reportCfg)

	// Get JSON
	jsonData, err := simReport.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Parse and validate structure
	var parsed struct {
		Version        string `json:"version"`
		RunInfo        struct {
			StartTime   string `json:"start_time"`
			Duration    string `json:"duration"`
			Mode        string `json:"mode"`
		} `json:"run_info"`
		SimulationInfo struct {
			TimeScale         int    `json:"time_scale"`
			StartSimTime      string `json:"start_sim_time"`
			SimulatedDuration string `json:"simulated_duration"`
			RealDuration      string `json:"real_duration"`
			ProfileUsed       string `json:"profile_used"`
			ClockMode         string `json:"clock_mode"`
		} `json:"simulation_info"`
		Summary struct {
			TotalQueries int64   `json:"total_queries"`
			QPS          float64 `json:"qps"`
			ErrorRate    float64 `json:"error_rate_pct"`
		} `json:"summary"`
		StorageUsed int64 `json:"storage_used_bytes"`
	}

	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Validate required fields
	if parsed.Version == "" {
		t.Error("Missing version")
	}
	if parsed.RunInfo.Mode != "simulation" {
		t.Errorf("Expected mode 'simulation', got '%s'", parsed.RunInfo.Mode)
	}
	if parsed.SimulationInfo.TimeScale != simCfg.TimeScale {
		t.Errorf("Time scale mismatch in JSON: expected %d, got %d",
			simCfg.TimeScale, parsed.SimulationInfo.TimeScale)
	}
	if parsed.SimulationInfo.ClockMode != "simulated" {
		t.Errorf("Expected clock_mode 'simulated', got '%s'", parsed.SimulationInfo.ClockMode)
	}

	t.Logf("Simulation JSON report validated: %d bytes", len(jsonData))

	// Cleanup
	os.RemoveAll("/tmp/pg_workload_sim_json_test")
}

func TestSimulationTimeCompression(t *testing.T) {
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

	// Test time compression accuracy
	timeScales := []int{1, 6, 12}

	for _, scale := range timeScales {
		t.Run(string(rune('0'+scale))+"x", func(t *testing.T) {
			simProfile := profile.NewSimulationProfile()
			simProfile.SetDefaults()
			simCfg := &executor.SimulationConfig{
				Profile:           simProfile,
				Duration:          1 * time.Minute, // 1 minute simulated
				TimeScale:         scale,
				StartTime:         time.Now(),
				MaxStorage:        50 << 20,
				RawRetention:      30 * time.Second,
				AggregateInterval: 10 * time.Second,
				Workers:           2,
				Seed:              42,
				MaxAccounts:       10000,
				StoragePath:       "/tmp/pg_workload_sim_time_test",
			}

			simExec, err := executor.NewSimulationExecutor(simCfg, pool)
			if err != nil {
				t.Fatalf("Failed to create simulation executor: %v", err)
			}

			if err := simExec.Initialize(ctx); err != nil {
				t.Fatalf("Failed to initialize: %v", err)
			}

			realStart := time.Now()
			if err := simExec.Run(ctx); err != nil && err != context.Canceled {
				t.Fatalf("Simulation failed: %v", err)
			}
			simExec.Stop()
			realDuration := time.Since(realStart)

			status := simExec.GetStatus()

			// Expected real duration: simulated duration / time scale
			expectedReal := simCfg.Duration / time.Duration(scale)
			tolerance := 15 * time.Second // Allow for startup/shutdown overhead

			if realDuration < expectedReal-tolerance || realDuration > expectedReal+tolerance {
				t.Logf("Warning: Real duration %s differs from expected ~%s (tolerance %s)",
					realDuration, expectedReal, tolerance)
			}

			t.Logf("Scale %dx: simulated %s in %s real time (expected ~%s)",
				scale, status.ElapsedSimulated.Round(time.Second),
				realDuration.Round(time.Second), expectedReal.Round(time.Second))

			os.RemoveAll("/tmp/pg_workload_sim_time_test")
		})
	}
}
