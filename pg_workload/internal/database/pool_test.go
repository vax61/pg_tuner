package database

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/config"
)

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

func skipIfNoPostgres(t *testing.T) {
	if os.Getenv("PGHOST") == "" && os.Getenv("PG_TEST") == "" {
		t.Skip("Skipping integration test: set PGHOST or PG_TEST=1 to run")
	}
}

func TestNewPool(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	if pool.pool == nil {
		t.Error("expected pool to be initialized")
	}
}

func TestNewPoolWithConfig(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	poolCfg := PoolConfig{
		MinConns:          1,
		MaxConns:          5,
		MaxConnLifetime:   10 * time.Minute,
		MaxConnIdleTime:   2 * time.Minute,
		HealthCheckPeriod: 15 * time.Second,
	}

	pool, err := NewPoolWithConfig(ctx, cfg, poolCfg)
	if err != nil {
		t.Fatalf("NewPoolWithConfig failed: %v", err)
	}
	defer pool.Close()

	stats := pool.Stats()
	if stats.MaxConns() != 5 {
		t.Errorf("expected MaxConns 5, got %d", stats.MaxConns())
	}
}

func TestHealthCheck(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	if err := pool.HealthCheck(ctx); err != nil {
		t.Errorf("HealthCheck failed: %v", err)
	}
}

func TestAcquireRelease(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Execute a simple query
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		t.Errorf("QueryRow failed: %v", err)
	}
	if result != 1 {
		t.Errorf("expected 1, got %d", result)
	}

	conn.Release()

	// Verify stats after release
	stats := pool.Stats()
	if stats.AcquiredConns() != 0 {
		t.Errorf("expected 0 acquired conns after release, got %d", stats.AcquiredConns())
	}
}

func TestExec(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	// Execute a simple statement
	err = pool.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Errorf("Exec failed: %v", err)
	}
}

func TestPoolStats(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := getTestConfig()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	stats := pool.Stats()
	if stats == nil {
		t.Error("expected stats to be non-nil")
	}
	if stats.MaxConns() != 10 {
		t.Errorf("expected default MaxConns 10, got %d", stats.MaxConns())
	}
}

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.MinConns != 2 {
		t.Errorf("expected MinConns 2, got %d", cfg.MinConns)
	}
	if cfg.MaxConns != 10 {
		t.Errorf("expected MaxConns 10, got %d", cfg.MaxConns)
	}
	if cfg.MaxConnLifetime != 30*time.Minute {
		t.Errorf("expected MaxConnLifetime 30m, got %v", cfg.MaxConnLifetime)
	}
	if cfg.HealthCheckPeriod != 30*time.Second {
		t.Errorf("expected HealthCheckPeriod 30s, got %v", cfg.HealthCheckPeriod)
	}
}

func TestNewPoolInvalidHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := &config.DatabaseConfig{
		Host:    "invalid-host-that-does-not-exist.local",
		Port:    5432,
		User:    "postgres",
		DBName:  "postgres",
		SSLMode: "disable",
	}

	// pgxpool creates the pool lazily, so NewPool may succeed
	// but HealthCheck should fail
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		// Connection failed immediately - this is acceptable
		return
	}
	defer pool.Close()

	// If pool was created, HealthCheck should fail
	err = pool.HealthCheck(ctx)
	if err == nil {
		t.Error("expected HealthCheck to fail for invalid host")
	}
}
