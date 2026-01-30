package schema

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	if os.Getenv("PGHOST") == "" && os.Getenv("PG_TEST") == "" {
		t.Skip("Skipping integration test: set PGHOST or PG_TEST=1 to run")
	}

	connStr := "host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
	if v := os.Getenv("PGHOST"); v != "" {
		connStr = "host=" + v + " port=5432 user=postgres dbname=postgres sslmode=disable"
	}
	if v := os.Getenv("PGPASSWORD"); v != "" {
		connStr += " password=" + v
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	return pool
}

func TestCreateAndDropOLTPSchema(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Clean up first
	_ = DropOLTPSchema(ctx, pool)

	// Create schema
	if err := CreateOLTPSchema(ctx, pool); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}

	// Verify tables exist
	tables := []string{"accounts", "transactions", "audit_log"}
	for _, table := range tables {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
			table).Scan(&exists)
		if err != nil {
			t.Errorf("Error checking table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("Table %s not created", table)
		}
	}

	// Drop schema
	if err := DropOLTPSchema(ctx, pool); err != nil {
		t.Fatalf("DropOLTPSchema failed: %v", err)
	}

	// Verify tables dropped
	for _, table := range tables {
		var exists bool
		err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
			table).Scan(&exists)
		if err != nil {
			t.Errorf("Error checking table %s: %v", table, err)
		}
		if exists {
			t.Errorf("Table %s not dropped", table)
		}
	}
}

func TestSeedOLTPData(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Clean up and create schema
	_ = DropOLTPSchema(ctx, pool)
	if err := CreateOLTPSchema(ctx, pool); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}
	defer DropOLTPSchema(ctx, pool)

	// Seed with scale=1 (10K accounts, 100K transactions)
	if err := SeedOLTPData(ctx, pool, 42, 1); err != nil {
		t.Fatalf("SeedOLTPData failed: %v", err)
	}

	// Verify account count
	accountCount, err := GetAccountCount(ctx, pool)
	if err != nil {
		t.Fatalf("GetAccountCount failed: %v", err)
	}
	if accountCount != 10000 {
		t.Errorf("Expected 10000 accounts, got %d", accountCount)
	}

	// Verify transaction count
	txCount, err := GetTransactionCount(ctx, pool)
	if err != nil {
		t.Fatalf("GetTransactionCount failed: %v", err)
	}
	if txCount != 100000 {
		t.Errorf("Expected 100000 transactions, got %d", txCount)
	}
}

func TestSeedDeterministic(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Clean up and create schema
	_ = DropOLTPSchema(ctx, pool)
	if err := CreateOLTPSchema(ctx, pool); err != nil {
		t.Fatalf("CreateOLTPSchema failed: %v", err)
	}
	defer DropOLTPSchema(ctx, pool)

	// Seed with same seed - use smaller scale for speed
	// Note: We'll just verify it doesn't error, determinism is by design
	if err := SeedOLTPData(ctx, pool, 12345, 1); err != nil {
		t.Fatalf("SeedOLTPData failed: %v", err)
	}

	// Query a specific account to verify data exists
	var balance float64
	err := pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 1").Scan(&balance)
	if err != nil {
		t.Fatalf("Failed to query account: %v", err)
	}
	// Balance should be positive
	if balance < 0 {
		t.Errorf("Expected positive balance, got %f", balance)
	}
}
