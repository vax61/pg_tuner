package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/myorg/pg_tuner/pg_workload/internal/config"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/schema"
)

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Data management commands",
	Long:  "Manage test data: seed with INSERT, load with COPY, clear tables, or view statistics.",
}

// Data command flags
var dataCfg struct {
	ConfigFile string
	Quiet      bool
	Force      bool

	// Seed flags
	Scale int
	Seed  int64

	// Load flags
	Size     string
	Parallel int
}

var dataSeedCmd = &cobra.Command{
	Use:   "seed",
	Short: "Seed data using INSERT (small datasets)",
	Long: `Seed test data using INSERT statements.

This is suitable for small to medium datasets (scale <= 10).
For large datasets, use 'pg_workload data load' with COPY.

The scale factor determines the amount of data:
  Scale 1:  5K customers, 50 branches, 10K accounts, 100K transactions
  Scale 10: 50K customers, 500 branches, 100K accounts, 1M transactions

Examples:
  pg_workload data seed --scale 1
  pg_workload data seed --scale 5 --seed 42
`,
	RunE: runDataSeed,
}

var dataLoadCmd = &cobra.Command{
	Use:   "load",
	Short: "Load data using COPY (large datasets)",
	Long: `Load large datasets using PostgreSQL COPY protocol.

COPY is 10-100x faster than INSERT for bulk loading.
Use this for datasets larger than ~100MB.

The --size flag accepts human-readable sizes:
  1GB, 5GB, 10GB, 50GB, etc.

Examples:
  pg_workload data load --size 1GB
  pg_workload data load --size 10GB --parallel 8
  pg_workload data load --size 50GB --parallel 16
`,
	RunE: runDataLoad,
}

var dataClearCmd = &cobra.Command{
	Use:   "clear",
	Short: "Clear all tables (TRUNCATE)",
	Long: `Clear all data from OLTP tables using TRUNCATE.

This is faster than DELETE and resets sequences.
Tables are truncated in the correct order respecting foreign keys.

Examples:
  pg_workload data clear
  pg_workload data clear --force  # Skip confirmation
`,
	RunE: runDataClear,
}

var dataStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show data statistics",
	Long: `Display data statistics for all OLTP tables.

Shows row counts, data sizes, and approximate data distribution.

Examples:
  pg_workload data stats
`,
	RunE: runDataStats,
}

func init() {
	// Add subcommands to data
	dataCmd.AddCommand(dataSeedCmd)
	dataCmd.AddCommand(dataLoadCmd)
	dataCmd.AddCommand(dataClearCmd)
	dataCmd.AddCommand(dataStatsCmd)

	// Common flags
	dataCmd.PersistentFlags().StringVar(&dataCfg.ConfigFile, "config", "", "configuration file")
	dataCmd.PersistentFlags().BoolVarP(&dataCfg.Quiet, "quiet", "q", false, "suppress output")

	// Seed flags
	dataSeedCmd.Flags().IntVar(&dataCfg.Scale, "scale", 1, "data scale factor (1 = 10K accounts)")
	dataSeedCmd.Flags().Int64Var(&dataCfg.Seed, "seed", 42, "random seed for reproducibility")

	// Load flags
	dataLoadCmd.Flags().StringVar(&dataCfg.Size, "size", "1GB", "target data size (e.g., 1GB, 10GB)")
	dataLoadCmd.Flags().IntVar(&dataCfg.Parallel, "parallel", 4, "parallel goroutines for COPY")
	dataLoadCmd.Flags().Int64Var(&dataCfg.Seed, "seed", 42, "random seed for reproducibility")

	// Clear flags
	dataClearCmd.Flags().BoolVarP(&dataCfg.Force, "force", "f", false, "skip confirmation prompt")
}

func getDataPool(ctx context.Context) (*database.Pool, error) {
	cfg := loadDataConfig()

	poolCfg := database.PoolConfig{
		MinConns:          2,
		MaxConns:          int32(dataCfg.Parallel + 2),
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}

	pool, err := database.NewPoolWithConfig(ctx, &cfg.Database, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	if err := pool.HealthCheck(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database health check failed: %w", err)
	}

	return pool, nil
}

func loadDataConfig() *config.Config {
	if dataCfg.ConfigFile != "" {
		cfg, err := config.LoadConfig(dataCfg.ConfigFile)
		if err != nil {
			dataLog("Warning: failed to load config file: %v", err)
			return config.LoadConfigWithDefaults()
		}
		return cfg
	}
	return config.LoadConfigWithDefaults()
}

func dataLog(format string, args ...interface{}) {
	if dataCfg.Quiet {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func runDataSeed(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	pool, err := getDataPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	cfg := loadDataConfig()
	dataLog("Connected to %s@%s:%d/%s",
		cfg.Database.User, cfg.Database.Host, cfg.Database.Port, cfg.Database.DBName)

	// Ensure schema exists
	dataLog("Ensuring schema exists...")
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	dataLog("Seeding data (scale=%d, seed=%d)...", dataCfg.Scale, dataCfg.Seed)
	startTime := time.Now()

	if err := schema.SeedOLTPData(ctx, pool.Pool(), dataCfg.Seed, dataCfg.Scale); err != nil {
		return fmt.Errorf("seeding data: %w", err)
	}

	elapsed := time.Since(startTime)
	dataLog("Seed complete in %s", elapsed.Round(time.Second))

	// Show stats
	return showDataStats(ctx, pool)
}

func runDataLoad(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Parse size
	targetSize, err := parseStorageSize(dataCfg.Size)
	if err != nil {
		return fmt.Errorf("invalid size: %w", err)
	}

	pool, err := getDataPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	cfg := loadDataConfig()
	dataLog("Connected to %s@%s:%d/%s",
		cfg.Database.User, cfg.Database.Host, cfg.Database.Port, cfg.Database.DBName)

	// Ensure schema exists
	dataLog("Ensuring schema exists...")
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	dataLog("Loading data with COPY (target: %s, parallel: %d)...",
		schema.FormatBytes(targetSize), dataCfg.Parallel)

	startTime := time.Now()

	preloadCfg := schema.PreloadConfig{
		TargetSize: targetSize,
		Parallel:   dataCfg.Parallel,
		Seed:       dataCfg.Seed,
		ProgressCallback: func(table string, loaded, total int64) {
			if total > 0 {
				pct := float64(loaded) / float64(total) * 100
				dataLog("  %s: %.1f%% (%d/%d)", table, pct, loaded, total)
			}
		},
	}

	pm := schema.NewPreloadManager(pool.Pool(), preloadCfg)
	if err := pm.Preload(ctx); err != nil {
		return fmt.Errorf("loading data: %w", err)
	}

	elapsed := time.Since(startTime)
	stats := pm.Stats()

	dataLog("")
	dataLog("Load complete in %s:", elapsed.Round(time.Second))
	dataLog("  Customers:    %d", stats.Customers)
	dataLog("  Branches:     %d", stats.Branches)
	dataLog("  Accounts:     %d", stats.Accounts)
	dataLog("  Transactions: %d", stats.Transactions)

	// Get actual size
	actualSize, err := schema.GetTotalDataSize(ctx, pool.Pool())
	if err == nil {
		dataLog("  Total size:   %s", schema.FormatBytes(actualSize))

		// Calculate throughput
		if elapsed.Seconds() > 0 {
			throughput := float64(actualSize) / elapsed.Seconds()
			dataLog("  Throughput:   %s/s", schema.FormatBytes(int64(throughput)))
		}
	}

	return nil
}

func runDataClear(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if !dataCfg.Force {
		fmt.Fprint(os.Stderr, "This will delete all data from OLTP tables. Continue? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			dataLog("Aborted")
			return nil
		}
	}

	pool, err := getDataPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	dataLog("Clearing tables...")

	// Truncate in correct order (respecting FK constraints)
	tables := []string{
		"transactions",
		"accounts",
		"customers",
		"branches",
		"account_types",
	}

	for _, table := range tables {
		_, err := pool.Pool().Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
		if err != nil {
			dataLog("Warning: could not truncate %s: %v", table, err)
		} else {
			dataLog("  Truncated %s", table)
		}
	}

	dataLog("Tables cleared")
	return nil
}

func runDataStats(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := getDataPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	return showDataStats(ctx, pool)
}

func showDataStats(ctx context.Context, pool *database.Pool) error {
	// Get table statistics
	stats, err := schema.GetTableStats(ctx, pool.Pool())
	if err != nil {
		return fmt.Errorf("getting table stats: %w", err)
	}

	// Get table sizes
	sizes, err := schema.GetTableSizes(ctx, pool.Pool())
	if err != nil {
		dataLog("Warning: could not get table sizes: %v", err)
	}

	fmt.Println()
	fmt.Println("Data Statistics")
	fmt.Println("===============")
	fmt.Println()

	// Calculate totals
	totalRows := stats.Customers + stats.Branches + stats.AccountTypes + stats.Accounts + stats.Transactions
	var totalSize int64

	// Print table with sizes
	fmt.Printf("%-20s %15s %15s\n", "Table", "Rows", "Size")
	fmt.Println("--------------------------------------------------")
	fmt.Printf("%-20s %15d %15s\n", "customers", stats.Customers, formatSize(sizes, "customers"))
	fmt.Printf("%-20s %15d %15s\n", "branches", stats.Branches, formatSize(sizes, "branches"))
	fmt.Printf("%-20s %15d %15s\n", "account_types", stats.AccountTypes, formatSize(sizes, "account_types"))
	fmt.Printf("%-20s %15d %15s\n", "accounts", stats.Accounts, formatSize(sizes, "accounts"))
	fmt.Printf("%-20s %15d %15s\n", "transactions", stats.Transactions, formatSize(sizes, "transactions"))
	fmt.Println("--------------------------------------------------")

	if sizes != nil {
		for _, size := range sizes {
			totalSize += size
		}
	}
	fmt.Printf("%-20s %15d %15s\n", "TOTAL", totalRows, schema.FormatBytes(totalSize))
	fmt.Println()

	// Data distribution
	if stats.Accounts > 0 && stats.Customers > 0 {
		fmt.Println("Data Distribution:")
		fmt.Printf("  Accounts per customer: %.1f\n", float64(stats.Accounts)/float64(stats.Customers))
		if stats.Branches > 0 {
			fmt.Printf("  Accounts per branch:   %.1f\n", float64(stats.Accounts)/float64(stats.Branches))
		}
		if stats.Transactions > 0 {
			fmt.Printf("  Transactions per account: %.1f\n", float64(stats.Transactions)/float64(stats.Accounts))
		}
		fmt.Println()
	}

	// Estimated scale
	estimatedScale := float64(stats.Accounts) / 10000.0
	if estimatedScale >= 1 {
		fmt.Printf("Estimated scale factor: %.1f\n", estimatedScale)
	}

	return nil
}

func formatSize(sizes map[string]int64, table string) string {
	if sizes == nil {
		return "-"
	}
	if size, ok := sizes[table]; ok {
		return schema.FormatBytes(size)
	}
	return "-"
}
