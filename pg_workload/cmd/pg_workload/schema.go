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

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Schema management commands",
	Long:  "Manage database schema: create tables, drop tables, view status, or migrate existing schema.",
}

// Schema command flags
var schemaCfg struct {
	ConfigFile string
	Force      bool
	Quiet      bool
}

var schemaCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create OLTP schema tables",
	Long: `Create the OLTP schema tables (customers, branches, account_types, accounts, transactions).

This creates the tables with proper foreign key relationships and indexes.
If tables already exist, use --force to drop and recreate them.

Examples:
  pg_workload schema create
  pg_workload schema create --force
`,
	RunE: runSchemaCreate,
}

var schemaDropCmd = &cobra.Command{
	Use:   "drop",
	Short: "Drop OLTP schema tables",
	Long: `Drop all OLTP schema tables.

This will permanently delete all tables and data. Use with caution.

Examples:
  pg_workload schema drop
  pg_workload schema drop --force  # Skip confirmation
`,
	RunE: runSchemaDrop,
}

var schemaStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show table status and statistics",
	Long: `Display status and statistics for all OLTP schema tables.

Shows:
  - Table existence and row counts
  - Table sizes (data, indexes, total)
  - Index information
  - Foreign key relationships

Examples:
  pg_workload schema status
`,
	RunE: runSchemaStatus,
}

var schemaMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate existing schema to latest version",
	Long: `Migrate an existing schema to the latest version.

This adds any missing tables, columns, indexes, or foreign keys
without losing existing data.

Examples:
  pg_workload schema migrate
  pg_workload schema migrate --force  # Skip confirmation
`,
	RunE: runSchemaMigrate,
}

func init() {
	// Add subcommands to schema
	schemaCmd.AddCommand(schemaCreateCmd)
	schemaCmd.AddCommand(schemaDropCmd)
	schemaCmd.AddCommand(schemaStatusCmd)
	schemaCmd.AddCommand(schemaMigrateCmd)

	// Common flags for all schema commands
	schemaCmd.PersistentFlags().StringVar(&schemaCfg.ConfigFile, "config", "", "configuration file")
	schemaCmd.PersistentFlags().BoolVarP(&schemaCfg.Quiet, "quiet", "q", false, "suppress output")

	// Command-specific flags
	schemaCreateCmd.Flags().BoolVarP(&schemaCfg.Force, "force", "f", false, "drop existing tables before creating")
	schemaDropCmd.Flags().BoolVarP(&schemaCfg.Force, "force", "f", false, "skip confirmation prompt")
	schemaMigrateCmd.Flags().BoolVarP(&schemaCfg.Force, "force", "f", false, "skip confirmation prompt")
}

func getSchemaPool(ctx context.Context) (*database.Pool, error) {
	cfg := loadSchemaConfig()

	poolCfg := database.PoolConfig{
		MinConns:          1,
		MaxConns:          5,
		MaxConnLifetime:   10 * time.Minute,
		MaxConnIdleTime:   2 * time.Minute,
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

func loadSchemaConfig() *config.Config {
	if schemaCfg.ConfigFile != "" {
		cfg, err := config.LoadConfig(schemaCfg.ConfigFile)
		if err != nil {
			schemaLog("Warning: failed to load config file: %v", err)
			return config.LoadConfigWithDefaults()
		}
		return cfg
	}
	return config.LoadConfigWithDefaults()
}

func schemaLog(format string, args ...interface{}) {
	if schemaCfg.Quiet {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func runSchemaCreate(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := getSchemaPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	cfg := loadSchemaConfig()
	schemaLog("Connected to %s@%s:%d/%s",
		cfg.Database.User, cfg.Database.Host, cfg.Database.Port, cfg.Database.DBName)

	if schemaCfg.Force {
		schemaLog("Dropping existing tables...")
		if err := schema.DropOLTPSchema(ctx, pool.Pool()); err != nil {
			schemaLog("Warning: error dropping tables: %v", err)
		}
	}

	schemaLog("Creating OLTP schema...")
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	schemaLog("Schema created successfully")

	// Show status after creation
	return showSchemaStatus(ctx, pool)
}

func runSchemaDrop(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if !schemaCfg.Force {
		fmt.Fprint(os.Stderr, "This will permanently delete all OLTP tables and data. Continue? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			schemaLog("Aborted")
			return nil
		}
	}

	pool, err := getSchemaPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	schemaLog("Dropping OLTP schema...")
	if err := schema.DropOLTPSchema(ctx, pool.Pool()); err != nil {
		return fmt.Errorf("dropping schema: %w", err)
	}

	schemaLog("Schema dropped successfully")
	return nil
}

func runSchemaStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := getSchemaPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	return showSchemaStatus(ctx, pool)
}

func showSchemaStatus(ctx context.Context, pool *database.Pool) error {
	// Get table statistics
	stats, err := schema.GetTableStats(ctx, pool.Pool())
	if err != nil {
		return fmt.Errorf("getting table stats: %w", err)
	}

	// Get table sizes
	sizes, err := schema.GetTableSizes(ctx, pool.Pool())
	if err != nil {
		schemaLog("Warning: could not get table sizes: %v", err)
	}

	fmt.Println()
	fmt.Println("OLTP Schema Status")
	fmt.Println("==================")
	fmt.Println()

	fmt.Println("Table Row Counts:")
	fmt.Printf("  %-20s %12d\n", "customers", stats.Customers)
	fmt.Printf("  %-20s %12d\n", "branches", stats.Branches)
	fmt.Printf("  %-20s %12d\n", "account_types", stats.AccountTypes)
	fmt.Printf("  %-20s %12d\n", "accounts", stats.Accounts)
	fmt.Printf("  %-20s %12d\n", "transactions", stats.Transactions)
	fmt.Println()

	if sizes != nil && len(sizes) > 0 {
		fmt.Println("Table Sizes:")
		var totalSize int64
		for table, size := range sizes {
			fmt.Printf("  %-20s %12s\n", table, schema.FormatBytes(size))
			totalSize += size
		}
		fmt.Printf("  %-20s %12s\n", "TOTAL", schema.FormatBytes(totalSize))
		fmt.Println()
	}

	// Calculate total data
	totalRows := stats.Customers + stats.Branches + stats.AccountTypes + stats.Accounts + stats.Transactions

	fmt.Printf("Total rows: %d\n", totalRows)
	if totalRows == 0 {
		fmt.Println("Schema is empty. Use 'pg_workload data seed' or 'pg_workload data load' to populate.")
	}

	return nil
}

func runSchemaMigrate(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if !schemaCfg.Force {
		fmt.Fprint(os.Stderr, "This will modify the existing schema. Continue? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			schemaLog("Aborted")
			return nil
		}
	}

	pool, err := getSchemaPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	schemaLog("Migrating schema...")

	// CreateOLTPSchema is idempotent - it uses CREATE TABLE IF NOT EXISTS
	if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
		return fmt.Errorf("migrating schema: %w", err)
	}

	schemaLog("Schema migration complete")
	return showSchemaStatus(ctx, pool)
}
