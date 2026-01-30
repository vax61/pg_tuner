package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "pg_workload",
	Short:   "PostgreSQL workload generator",
	Long: `pg_workload generates configurable workloads against PostgreSQL for benchmarking and tuning.

Commands:
  Workload Execution:
    run         Execute a workload profile (burst or simulation mode)
    simulate    Run a time-compressed workload simulation

  Schema Management:
    schema create   Create OLTP schema tables
    schema drop     Drop OLTP schema tables
    schema status   Show table status and statistics
    schema migrate  Migrate existing schema to latest version

  Data Management:
    data seed    Seed data using INSERT (small datasets)
    data load    Load data using COPY (large datasets)
    data clear   Clear all tables (TRUNCATE)
    data stats   Show data statistics

  Profile Management:
    profile list      List available profiles
    profile show      Show profile details
    profile validate  Validate a profile YAML file
    profile generate  Generate a new profile template

  Report Analysis:
    report show     Show formatted report
    report compare  Compare two reports
    report export   Export report to CSV/HTML

  Configuration:
    config init      Generate example configuration file
    config validate  Validate configuration file
    config test      Test database connection
    config show      Show current configuration

Examples:
  # Quick start: create schema and run workload
  pg_workload schema create
  pg_workload data seed --scale 1
  pg_workload run --duration 5m --output results.json

  # Large scale benchmark
  pg_workload data load --size 10GB
  pg_workload run --duration 15m --workers 8 --connections 20

  # Compare results
  pg_workload report compare baseline.json optimized.json`,
	Version: Version,
}

func init() {
	// Workload execution commands
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(simulateCmd)

	// Management commands
	rootCmd.AddCommand(schemaCmd)
	rootCmd.AddCommand(dataCmd)
	rootCmd.AddCommand(profileCmd)
	rootCmd.AddCommand(reportCmd)
	rootCmd.AddCommand(configCmd)

	// Info commands
	rootCmd.AddCommand(versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
