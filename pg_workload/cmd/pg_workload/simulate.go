package main

import (
	"time"

	"github.com/spf13/cobra"
)

var simulateCmd = &cobra.Command{
	Use:   "simulate",
	Short: "Run a time-compressed workload simulation",
	Long: `Run a time-compressed workload simulation against PostgreSQL.

Simulation mode runs workloads with time compression, allowing you to simulate
hours or days of activity in minutes. This is useful for testing time-based
patterns, events, and long-running scenarios.

Examples:
  # Run OLTP simulation for 5 minutes at 12x speed (simulates 1 hour)
  pg_workload simulate --profile oltp --duration 5m --time-scale 12

  # Run OLAP simulation starting at 9:00 AM simulated time
  pg_workload simulate --profile olap --duration 10m --time-scale 6 --start-time 09:00

  # Run mixed workload with timeline output
  pg_workload simulate --profile mixed --duration 5m --time-scale 12 --timeline-output timeline.csv
`,
	PreRunE: validateSimulateFlags,
	RunE:    runSimulateCommand,
}

func init() {
	// Connection flags
	simulateCmd.Flags().StringVar(&runCfg.ConfigFile, "config", "", "configuration file")

	// Simulation flags
	simulateCmd.Flags().StringVar(&runCfg.Profile, "profile", "oltp", "simulation profile: oltp, olap, mixed, batch")
	simulateCmd.Flags().DurationVar(&runCfg.Duration, "duration", 5*time.Minute, "real-time duration of simulation")
	simulateCmd.Flags().IntVar(&runCfg.TimeScale, "time-scale", 12, "time compression factor 1-24 (12 = 1 min real = 12 min simulated)")
	simulateCmd.Flags().StringVar(&runCfg.StartTime, "start-time", "", "simulation start time: HH:MM or YYYY-MM-DDTHH:MM:SS")
	simulateCmd.Flags().StringVar(&runCfg.Clock, "clock", "simulated", "clock mode: 'real' or 'simulated'")

	// Worker/connection flags
	simulateCmd.Flags().IntVar(&runCfg.Workers, "workers", 4, "number of worker goroutines")
	simulateCmd.Flags().IntVar(&runCfg.Connections, "connections", 10, "number of database connections")
	simulateCmd.Flags().Int64Var(&runCfg.Seed, "seed", 42, "random seed for reproducibility")
	simulateCmd.Flags().IntVar(&runCfg.Scale, "scale", 1, "data scale factor (1 = 10K accounts)")

	// Storage flags
	simulateCmd.Flags().StringVar(&runCfg.MaxStorage, "max-storage", "500MB", "maximum storage for simulation data")
	simulateCmd.Flags().DurationVar(&runCfg.RawRetention, "raw-retention", 10*time.Minute, "rolling window for raw metrics")
	simulateCmd.Flags().DurationVar(&runCfg.AggregateInterval, "aggregate-interval", 1*time.Minute, "aggregation granularity")

	// Output flags
	simulateCmd.Flags().StringVar(&runCfg.Output, "report", "", "output report file (JSON)")
	simulateCmd.Flags().StringVar(&runCfg.TimelineOutput, "timeline", "", "timeline output file (CSV)")
	simulateCmd.Flags().BoolVar(&runCfg.Quiet, "quiet", false, "suppress progress output")

	// Schema flags
	simulateCmd.Flags().BoolVar(&runCfg.SkipSetup, "skip-setup", false, "skip schema creation and data seeding")
	simulateCmd.Flags().BoolVar(&runCfg.SkipCleanup, "skip-cleanup", false, "skip schema cleanup after run")
}

func validateSimulateFlags(cmd *cobra.Command, args []string) error {
	// Set mode to simulation
	runCfg.Mode = "simulation"
	runCfg.Warmup = 0
	runCfg.Cooldown = 0

	// Map --report to --output and --timeline to --timeline-output
	if cmd.Flags().Changed("report") {
		// runCfg.Output is already set by the flag
	}
	if cmd.Flags().Changed("timeline") {
		// runCfg.TimelineOutput is already set by the flag
	}

	// Use the existing simulation validation
	return validateSimulationMode(cmd)
}

func runSimulateCommand(cmd *cobra.Command, args []string) error {
	// Delegate to the common workload runner
	return runWorkload(cmd, args)
}
