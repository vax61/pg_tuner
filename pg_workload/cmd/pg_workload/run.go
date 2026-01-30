package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/myorg/pg_tuner/pg_workload/internal/config"
	"github.com/myorg/pg_tuner/pg_workload/internal/database"
	"github.com/myorg/pg_tuner/pg_workload/internal/executor"
	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
	"github.com/myorg/pg_tuner/pg_workload/internal/report"
	"github.com/myorg/pg_tuner/pg_workload/internal/schema"
)

// RunConfig holds all run configuration including mode-specific options.
type RunConfig struct {
	// Common options
	Mode        string
	Profile     string
	Duration    time.Duration
	Warmup      time.Duration
	Cooldown    time.Duration
	Connections int
	Workers     int
	Seed        int64
	ConfigFile  string
	Output      string
	Quiet       bool
	Scale       int
	SkipSetup   bool
	SkipCleanup bool

	// Preload options
	PreloadSize     string
	TargetRAM       string
	PreloadParallel int
	PreloadOnly     bool

	// Simulation mode options
	TimeScale         int
	StartTime         string
	Clock             string
	MaxStorage        string
	RawRetention      time.Duration
	AggregateInterval time.Duration
	TimelineOutput    string
}

// IsSimulation returns true if running in simulation mode.
func (c *RunConfig) IsSimulation() bool {
	return c.Mode == "simulation"
}

// IsBurst returns true if running in burst mode.
func (c *RunConfig) IsBurst() bool {
	return c.Mode == "burst"
}

// GetStartTime parses and returns the start time for simulation mode.
func (c *RunConfig) GetStartTime() (time.Time, error) {
	if c.StartTime == "" {
		return time.Now(), nil
	}

	// Try HH:MM format
	if len(c.StartTime) == 5 && c.StartTime[2] == ':' {
		t, err := time.Parse("15:04", c.StartTime)
		if err == nil {
			now := time.Now()
			return time.Date(now.Year(), now.Month(), now.Day(),
				t.Hour(), t.Minute(), 0, 0, now.Location()), nil
		}
	}

	// Try full ISO format
	t, err := time.Parse("2006-01-02T15:04:05", c.StartTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid start-time format: use HH:MM or YYYY-MM-DDTHH:MM:SS")
	}
	return t, nil
}

// GetPreloadSize returns the preload size in bytes, calculating from TargetRAM if needed.
func (c *RunConfig) GetPreloadSize() (int64, error) {
	// If preload-size is explicitly specified, use it
	if c.PreloadSize != "" {
		return parseStorageSize(c.PreloadSize)
	}

	// If target-ram is specified, calculate preload = RAM * 1.5
	if c.TargetRAM != "" {
		ramSize, err := parseStorageSize(c.TargetRAM)
		if err != nil {
			return 0, err
		}
		return int64(float64(ramSize) * 1.5), nil
	}

	// No preload specified
	return 0, nil
}

// ShouldUsePreload returns true if preloading should be used.
func (c *RunConfig) ShouldUsePreload() bool {
	return c.PreloadSize != "" || c.TargetRAM != ""
}

var runCfg RunConfig

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute a workload profile",
	Long: `Execute a workload profile against PostgreSQL with specified parameters.

Modes:
  burst       - Maximum throughput benchmark (default)
  simulation  - Time-compressed workload simulation with timeline output

Data Loading:
  --scale N         - Use INSERT batches (fast for N <= 10)
  --preload-size    - Use COPY for massive data loading (e.g., 10GB, 50GB)
  --target-ram      - Calculate preload size as RAM * 1.5 to force disk I/O

Examples:
  # Burst mode with default scale
  pg_workload run --duration 5m --workers 4

  # Large scale with COPY preloading
  pg_workload run --preload-size 20GB --duration 10m

  # Force disk I/O on 16GB machine
  pg_workload run --target-ram 16GB --duration 15m

  # Preload only (no workload execution)
  pg_workload run --preload-size 10GB --preload-only

  # Simulation mode with 12x time compression
  pg_workload run --mode simulation --duration 24h --time-scale 12 --timeline-output timeline.csv
`,
	PreRunE: validateRunFlags,
	RunE:    runWorkload,
}

func init() {
	// Common flags
	runCmd.Flags().StringVar(&runCfg.Mode, "mode", "burst", "workload mode: 'burst' or 'simulation'")
	runCmd.Flags().StringVar(&runCfg.Profile, "profile", "oltp_standard", "workload profile name")
	runCmd.Flags().DurationVar(&runCfg.Duration, "duration", 15*time.Minute, "test duration")
	runCmd.Flags().DurationVar(&runCfg.Warmup, "warmup", 2*time.Minute, "warmup duration (burst mode)")
	runCmd.Flags().DurationVar(&runCfg.Cooldown, "cooldown", 1*time.Minute, "cooldown duration (burst mode)")
	runCmd.Flags().IntVar(&runCfg.Connections, "connections", 10, "number of database connections")
	runCmd.Flags().IntVar(&runCfg.Workers, "workers", 4, "number of worker goroutines")
	runCmd.Flags().Int64Var(&runCfg.Seed, "seed", 42, "random seed for reproducibility")
	runCmd.Flags().StringVar(&runCfg.ConfigFile, "config", "", "configuration file")
	runCmd.Flags().StringVar(&runCfg.Output, "output", "", "output file (JSON), defaults to stdout")
	runCmd.Flags().BoolVar(&runCfg.Quiet, "quiet", false, "suppress progress output")
	runCmd.Flags().IntVar(&runCfg.Scale, "scale", 1, "data scale factor (1 = 10K accounts, uses INSERT)")
	runCmd.Flags().BoolVar(&runCfg.SkipSetup, "skip-setup", false, "skip schema creation and data seeding")
	runCmd.Flags().BoolVar(&runCfg.SkipCleanup, "skip-cleanup", false, "skip schema cleanup after run")

	// Preload flags
	runCmd.Flags().StringVar(&runCfg.PreloadSize, "preload-size", "", "target data size for COPY preload (e.g., 10GB, 50GB)")
	runCmd.Flags().StringVar(&runCfg.TargetRAM, "target-ram", "", "machine RAM - calculates preload as RAM * 1.5")
	runCmd.Flags().IntVar(&runCfg.PreloadParallel, "preload-parallel", 4, "parallel goroutines for COPY preload")
	runCmd.Flags().BoolVar(&runCfg.PreloadOnly, "preload-only", false, "only preload data, skip workload execution")

	// Simulation mode flags
	runCmd.Flags().IntVar(&runCfg.TimeScale, "time-scale", 1, "time compression factor 1-24 (simulation mode)")
	runCmd.Flags().StringVar(&runCfg.StartTime, "start-time", "", "simulation start time: HH:MM or YYYY-MM-DDTHH:MM:SS")
	runCmd.Flags().StringVar(&runCfg.Clock, "clock", "", "clock mode: 'real' or 'simulated' (default: simulated for simulation mode)")
	runCmd.Flags().StringVar(&runCfg.MaxStorage, "max-storage", "500MB", "maximum storage for simulation data")
	runCmd.Flags().DurationVar(&runCfg.RawRetention, "raw-retention", 10*time.Minute, "rolling window for raw metrics data")
	runCmd.Flags().DurationVar(&runCfg.AggregateInterval, "aggregate-interval", 1*time.Minute, "aggregation granularity")
	runCmd.Flags().StringVar(&runCfg.TimelineOutput, "timeline-output", "", "CSV file for timeline output (simulation mode)")
}

func validateRunFlags(cmd *cobra.Command, args []string) error {
	// Validate mode
	if runCfg.Mode != "burst" && runCfg.Mode != "simulation" {
		return fmt.Errorf("--mode must be 'burst' or 'simulation'")
	}

	// Common validations
	if runCfg.Connections < 1 {
		return fmt.Errorf("--connections must be >= 1")
	}
	if runCfg.Workers < 1 {
		return fmt.Errorf("--workers must be >= 1")
	}
	if runCfg.Scale < 1 {
		runCfg.Scale = 1
	}

	// Validate preload options
	if runCfg.PreloadSize != "" && runCfg.TargetRAM != "" {
		return fmt.Errorf("--preload-size and --target-ram are mutually exclusive")
	}

	if runCfg.PreloadSize != "" {
		if _, err := parseStorageSize(runCfg.PreloadSize); err != nil {
			return fmt.Errorf("invalid --preload-size: %w", err)
		}
	}

	if runCfg.TargetRAM != "" {
		if _, err := parseStorageSize(runCfg.TargetRAM); err != nil {
			return fmt.Errorf("invalid --target-ram: %w", err)
		}
	}

	if runCfg.PreloadParallel < 1 {
		runCfg.PreloadParallel = 1
	}
	if runCfg.PreloadParallel > 32 {
		return fmt.Errorf("--preload-parallel must be <= 32")
	}

	// If preload-only, skip further mode validation
	if runCfg.PreloadOnly {
		if !runCfg.ShouldUsePreload() {
			return fmt.Errorf("--preload-only requires --preload-size or --target-ram")
		}
		return nil
	}

	// Mode-specific validation
	if runCfg.IsBurst() {
		return validateBurstMode(cmd)
	}
	return validateSimulationMode(cmd)
}

func validateBurstMode(cmd *cobra.Command) error {
	// Duration validation for burst mode
	if runCfg.Duration < 1*time.Minute {
		return fmt.Errorf("--duration must be >= 1m for burst mode")
	}
	if runCfg.Duration > 30*time.Minute {
		logProgress("Warning: burst mode duration > 30m may produce large amounts of data")
	}

	// Warn about ignored simulation flags
	warnIgnoredFlags(cmd, []string{
		"time-scale", "start-time", "clock", "max-storage",
		"raw-retention", "aggregate-interval", "timeline-output",
	})

	return nil
}

func validateSimulationMode(cmd *cobra.Command) error {
	// Duration validation for simulation mode (more permissive)
	if runCfg.Duration < 1*time.Minute {
		return fmt.Errorf("--duration must be >= 1m")
	}

	// Time scale validation
	if runCfg.TimeScale < 1 || runCfg.TimeScale > 24 {
		return fmt.Errorf("--time-scale must be between 1 and 24")
	}

	// Validate start-time if provided
	if runCfg.StartTime != "" {
		if _, err := runCfg.GetStartTime(); err != nil {
			return err
		}
	}

	// Validate clock
	if runCfg.Clock == "" {
		runCfg.Clock = "simulated" // Default for simulation mode
	}
	if runCfg.Clock != "real" && runCfg.Clock != "simulated" {
		return fmt.Errorf("--clock must be 'real' or 'simulated'")
	}

	// Validate max-storage format
	if !isValidStorageSize(runCfg.MaxStorage) {
		return fmt.Errorf("--max-storage must be a valid size (e.g., 500MB, 1GB)")
	}

	// Validate aggregate-interval
	if runCfg.AggregateInterval < 1*time.Second {
		return fmt.Errorf("--aggregate-interval must be >= 1s")
	}

	// Default warmup/cooldown to 0 for simulation mode if not explicitly set
	if !cmd.Flags().Changed("warmup") {
		runCfg.Warmup = 0
	}
	if !cmd.Flags().Changed("cooldown") {
		runCfg.Cooldown = 0
	}

	return nil
}

func warnIgnoredFlags(cmd *cobra.Command, flags []string) {
	var ignored []string
	for _, flag := range flags {
		if cmd.Flags().Changed(flag) {
			ignored = append(ignored, "--"+flag)
		}
	}
	if len(ignored) > 0 {
		logProgress("Warning: flags ignored in burst mode: %s", strings.Join(ignored, ", "))
	}
}

func isValidStorageSize(s string) bool {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) < 2 {
		return false
	}

	// Check suffix
	validSuffixes := []string{"B", "KB", "MB", "GB", "TB"}
	hasSuffix := false
	for _, suffix := range validSuffixes {
		if strings.HasSuffix(s, suffix) {
			hasSuffix = true
			break
		}
	}
	if !hasSuffix {
		return false
	}

	// Check numeric part
	numPart := strings.TrimRight(s, "KMBGTP")
	for _, c := range numPart {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(numPart) > 0
}

func runWorkload(cmd *cobra.Command, args []string) error {
	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logProgress("Received signal %v, shutting down gracefully...", sig)
		cancel()
	}()

	// Load configuration
	cfg := loadConfig()

	// Override config with flags
	applyFlagsToConfig(cfg)

	logProgress("pg_workload %s - PostgreSQL Workload Generator", Version)
	logProgress("=========================================")

	// Create database pool
	logProgress("Connecting to database %s@%s:%d/%s...",
		cfg.Database.User, cfg.Database.Host, cfg.Database.Port, cfg.Database.DBName)

	poolCfg := database.PoolConfig{
		MinConns:          int32(runCfg.Connections / 2),
		MaxConns:          int32(runCfg.Connections),
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}

	pool, err := database.NewPoolWithConfig(ctx, &cfg.Database, poolCfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	// Health check
	if err := pool.HealthCheck(ctx); err != nil {
		return fmt.Errorf("database health check failed: %w", err)
	}
	logProgress("Database connection established")

	// Setup schema if needed
	if !runCfg.SkipSetup {
		logProgress("Setting up OLTP schema...")
		if err := schema.CreateOLTPSchema(ctx, pool.Pool()); err != nil {
			return fmt.Errorf("creating schema: %w", err)
		}

		// Determine data loading strategy
		if runCfg.ShouldUsePreload() {
			// Use COPY-based preloading
			preloadSize, err := runCfg.GetPreloadSize()
			if err != nil {
				return fmt.Errorf("calculating preload size: %w", err)
			}

			logProgress("Preloading data with COPY (target: %s, parallel: %d)...",
				schema.FormatBytes(preloadSize), runCfg.PreloadParallel)

			preloadCfg := schema.PreloadConfig{
				TargetSize: preloadSize,
				Parallel:   runCfg.PreloadParallel,
				Seed:       runCfg.Seed,
				ProgressCallback: func(table string, loaded, total int64) {
					if total > 0 {
						pct := float64(loaded) / float64(total) * 100
						logProgress("  %s: %.1f%% (%d/%d)", table, pct, loaded, total)
					}
				},
			}

			pm := schema.NewPreloadManager(pool.Pool(), preloadCfg)
			if err := pm.Preload(ctx); err != nil {
				return fmt.Errorf("preloading data: %w", err)
			}

			stats := pm.Stats()
			logProgress("Preload complete in %s:", stats.Duration.Round(time.Second))
			logProgress("  Customers:    %d", stats.Customers)
			logProgress("  Branches:     %d", stats.Branches)
			logProgress("  Accounts:     %d", stats.Accounts)
			logProgress("  Transactions: %d", stats.Transactions)

			// Get actual data size
			actualSize, err := schema.GetTotalDataSize(ctx, pool.Pool())
			if err == nil {
				logProgress("  Total size:   %s", schema.FormatBytes(actualSize))
			}
		} else {
			// Use INSERT-based seeding for small scale
			logProgress("Seeding test data (scale=%d)...", runCfg.Scale)
			if err := schema.SeedOLTPData(ctx, pool.Pool(), runCfg.Seed, runCfg.Scale); err != nil {
				return fmt.Errorf("seeding data: %w", err)
			}
		}

		logProgress("Schema setup complete")
	}

	// If preload-only, exit here
	if runCfg.PreloadOnly {
		logProgress("")
		logProgress("Preload-only mode: skipping workload execution")
		logProgress("Data is ready for workload testing")
		return nil
	}

	// Cleanup on exit if not skipped
	if !runCfg.SkipCleanup && !runCfg.SkipSetup {
		defer func() {
			logProgress("Cleaning up schema...")
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			schema.DropOLTPSchema(cleanupCtx, pool.Pool())
		}()
	}

	// Get entity counts for workers
	tableStats, err := schema.GetTableStats(ctx, pool.Pool())
	if err != nil {
		return fmt.Errorf("getting table stats: %w", err)
	}

	// Branch based on mode
	if runCfg.IsSimulation() {
		return runSimulation(ctx, cfg, pool, tableStats)
	}

	return runBurst(ctx, cfg, pool, tableStats)
}

func runBurst(ctx context.Context, cfg *config.Config, pool *database.Pool, tableStats *schema.TableStats) error {
	// Create metrics collector
	collector := metrics.NewCollector()

	// Create executor config with accurate entity counts
	maxAccounts := int(tableStats.Accounts)
	if maxAccounts < 1 {
		maxAccounts = 10000 * runCfg.Scale
	}

	execCfg := executor.Config{
		Duration:     runCfg.Duration,
		Warmup:       runCfg.Warmup,
		Cooldown:     runCfg.Cooldown,
		Workers:      runCfg.Workers,
		Seed:         runCfg.Seed,
		Queries:      profile.OLTPQueries,
		MaxAccounts:  maxAccounts,
		MaxCustomers: int(tableStats.Customers),
		MaxBranches:  int(tableStats.Branches),
	}

	// Create executor
	exec := executor.NewExecutor(pool, collector, execCfg)

	// Run info for report
	startTime := time.Now()

	// Log execution plan
	logProgress("")
	logExecutionPlan(tableStats)
	logProgress("")

	// Start progress reporter with separate context
	progressCtx, stopProgress := context.WithCancel(ctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		reportBurstProgress(progressCtx, exec, collector)
	}()

	// Run workload
	logProgress("Starting workload execution...")
	execErr := exec.Run(ctx)

	// Stop progress reporter immediately after executor finishes
	stopProgress()
	<-progressDone

	if execErr != nil && execErr != context.Canceled {
		return fmt.Errorf("execution failed: %w", execErr)
	}

	endTime := time.Now()

	// Generate report
	logProgress("")
	logProgress("Generating report...")

	runInfo := report.RunInfo{
		StartTime:   startTime,
		EndTime:     endTime,
		Duration:    endTime.Sub(startTime),
		Mode:        runCfg.Mode,
		Profile:     runCfg.Profile,
		Seed:        runCfg.Seed,
		Workers:     runCfg.Workers,
		Connections: runCfg.Connections,
	}

	snapshot := collector.GetSnapshot()
	rpt := report.GenerateReport(runInfo, snapshot)

	// Add system info
	rpt.WithSystemInfo(&report.SystemInfo{
		DatabaseName: cfg.Database.DBName,
		HostInfo:     fmt.Sprintf("%s:%d", cfg.Database.Host, cfg.Database.Port),
	})

	// Output report to file if specified
	if runCfg.Output != "" {
		if err := rpt.WriteToFile(runCfg.Output); err != nil {
			return fmt.Errorf("writing report: %w", err)
		}
		logProgress("Report written to %s", runCfg.Output)
	}

	// Print console summary (unless quiet mode or JSON to stdout)
	if !runCfg.Quiet {
		if runCfg.Output != "" {
			// Print formatted console summary
			fmt.Fprintln(os.Stderr, "")
			formatter := report.NewConsoleFormatter().WithReportPath(runCfg.Output)
			formatter.PrintSummary(rpt)
		} else {
			// No output file specified, output JSON to stdout
			jsonData, err := rpt.ToJSON()
			if err != nil {
				return fmt.Errorf("serializing report: %w", err)
			}
			fmt.Println(string(jsonData))
		}
	} else if runCfg.Output == "" {
		// Quiet mode but no output file - still output JSON
		jsonData, err := rpt.ToJSON()
		if err != nil {
			return fmt.Errorf("serializing report: %w", err)
		}
		fmt.Println(string(jsonData))
	}

	logProgress("")
	logProgress("Done.")

	return nil
}

func runSimulation(ctx context.Context, cfg *config.Config, pool *database.Pool, tableStats *schema.TableStats) error {
	// Parse start time
	startTime, err := runCfg.GetStartTime()
	if err != nil {
		return fmt.Errorf("parsing start time: %w", err)
	}

	// Parse max storage
	maxStorage, err := parseStorageSize(runCfg.MaxStorage)
	if err != nil {
		return fmt.Errorf("parsing max storage: %w", err)
	}

	// Load simulation profile
	simProfile, err := profile.LoadSimulationProfile(runCfg.Profile)
	if err != nil {
		// Fall back to default profile
		simProfile = profile.NewSimulationProfile()
		simProfile.SetDefaults()
		logProgress("Using default simulation profile")
	}

	// Calculate accurate entity counts
	maxAccounts := int(tableStats.Accounts)
	if maxAccounts < 1 {
		maxAccounts = 10000 * runCfg.Scale
	}

	// Create simulation config
	simCfg := &executor.SimulationConfig{
		Profile:           simProfile,
		Duration:          runCfg.Duration,
		TimeScale:         runCfg.TimeScale,
		StartTime:         startTime,
		MaxStorage:        maxStorage,
		RawRetention:      runCfg.RawRetention,
		AggregateInterval: runCfg.AggregateInterval,
		TimelineOutput:    runCfg.TimelineOutput,
		Workers:           runCfg.Workers,
		Seed:              runCfg.Seed,
		MaxAccounts:       maxAccounts,
		MaxCustomers:      int(tableStats.Customers),
		MaxBranches:       int(tableStats.Branches),
	}

	// Create simulation executor
	simExec, err := executor.NewSimulationExecutor(simCfg, pool)
	if err != nil {
		return fmt.Errorf("creating simulation executor: %w", err)
	}

	// Initialize components
	if err := simExec.Initialize(ctx); err != nil {
		return fmt.Errorf("initializing simulation: %w", err)
	}

	// Log execution plan
	logProgress("")
	logExecutionPlan(tableStats)
	logProgress("")

	// Start progress reporter
	progressCtx, stopProgress := context.WithCancel(ctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		reportSimulationProgress(progressCtx, simExec)
	}()

	// Run simulation
	logProgress("Starting simulation...")
	realStartTime := time.Now()
	execErr := simExec.Run(ctx)

	// Stop progress reporter
	stopProgress()
	<-progressDone

	// Graceful shutdown
	if err := simExec.Stop(); err != nil {
		logProgress("Warning: error during shutdown: %v", err)
	}

	if execErr != nil && execErr != context.Canceled {
		return fmt.Errorf("simulation failed: %w", execErr)
	}

	realEndTime := time.Now()

	// Get final status
	status := simExec.GetStatus()

	// Generate simulation report
	logProgress("")
	logProgress("Generating simulation report...")

	runInfo := report.RunInfo{
		StartTime:   realStartTime,
		EndTime:     realEndTime,
		Duration:    realEndTime.Sub(realStartTime),
		Mode:        runCfg.Mode,
		Profile:     runCfg.Profile,
		Seed:        runCfg.Seed,
		Workers:     runCfg.Workers,
		Connections: runCfg.Connections,
	}

	simInfo := report.SimulationInfo{
		TimeScale:         runCfg.TimeScale,
		StartSimTime:      startTime,
		EndSimTime:        status.SimulatedTime,
		SimulatedDuration: status.ElapsedSimulated,
		RealDuration:      status.ElapsedReal,
		ProfileUsed:       runCfg.Profile,
		ClockMode:         runCfg.Clock,
	}

	// Convert events triggered
	eventsTriggered := make([]report.EventRecord, 0, len(simExec.GetEventsTriggered()))
	for _, e := range simExec.GetEventsTriggered() {
		eventsTriggered = append(eventsTriggered, report.EventRecord{
			Name:      e.Name,
			StartTime: e.StartTime,
			EndTime:   e.EndTime,
			Triggered: e.Triggered,
		})
	}

	reportCfg := report.SimulationReportConfig{
		RunInfo:         runInfo,
		SimInfo:         simInfo,
		Snapshot:        simExec.GetMetrics().GetSnapshot(),
		TimelineSummary: simExec.GetTimelineSummary(),
		EventsTriggered: eventsTriggered,
		StorageUsed:     simExec.GetStorageUsed(),
		System: &report.SystemInfo{
			DatabaseName: cfg.Database.DBName,
			HostInfo:     fmt.Sprintf("%s:%d", cfg.Database.Host, cfg.Database.Port),
		},
	}

	simReport := report.GenerateSimulationReport(reportCfg)

	// Output report to file if specified
	if runCfg.Output != "" {
		if err := simReport.WriteToFile(runCfg.Output); err != nil {
			return fmt.Errorf("writing report: %w", err)
		}
		logProgress("Report written to %s", runCfg.Output)
	}

	// Print console summary (unless quiet mode or JSON to stdout)
	if !runCfg.Quiet {
		if runCfg.Output != "" {
			// Print formatted console summary
			fmt.Fprintln(os.Stderr, "")
			formatter := report.NewConsoleFormatter().WithReportPath(runCfg.Output)
			formatter.PrintSimulationSummary(simReport)
		} else {
			// No output file specified, output JSON to stdout
			jsonData, err := simReport.ToJSON()
			if err != nil {
				return fmt.Errorf("serializing report: %w", err)
			}
			fmt.Println(string(jsonData))
		}
	} else if runCfg.Output == "" {
		// Quiet mode but no output file - still output JSON
		jsonData, err := simReport.ToJSON()
		if err != nil {
			return fmt.Errorf("serializing report: %w", err)
		}
		fmt.Println(string(jsonData))
	}

	logProgress("")
	logProgress("Done.")

	return nil
}

func reportSimulationProgress(ctx context.Context, simExec *executor.SimulationExecutor) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status := simExec.GetStatus()
			activeEvents := ""
			if len(status.ActiveEvents) > 0 {
				activeEvents = fmt.Sprintf(" | Events: %s", strings.Join(status.ActiveEvents, ", "))
			}
			logProgress("[%s] SimTime: %s | Real: %s | Queries: %d | QPS: %.1f | Workers: %d | Storage: %.1f%%%s",
				status.Phase,
				status.SimulatedTime.Format("15:04:05"),
				status.ElapsedReal.Round(time.Second),
				status.TotalQueries,
				status.ActualQPS,
				status.ActiveWorkers,
				status.StorageUsedPct,
				activeEvents,
			)
		}
	}
}

func parseStorageSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid storage size: %s", s)
	}

	var multiplier int64 = 1
	var numPart string

	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1 << 40
		numPart = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1 << 30
		numPart = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1 << 20
		numPart = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1 << 10
		numPart = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		multiplier = 1
		numPart = strings.TrimSuffix(s, "B")
	default:
		return 0, fmt.Errorf("invalid storage size suffix: %s", s)
	}

	val, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid storage size number: %s", numPart)
	}

	return val * multiplier, nil
}

func logExecutionPlan(tableStats *schema.TableStats) {
	logProgress("Execution plan:")
	logProgress("  Mode:        %s", runCfg.Mode)
	logProgress("  Profile:     %s", runCfg.Profile)
	logProgress("  Duration:    %s", runCfg.Duration)

	if runCfg.IsBurst() {
		logProgress("  Warmup:      %s", runCfg.Warmup)
		logProgress("  Cooldown:    %s", runCfg.Cooldown)
	}

	logProgress("  Workers:     %d", runCfg.Workers)
	logProgress("  Connections: %d", runCfg.Connections)
	logProgress("  Seed:        %d", runCfg.Seed)

	// Show data stats
	if tableStats != nil {
		logProgress("")
		logProgress("Data statistics:")
		logProgress("  Customers:    %d", tableStats.Customers)
		logProgress("  Branches:     %d", tableStats.Branches)
		logProgress("  Accounts:     %d", tableStats.Accounts)
		logProgress("  Transactions: %d", tableStats.Transactions)
	}

	if runCfg.IsSimulation() {
		logProgress("")
		logProgress("Simulation settings:")
		logProgress("  Time-scale:  %dx", runCfg.TimeScale)
		logProgress("  Clock:       %s", runCfg.Clock)
		if runCfg.StartTime != "" {
			logProgress("  Start-time:  %s", runCfg.StartTime)
		}
		logProgress("  Max-storage: %s", runCfg.MaxStorage)
		logProgress("  Raw-retention: %s", runCfg.RawRetention)
		logProgress("  Aggregate-interval: %s", runCfg.AggregateInterval)
		if runCfg.TimelineOutput != "" {
			logProgress("  Timeline:    %s", runCfg.TimelineOutput)
		}
	}
}

func loadConfig() *config.Config {
	if runCfg.ConfigFile != "" {
		cfg, err := config.LoadConfig(runCfg.ConfigFile)
		if err != nil {
			logProgress("Warning: failed to load config file: %v", err)
			return config.LoadConfigWithDefaults()
		}
		return cfg
	}
	return config.LoadConfigWithDefaults()
}

func applyFlagsToConfig(cfg *config.Config) {
	cfg.Workload.Mode = runCfg.Mode
	cfg.Workload.Profile = runCfg.Profile
	cfg.Workload.Duration = runCfg.Duration
	cfg.Workload.Warmup = runCfg.Warmup
	cfg.Workload.Cooldown = runCfg.Cooldown
	cfg.Workload.Connections = runCfg.Connections
	cfg.Workload.Workers = runCfg.Workers
	cfg.Workload.Seed = runCfg.Seed
	cfg.Output.File = runCfg.Output
}

func reportBurstProgress(ctx context.Context, exec *executor.Executor, collector *metrics.Collector) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			phase := exec.Phase()
			snap := collector.GetSnapshot()
			logProgress("[%s] Queries: %d | QPS: %.1f | Errors: %d",
				phase, snap.TotalQueries, snap.QPS, snap.TotalErrors)
		}
	}
}

func logProgress(format string, args ...interface{}) {
	if runCfg.Quiet {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}
