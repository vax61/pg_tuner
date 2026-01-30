package report

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
	"github.com/myorg/pg_tuner/pg_workload/internal/timeline"
)

// SimulationReport extends Report with simulation-specific data.
type SimulationReport struct {
	Version          string                    `json:"version"`
	RunInfo          RunInfo                   `json:"run_info"`
	SimulationInfo   SimulationInfo            `json:"simulation_info"`
	Summary          Summary                   `json:"summary"`
	Latencies        map[string]*LatencyReport `json:"latencies"`
	Errors           map[string]*ErrorReport   `json:"errors,omitempty"`
	TimelineSummary  *TimelineSummaryReport    `json:"timeline_summary,omitempty"`
	EventsTriggered  []EventRecord             `json:"events_triggered,omitempty"`
	StorageUsed      int64                     `json:"storage_used_bytes"`
	System           *SystemInfo               `json:"system,omitempty"`
}

// SimulationInfo contains simulation-specific metadata.
type SimulationInfo struct {
	TimeScale         int           `json:"time_scale"`
	StartSimTime      time.Time     `json:"start_sim_time"`
	EndSimTime        time.Time     `json:"end_sim_time"`
	SimulatedDuration time.Duration `json:"simulated_duration"`
	RealDuration      time.Duration `json:"real_duration"`
	ProfileUsed       string        `json:"profile_used"`
	ClockMode         string        `json:"clock_mode"`
}

// EventRecord represents a triggered event.
type EventRecord struct {
	Name      string    `json:"name"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Triggered bool      `json:"triggered"`
}

// TimelineSummaryReport is a serializable version of timeline summary.
type TimelineSummaryReport struct {
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
	TotalSeconds      float64   `json:"total_seconds"`
	Intervals         int       `json:"intervals"`
	TotalQueries      int64     `json:"total_queries"`
	TotalErrors       int64     `json:"total_errors"`
	TotalReadQueries  int64     `json:"total_read_queries"`
	TotalWriteQueries int64     `json:"total_write_queries"`
	ErrorRate         float64   `json:"error_rate_pct"`
	AvgQPS            float64   `json:"avg_qps"`
	MinQPS            float64   `json:"min_qps"`
	MaxQPS            float64   `json:"max_qps"`
	AvgLatencyUs      int64     `json:"avg_latency_us"`
	P50LatencyUs      int64     `json:"p50_latency_us"`
	P95LatencyUs      int64     `json:"p95_latency_us"`
	P99LatencyUs      int64     `json:"p99_latency_us"`
	AvgWorkers        int       `json:"avg_workers"`
	MinWorkers        int       `json:"min_workers"`
	MaxWorkers        int       `json:"max_workers"`
	TargetHitRate     float64   `json:"target_hit_rate_pct"`
}

// SimulationReportConfig contains data needed to generate a simulation report.
type SimulationReportConfig struct {
	RunInfo           RunInfo
	SimInfo           SimulationInfo
	Snapshot          *metrics.Snapshot
	TimelineSummary   *timeline.TimelineSummary
	EventsTriggered   []EventRecord
	StorageUsed       int64
	System            *SystemInfo
}

// GenerateSimulationReport creates a complete simulation report.
func GenerateSimulationReport(cfg SimulationReportConfig) *SimulationReport {
	report := &SimulationReport{
		Version:        "1.0",
		RunInfo:        cfg.RunInfo,
		SimulationInfo: cfg.SimInfo,
		Latencies:      make(map[string]*LatencyReport),
		Errors:         make(map[string]*ErrorReport),
		EventsTriggered: cfg.EventsTriggered,
		StorageUsed:    cfg.StorageUsed,
		System:         cfg.System,
	}

	// Process metrics snapshot
	if cfg.Snapshot != nil {
		report.Summary = Summary{
			TotalQueries: cfg.Snapshot.TotalQueries,
			TotalErrors:  cfg.Snapshot.TotalErrors,
			QPS:          cfg.Snapshot.QPS,
			ErrorRate:    cfg.Snapshot.ErrorRate(),
			SuccessRate:  cfg.Snapshot.SuccessRate(),
		}

		// Calculate read/write breakdown
		for opName, opStats := range cfg.Snapshot.Operations {
			if classifyOperation(opName) == "read" {
				report.Summary.ReadQueries += opStats.Count
			} else {
				report.Summary.WriteQueries += opStats.Count
			}

			// Latency report
			report.Latencies[opName] = &LatencyReport{
				Operation: opName,
				Type:      classifyOperation(opName),
				Count:     opStats.Count,
				QPS:       opStats.QPS,
				Min:       opStats.Latency.Min,
				Max:       opStats.Latency.Max,
				Mean:      opStats.Latency.Mean,
				StdDev:    opStats.Latency.StdDev,
				P50:       opStats.Latency.P50,
				P90:       opStats.Latency.P90,
				P95:       opStats.Latency.P95,
				P99:       opStats.Latency.P99,
				P999:      opStats.Latency.P999,
			}

			// Error report if there are errors
			if opStats.Errors > 0 && len(opStats.ErrorTypes) > 0 {
				errReport := &ErrorReport{
					Operation:  opName,
					TotalCount: opStats.Errors,
					ByType:     make(map[string]int64),
				}
				for errType, count := range opStats.ErrorTypes {
					errReport.ByType[errType] = count
				}
				report.Errors[opName] = errReport
			}
		}

		// Calculate read/write ratio
		if report.Summary.WriteQueries > 0 {
			report.Summary.ReadWriteRatio = float64(report.Summary.ReadQueries) / float64(report.Summary.WriteQueries)
		}
	}

	// Process timeline summary
	if cfg.TimelineSummary != nil {
		report.TimelineSummary = &TimelineSummaryReport{
			StartTime:         cfg.TimelineSummary.StartTime,
			EndTime:           cfg.TimelineSummary.EndTime,
			TotalSeconds:      cfg.TimelineSummary.TotalSeconds,
			Intervals:         cfg.TimelineSummary.Intervals,
			TotalQueries:      cfg.TimelineSummary.TotalQueries,
			TotalErrors:       cfg.TimelineSummary.TotalErrors,
			TotalReadQueries:  cfg.TimelineSummary.TotalReadQueries,
			TotalWriteQueries: cfg.TimelineSummary.TotalWriteQueries,
			ErrorRate:         cfg.TimelineSummary.ErrorRate,
			AvgQPS:            cfg.TimelineSummary.AvgQPS,
			MinQPS:            cfg.TimelineSummary.MinQPS,
			MaxQPS:            cfg.TimelineSummary.MaxQPS,
			AvgLatencyUs:      cfg.TimelineSummary.AvgLatencyUs,
			P50LatencyUs:      cfg.TimelineSummary.P50LatencyUs,
			P95LatencyUs:      cfg.TimelineSummary.P95LatencyUs,
			P99LatencyUs:      cfg.TimelineSummary.P99LatencyUs,
			AvgWorkers:        cfg.TimelineSummary.AvgWorkers,
			MinWorkers:        cfg.TimelineSummary.MinWorkers,
			MaxWorkers:        cfg.TimelineSummary.MaxWorkers,
			TargetHitRate:     cfg.TimelineSummary.TargetHitRate,
		}
	}

	return report
}

// jsonSimulationReport is the JSON-serializable version of SimulationReport.
type jsonSimulationReport struct {
	Version          string                    `json:"version"`
	RunInfo          jsonRunInfo               `json:"run_info"`
	SimulationInfo   jsonSimulationInfo        `json:"simulation_info"`
	Summary          jsonSummary               `json:"summary"`
	Latencies        map[string]*jsonLatency   `json:"latencies"`
	Errors           map[string]*ErrorReport   `json:"errors,omitempty"`
	TimelineSummary  *jsonTimelineSummary      `json:"timeline_summary,omitempty"`
	EventsTriggered  []jsonEventRecord         `json:"events_triggered,omitempty"`
	StorageUsedBytes int64                     `json:"storage_used_bytes"`
	System           *SystemInfo               `json:"system,omitempty"`
}

type jsonSimulationInfo struct {
	TimeScale            int     `json:"time_scale"`
	StartSimTime         string  `json:"start_sim_time"`
	EndSimTime           string  `json:"end_sim_time"`
	SimulatedDurationMs  float64 `json:"simulated_duration_ms"`
	RealDurationMs       float64 `json:"real_duration_ms"`
	ProfileUsed          string  `json:"profile_used"`
	ClockMode            string  `json:"clock_mode"`
}

type jsonTimelineSummary struct {
	StartTime         string  `json:"start_time"`
	EndTime           string  `json:"end_time"`
	TotalSeconds      float64 `json:"total_seconds"`
	Intervals         int     `json:"intervals"`
	TotalQueries      int64   `json:"total_queries"`
	TotalErrors       int64   `json:"total_errors"`
	TotalReadQueries  int64   `json:"total_read_queries"`
	TotalWriteQueries int64   `json:"total_write_queries"`
	ErrorRate         float64 `json:"error_rate_pct"`
	AvgQPS            float64 `json:"avg_qps"`
	MinQPS            float64 `json:"min_qps"`
	MaxQPS            float64 `json:"max_qps"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	AvgWorkers        int     `json:"avg_workers"`
	MinWorkers        int     `json:"min_workers"`
	MaxWorkers        int     `json:"max_workers"`
	TargetHitRate     float64 `json:"target_hit_rate_pct"`
}

type jsonEventRecord struct {
	Name      string `json:"name"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Triggered bool   `json:"triggered"`
}

// ToJSON serializes the report to JSON with readable ms values.
func (r *SimulationReport) ToJSON() ([]byte, error) {
	jr := r.toJSONReport()
	return json.MarshalIndent(jr, "", "  ")
}

func (r *SimulationReport) toJSONReport() jsonSimulationReport {
	jr := jsonSimulationReport{
		Version: r.Version,
		RunInfo: jsonRunInfo{
			StartTime:   r.RunInfo.StartTime.Format(time.RFC3339),
			EndTime:     r.RunInfo.EndTime.Format(time.RFC3339),
			Duration:    r.RunInfo.Duration.Round(time.Millisecond).String(),
			DurationSec: r.RunInfo.Duration.Seconds(),
			Mode:        r.RunInfo.Mode,
			Profile:     r.RunInfo.Profile,
			Seed:        r.RunInfo.Seed,
			Workers:     r.RunInfo.Workers,
			Connections: r.RunInfo.Connections,
		},
		SimulationInfo: jsonSimulationInfo{
			TimeScale:            r.SimulationInfo.TimeScale,
			StartSimTime:         r.SimulationInfo.StartSimTime.Format(time.RFC3339),
			EndSimTime:           r.SimulationInfo.EndSimTime.Format(time.RFC3339),
			SimulatedDurationMs:  float64(r.SimulationInfo.SimulatedDuration.Milliseconds()),
			RealDurationMs:       float64(r.SimulationInfo.RealDuration.Milliseconds()),
			ProfileUsed:          r.SimulationInfo.ProfileUsed,
			ClockMode:            r.SimulationInfo.ClockMode,
		},
		Summary: jsonSummary{
			TotalQueries:   r.Summary.TotalQueries,
			TotalErrors:    r.Summary.TotalErrors,
			QPS:            r.Summary.QPS,
			ErrorRate:      r.Summary.ErrorRate,
			SuccessRate:    r.Summary.SuccessRate,
			ReadQueries:    r.Summary.ReadQueries,
			WriteQueries:   r.Summary.WriteQueries,
			ReadWriteRatio: r.Summary.ReadWriteRatio,
		},
		Latencies:        make(map[string]*jsonLatency),
		Errors:           r.Errors,
		StorageUsedBytes: r.StorageUsed,
		System:           r.System,
	}

	// Convert latencies to ms
	for name, lat := range r.Latencies {
		jr.Latencies[name] = &jsonLatency{
			Operation: lat.Operation,
			Type:      lat.Type,
			Count:     lat.Count,
			QPS:       lat.QPS,
			MinMs:     float64(lat.Min.Microseconds()) / 1000.0,
			MaxMs:     float64(lat.Max.Microseconds()) / 1000.0,
			MeanMs:    float64(lat.Mean.Microseconds()) / 1000.0,
			StdDevMs:  float64(lat.StdDev.Microseconds()) / 1000.0,
			P50Ms:     float64(lat.P50.Microseconds()) / 1000.0,
			P90Ms:     float64(lat.P90.Microseconds()) / 1000.0,
			P95Ms:     float64(lat.P95.Microseconds()) / 1000.0,
			P99Ms:     float64(lat.P99.Microseconds()) / 1000.0,
			P999Ms:    float64(lat.P999.Microseconds()) / 1000.0,
		}
	}

	// Convert timeline summary
	if r.TimelineSummary != nil {
		jr.TimelineSummary = &jsonTimelineSummary{
			StartTime:         r.TimelineSummary.StartTime.Format(time.RFC3339),
			EndTime:           r.TimelineSummary.EndTime.Format(time.RFC3339),
			TotalSeconds:      r.TimelineSummary.TotalSeconds,
			Intervals:         r.TimelineSummary.Intervals,
			TotalQueries:      r.TimelineSummary.TotalQueries,
			TotalErrors:       r.TimelineSummary.TotalErrors,
			TotalReadQueries:  r.TimelineSummary.TotalReadQueries,
			TotalWriteQueries: r.TimelineSummary.TotalWriteQueries,
			ErrorRate:         r.TimelineSummary.ErrorRate,
			AvgQPS:            r.TimelineSummary.AvgQPS,
			MinQPS:            r.TimelineSummary.MinQPS,
			MaxQPS:            r.TimelineSummary.MaxQPS,
			AvgLatencyMs:      float64(r.TimelineSummary.AvgLatencyUs) / 1000.0,
			P50LatencyMs:      float64(r.TimelineSummary.P50LatencyUs) / 1000.0,
			P95LatencyMs:      float64(r.TimelineSummary.P95LatencyUs) / 1000.0,
			P99LatencyMs:      float64(r.TimelineSummary.P99LatencyUs) / 1000.0,
			AvgWorkers:        r.TimelineSummary.AvgWorkers,
			MinWorkers:        r.TimelineSummary.MinWorkers,
			MaxWorkers:        r.TimelineSummary.MaxWorkers,
			TargetHitRate:     r.TimelineSummary.TargetHitRate,
		}
	}

	// Convert events
	jr.EventsTriggered = make([]jsonEventRecord, len(r.EventsTriggered))
	for i, e := range r.EventsTriggered {
		jr.EventsTriggered[i] = jsonEventRecord{
			Name:      e.Name,
			StartTime: e.StartTime.Format(time.RFC3339),
			EndTime:   e.EndTime.Format(time.RFC3339),
			Triggered: e.Triggered,
		}
	}

	return jr
}

// WriteToFile writes the report to a file.
func (r *SimulationReport) WriteToFile(path string) error {
	data, err := r.ToJSON()
	if err != nil {
		return fmt.Errorf("marshaling report: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	return nil
}

// String returns a human-readable summary.
func (r *SimulationReport) String() string {
	return fmt.Sprintf(
		"Simulation: %s | Duration: %s (real) / %s (sim) | Queries: %d | QPS: %.1f | Errors: %d (%.2f%%) | Events: %d",
		r.SimulationInfo.ProfileUsed,
		r.SimulationInfo.RealDuration.Round(time.Second),
		r.SimulationInfo.SimulatedDuration.Round(time.Second),
		r.Summary.TotalQueries,
		r.Summary.QPS,
		r.Summary.TotalErrors,
		r.Summary.ErrorRate,
		len(r.EventsTriggered),
	)
}

// WithSystemInfo adds system information to the report.
func (r *SimulationReport) WithSystemInfo(info *SystemInfo) *SimulationReport {
	r.System = info
	return r
}
