package report

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// jsonReport is the JSON-serializable version of Report.
type jsonReport struct {
	Version   string                    `json:"version"`
	RunInfo   jsonRunInfo               `json:"run_info"`
	Summary   jsonSummary               `json:"summary"`
	Latencies map[string]*jsonLatency   `json:"latencies"`
	Errors    map[string]*ErrorReport   `json:"errors,omitempty"`
	System    *SystemInfo               `json:"system,omitempty"`
}

type jsonRunInfo struct {
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Duration    string `json:"duration"`
	DurationSec float64 `json:"duration_sec"`
	Mode        string `json:"mode"`
	Profile     string `json:"profile"`
	Seed        int64  `json:"seed"`
	Workers     int    `json:"workers"`
	Connections int    `json:"connections"`
}

type jsonSummary struct {
	TotalQueries   int64   `json:"total_queries"`
	TotalErrors    int64   `json:"total_errors"`
	QPS            float64 `json:"qps"`
	ErrorRate      float64 `json:"error_rate_pct"`
	SuccessRate    float64 `json:"success_rate_pct"`
	ReadQueries    int64   `json:"read_queries"`
	WriteQueries   int64   `json:"write_queries"`
	ReadWriteRatio float64 `json:"read_write_ratio"`
}

type jsonLatency struct {
	Operation string  `json:"operation"`
	Type      string  `json:"type"`
	Count     int64   `json:"count"`
	QPS       float64 `json:"qps"`
	// Numeric values in milliseconds for programmatic access
	MinMs    float64 `json:"min_ms"`
	MaxMs    float64 `json:"max_ms"`
	MeanMs   float64 `json:"mean_ms"`
	StdDevMs float64 `json:"std_dev_ms"`
	P50Ms    float64 `json:"p50_ms"`
	P90Ms    float64 `json:"p90_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
	P999Ms   float64 `json:"p999_ms"`
}

// ToJSON serializes the report to JSON.
func (r *Report) ToJSON() ([]byte, error) {
	jr := r.toJSONReport()
	return json.MarshalIndent(jr, "", "  ")
}

// ToJSONCompact serializes the report to compact JSON.
func (r *Report) ToJSONCompact() ([]byte, error) {
	jr := r.toJSONReport()
	return json.Marshal(jr)
}

// WriteToFile writes the report to a file.
func (r *Report) WriteToFile(path string) error {
	data, err := r.ToJSON()
	if err != nil {
		return fmt.Errorf("serializing report: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	return nil
}

// WriteToFileCompact writes the report to a file in compact format.
func (r *Report) WriteToFileCompact(path string) error {
	data, err := r.ToJSONCompact()
	if err != nil {
		return fmt.Errorf("serializing report: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	return nil
}

func (r *Report) toJSONReport() jsonReport {
	jr := jsonReport{
		Version: r.Version,
		RunInfo: jsonRunInfo{
			StartTime:   r.RunInfo.StartTime.Format(time.RFC3339),
			EndTime:     r.RunInfo.EndTime.Format(time.RFC3339),
			Duration:    r.RunInfo.Duration.String(),
			DurationSec: r.RunInfo.Duration.Seconds(),
			Mode:        r.RunInfo.Mode,
			Profile:     r.RunInfo.Profile,
			Seed:        r.RunInfo.Seed,
			Workers:     r.RunInfo.Workers,
			Connections: r.RunInfo.Connections,
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
		Latencies: make(map[string]*jsonLatency),
		Errors:    r.Errors,
		System:    r.System,
	}

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

	return jr
}

// String returns a human-readable summary of the report.
func (r *Report) String() string {
	return fmt.Sprintf(
		"Report: %d queries (%.2f QPS), %d errors (%.2f%%), duration: %s",
		r.Summary.TotalQueries,
		r.Summary.QPS,
		r.Summary.TotalErrors,
		r.Summary.ErrorRate,
		r.RunInfo.Duration,
	)
}
