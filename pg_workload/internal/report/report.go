package report

import (
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
)

// Report contains the complete workload execution report.
type Report struct {
	Version   string                     `json:"version"`
	RunInfo   RunInfo                    `json:"run_info"`
	Summary   Summary                    `json:"summary"`
	Latencies map[string]*LatencyReport  `json:"latencies"`
	Errors    map[string]*ErrorReport    `json:"errors,omitempty"`
	System    *SystemInfo                `json:"system,omitempty"`
}

// RunInfo contains execution metadata.
type RunInfo struct {
	StartTime   time.Time     `json:"start_time"`
	EndTime     time.Time     `json:"end_time"`
	Duration    time.Duration `json:"duration"`
	Mode        string        `json:"mode"`
	Profile     string        `json:"profile"`
	Seed        int64         `json:"seed"`
	Workers     int           `json:"workers"`
	Connections int           `json:"connections"`
}

// Summary contains aggregated metrics.
type Summary struct {
	TotalQueries   int64   `json:"total_queries"`
	TotalErrors    int64   `json:"total_errors"`
	QPS            float64 `json:"qps"`
	ErrorRate      float64 `json:"error_rate_pct"`
	SuccessRate    float64 `json:"success_rate_pct"`
	ReadQueries    int64   `json:"read_queries"`
	WriteQueries   int64   `json:"write_queries"`
	ReadWriteRatio float64 `json:"read_write_ratio"`
}

// LatencyReport contains latency statistics for an operation.
type LatencyReport struct {
	Operation string        `json:"operation"`
	Type      string        `json:"type"`
	Count     int64         `json:"count"`
	QPS       float64       `json:"qps"`
	Min       time.Duration `json:"min"`
	Max       time.Duration `json:"max"`
	Mean      time.Duration `json:"mean"`
	StdDev    time.Duration `json:"std_dev"`
	P50       time.Duration `json:"p50"`
	P90       time.Duration `json:"p90"`
	P95       time.Duration `json:"p95"`
	P99       time.Duration `json:"p99"`
	P999      time.Duration `json:"p999"`
}

// ErrorReport contains error statistics for an operation.
type ErrorReport struct {
	Operation  string           `json:"operation"`
	TotalCount int64            `json:"total_count"`
	ByType     map[string]int64 `json:"by_type"`
}

// SystemInfo contains system/database information.
type SystemInfo struct {
	PostgresVersion string `json:"postgres_version,omitempty"`
	DatabaseName    string `json:"database_name,omitempty"`
	HostInfo        string `json:"host_info,omitempty"`
}

// GenerateReport creates a Report from run info and metrics snapshot.
func GenerateReport(runInfo RunInfo, snapshot *metrics.Snapshot) *Report {
	report := &Report{
		Version:   "1.0",
		RunInfo:   runInfo,
		Latencies: make(map[string]*LatencyReport),
		Errors:    make(map[string]*ErrorReport),
	}

	// Build summary
	report.Summary = buildSummary(snapshot)

	// Build per-operation latency reports
	for opName, opStats := range snapshot.Operations {
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

		// Build error reports if there are errors
		if opStats.Errors > 0 && len(opStats.ErrorTypes) > 0 {
			report.Errors[opName] = &ErrorReport{
				Operation:  opName,
				TotalCount: opStats.Errors,
				ByType:     opStats.ErrorTypes,
			}
		}
	}

	// Remove empty errors map
	if len(report.Errors) == 0 {
		report.Errors = nil
	}

	return report
}

func buildSummary(snapshot *metrics.Snapshot) Summary {
	summary := Summary{
		TotalQueries: snapshot.TotalQueries,
		TotalErrors:  snapshot.TotalErrors,
		QPS:          snapshot.QPS,
		ErrorRate:    snapshot.ErrorRate(),
		SuccessRate:  snapshot.SuccessRate(),
	}

	// Calculate read/write breakdown
	for opName, opStats := range snapshot.Operations {
		if isReadOperation(opName) {
			summary.ReadQueries += opStats.Count
		} else {
			summary.WriteQueries += opStats.Count
		}
	}

	if summary.WriteQueries > 0 {
		summary.ReadWriteRatio = float64(summary.ReadQueries) / float64(summary.WriteQueries)
	}

	return summary
}

func classifyOperation(opName string) string {
	if isReadOperation(opName) {
		return "read"
	}
	return "write"
}

func isReadOperation(opName string) bool {
	readOps := map[string]bool{
		// Point lookups
		"point_select": true,
		// Range selects
		"range_select": true,
		// 2-way JOINs
		"customer_accounts":    true,
		"account_transactions": true,
		"branch_accounts":      true,
		// 3-way+ JOINs
		"customer_tx_summary":  true,
		"branch_tx_summary":    true,
		"customer_audit_trail": true,
		"full_customer_report": true,
		"complex_join":         true,
	}
	return readOps[opName]
}

// WithSystemInfo adds system information to the report.
func (r *Report) WithSystemInfo(info *SystemInfo) *Report {
	r.System = info
	return r
}
