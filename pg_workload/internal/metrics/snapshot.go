package metrics

import (
	"encoding/json"
	"time"
)

// Snapshot represents a point-in-time view of collected metrics.
type Snapshot struct {
	StartTime    time.Time                  `json:"start_time"`
	Duration     time.Duration              `json:"duration"`
	TotalQueries int64                      `json:"total_queries"`
	TotalErrors  int64                      `json:"total_errors"`
	QPS          float64                    `json:"qps"`
	Operations   map[string]*OperationStats `json:"operations"`
}

// OperationStats holds metrics for a single operation type.
type OperationStats struct {
	Count      int64            `json:"count"`
	Errors     int64            `json:"errors"`
	QPS        float64          `json:"qps"`
	Latency    LatencyStats     `json:"latency"`
	ErrorTypes map[string]int64 `json:"error_types,omitempty"`
}

// LatencyStats holds latency distribution statistics.
type LatencyStats struct {
	Min    time.Duration `json:"min"`
	Max    time.Duration `json:"max"`
	Mean   time.Duration `json:"mean"`
	StdDev time.Duration `json:"std_dev"`
	P50    time.Duration `json:"p50"`
	P90    time.Duration `json:"p90"`
	P95    time.Duration `json:"p95"`
	P99    time.Duration `json:"p99"`
	P999   time.Duration `json:"p999"`
}

// ToJSON serializes the snapshot to JSON.
func (s *Snapshot) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// ToJSONIndent serializes the snapshot to indented JSON.
func (s *Snapshot) ToJSONIndent() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}

// ErrorRate returns the error rate as a percentage.
func (s *Snapshot) ErrorRate() float64 {
	if s.TotalQueries == 0 {
		return 0
	}
	return float64(s.TotalErrors) / float64(s.TotalQueries) * 100
}

// SuccessRate returns the success rate as a percentage.
func (s *Snapshot) SuccessRate() float64 {
	return 100 - s.ErrorRate()
}

// MarshalJSON customizes JSON output for LatencyStats.
func (l LatencyStats) MarshalJSON() ([]byte, error) {
	type latencyJSON struct {
		Min    string `json:"min"`
		Max    string `json:"max"`
		Mean   string `json:"mean"`
		StdDev string `json:"std_dev"`
		P50    string `json:"p50"`
		P90    string `json:"p90"`
		P95    string `json:"p95"`
		P99    string `json:"p99"`
		P999   string `json:"p999"`
	}

	return json.Marshal(latencyJSON{
		Min:    l.Min.String(),
		Max:    l.Max.String(),
		Mean:   l.Mean.String(),
		StdDev: l.StdDev.String(),
		P50:    l.P50.String(),
		P90:    l.P90.String(),
		P95:    l.P95.String(),
		P99:    l.P99.String(),
		P999:   l.P999.String(),
	})
}

// MarshalJSON customizes JSON output for Snapshot.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	type snapshotJSON struct {
		StartTime    string                     `json:"start_time"`
		Duration     string                     `json:"duration"`
		TotalQueries int64                      `json:"total_queries"`
		TotalErrors  int64                      `json:"total_errors"`
		QPS          float64                    `json:"qps"`
		ErrorRate    float64                    `json:"error_rate_pct"`
		Operations   map[string]*OperationStats `json:"operations"`
	}

	return json.Marshal(snapshotJSON{
		StartTime:    s.StartTime.Format(time.RFC3339),
		Duration:     s.Duration.String(),
		TotalQueries: s.TotalQueries,
		TotalErrors:  s.TotalErrors,
		QPS:          s.QPS,
		ErrorRate:    s.ErrorRate(),
		Operations:   s.Operations,
	})
}
