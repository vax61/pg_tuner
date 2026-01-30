package timeline

import (
	"time"
)

// TimelineSummary contains aggregated statistics for a timeline.
type TimelineSummary struct {
	// Time range
	StartTime    time.Time     `json:"start_time"`
	EndTime      time.Time     `json:"end_time"`
	TotalSeconds float64       `json:"total_seconds"`
	Intervals    int           `json:"intervals"`
	Interval     time.Duration `json:"interval"`

	// Query statistics
	TotalQueries    int64   `json:"total_queries"`
	TotalErrors     int64   `json:"total_errors"`
	TotalReadQueries int64  `json:"total_read_queries"`
	TotalWriteQueries int64 `json:"total_write_queries"`
	ErrorRate       float64 `json:"error_rate"`

	// QPS statistics
	AvgQPS float64 `json:"avg_qps"`
	MinQPS float64 `json:"min_qps"`
	MaxQPS float64 `json:"max_qps"`

	// Latency statistics (microseconds)
	AvgLatencyUs int64 `json:"avg_latency_us"`
	MinLatencyUs int64 `json:"min_latency_us"`
	MaxLatencyUs int64 `json:"max_latency_us"`

	// Percentile latencies
	P50LatencyUs int64 `json:"p50_latency_us"`
	P95LatencyUs int64 `json:"p95_latency_us"`
	P99LatencyUs int64 `json:"p99_latency_us"`

	// Worker statistics
	AvgWorkers int `json:"avg_workers"`
	MinWorkers int `json:"min_workers"`
	MaxWorkers int `json:"max_workers"`

	// Target vs Actual
	AvgTargetQPS float64 `json:"avg_target_qps"`
	TargetHitRate float64 `json:"target_hit_rate"` // % of intervals where actual >= target
}

// CalculateSummary calculates summary statistics for the timeline.
func (t *Timeline) CalculateSummary() *TimelineSummary {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.Entries) == 0 {
		return &TimelineSummary{
			Interval: t.Interval,
		}
	}

	summary := &TimelineSummary{
		StartTime:    t.StartTime,
		EndTime:      t.EndTime,
		Intervals:    len(t.Entries),
		Interval:     t.Interval,
		TotalSeconds: t.EndTime.Sub(t.StartTime).Seconds(),
		MinQPS:       t.Entries[0].ActualQPS,
		MaxQPS:       t.Entries[0].ActualQPS,
		MinLatencyUs: t.Entries[0].AvgLatencyUs,
		MaxLatencyUs: t.Entries[0].AvgLatencyUs,
		MinWorkers:   t.Entries[0].ActiveWorkers,
		MaxWorkers:   t.Entries[0].ActiveWorkers,
	}

	var (
		totalQPS         float64
		totalLatency     int64
		totalP50         int64
		totalP95         int64
		totalP99         int64
		totalWorkers     int
		totalTargetQPS   float64
		intervalsHitTarget int
		entriesWithQueries int
	)

	for _, e := range t.Entries {
		// Aggregate counts
		summary.TotalQueries += e.TotalQueries
		summary.TotalErrors += e.TotalErrors
		summary.TotalReadQueries += e.ReadQueries
		summary.TotalWriteQueries += e.WriteQueries

		// QPS stats
		totalQPS += e.ActualQPS
		if e.ActualQPS < summary.MinQPS {
			summary.MinQPS = e.ActualQPS
		}
		if e.ActualQPS > summary.MaxQPS {
			summary.MaxQPS = e.ActualQPS
		}

		// Latency stats (only from entries with queries)
		if e.TotalQueries > 0 {
			entriesWithQueries++
			totalLatency += e.AvgLatencyUs
			totalP50 += e.P50LatencyUs
			totalP95 += e.P95LatencyUs
			totalP99 += e.P99LatencyUs

			if e.AvgLatencyUs < summary.MinLatencyUs || summary.MinLatencyUs == 0 {
				summary.MinLatencyUs = e.AvgLatencyUs
			}
			if e.AvgLatencyUs > summary.MaxLatencyUs {
				summary.MaxLatencyUs = e.AvgLatencyUs
			}
		}

		// Worker stats
		totalWorkers += e.ActiveWorkers
		if e.ActiveWorkers < summary.MinWorkers {
			summary.MinWorkers = e.ActiveWorkers
		}
		if e.ActiveWorkers > summary.MaxWorkers {
			summary.MaxWorkers = e.ActiveWorkers
		}

		// Target tracking
		totalTargetQPS += float64(e.TargetQPS)
		if e.TargetQPS > 0 && e.ActualQPS >= float64(e.TargetQPS)*0.9 {
			// Consider target "hit" if within 90%
			intervalsHitTarget++
		}
	}

	// Calculate averages
	n := len(t.Entries)
	summary.AvgQPS = totalQPS / float64(n)
	summary.AvgWorkers = totalWorkers / n
	summary.AvgTargetQPS = totalTargetQPS / float64(n)

	if entriesWithQueries > 0 {
		summary.AvgLatencyUs = totalLatency / int64(entriesWithQueries)
		summary.P50LatencyUs = totalP50 / int64(entriesWithQueries)
		summary.P95LatencyUs = totalP95 / int64(entriesWithQueries)
		summary.P99LatencyUs = totalP99 / int64(entriesWithQueries)
	}

	// Error rate
	if summary.TotalQueries > 0 {
		summary.ErrorRate = float64(summary.TotalErrors) / float64(summary.TotalQueries) * 100
	}

	// Target hit rate
	if n > 0 {
		summary.TargetHitRate = float64(intervalsHitTarget) / float64(n) * 100
	}

	return summary
}

// Format returns a formatted string representation of the summary.
func (s *TimelineSummary) Format() string {
	if s.Intervals == 0 {
		return "No data collected"
	}

	return formatSummary(s)
}

// formatSummary formats the summary for display.
func formatSummary(s *TimelineSummary) string {
	duration := s.EndTime.Sub(s.StartTime)

	return formatLines([]string{
		formatLine("Duration", formatDuration(duration)),
		formatLine("Intervals", formatInt(s.Intervals)),
		"",
		formatLine("Total Queries", formatInt64(s.TotalQueries)),
		formatLine("  Read", formatInt64(s.TotalReadQueries)),
		formatLine("  Write", formatInt64(s.TotalWriteQueries)),
		formatLine("Total Errors", formatInt64(s.TotalErrors)),
		formatLine("Error Rate", formatPct(s.ErrorRate)),
		"",
		formatLine("QPS (avg/min/max)", formatQPS(s.AvgQPS, s.MinQPS, s.MaxQPS)),
		formatLine("Target QPS (avg)", formatFloat(s.AvgTargetQPS)),
		formatLine("Target Hit Rate", formatPct(s.TargetHitRate)),
		"",
		formatLine("Latency avg", formatLatency(s.AvgLatencyUs)),
		formatLine("Latency p50", formatLatency(s.P50LatencyUs)),
		formatLine("Latency p95", formatLatency(s.P95LatencyUs)),
		formatLine("Latency p99", formatLatency(s.P99LatencyUs)),
		"",
		formatLine("Workers (avg/min/max)", formatWorkers(s.AvgWorkers, s.MinWorkers, s.MaxWorkers)),
	})
}

// Helper functions for formatting.
func formatLine(label, value string) string {
	return label + ": " + value
}

func formatLines(lines []string) string {
	result := ""
	for i, line := range lines {
		if i > 0 {
			result += "\n"
		}
		result += line
	}
	return result
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Round(time.Second).String()
	}
	if d < time.Hour {
		return d.Round(time.Second).String()
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return formatInt(hours) + "h " + formatInt(minutes) + "m"
}

func formatInt(v int) string {
	return intToString(v)
}

func formatInt64(v int64) string {
	return int64ToString(v)
}

func formatFloat(v float64) string {
	return floatToString(v, 2)
}

func formatPct(v float64) string {
	return floatToString(v, 2) + "%"
}

func formatQPS(avg, min, max float64) string {
	return floatToString(avg, 1) + " / " + floatToString(min, 1) + " / " + floatToString(max, 1)
}

func formatLatency(us int64) string {
	if us < 1000 {
		return int64ToString(us) + "us"
	}
	if us < 1000000 {
		return floatToString(float64(us)/1000, 2) + "ms"
	}
	return floatToString(float64(us)/1000000, 2) + "s"
}

func formatWorkers(avg, min, max int) string {
	return intToString(avg) + " / " + intToString(min) + " / " + intToString(max)
}

// Simple number to string conversions.
func intToString(v int) string {
	if v == 0 {
		return "0"
	}
	s := ""
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}

func int64ToString(v int64) string {
	if v == 0 {
		return "0"
	}
	s := ""
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}

func floatToString(v float64, decimals int) string {
	// Simple float formatting
	neg := v < 0
	if neg {
		v = -v
	}

	// Scale by decimals
	scale := 1.0
	for i := 0; i < decimals; i++ {
		scale *= 10
	}

	// Round
	scaled := int64(v*scale + 0.5)

	// Integer part
	intPart := scaled / int64(scale)
	fracPart := scaled % int64(scale)

	result := int64ToString(intPart)
	if decimals > 0 {
		result += "."
		// Pad fractional part with leading zeros
		fracStr := int64ToString(fracPart)
		for len(fracStr) < decimals {
			fracStr = "0" + fracStr
		}
		result += fracStr
	}

	if neg {
		result = "-" + result
	}

	return result
}
