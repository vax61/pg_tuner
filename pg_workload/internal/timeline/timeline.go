package timeline

import (
	"sync"
	"time"
)

// Timeline stores a sequence of performance metrics over time.
type Timeline struct {
	Interval  time.Duration
	Entries   []TimelineEntry
	StartTime time.Time
	EndTime   time.Time
	mu        sync.RWMutex
}

// TimelineEntry represents metrics for a single time interval.
type TimelineEntry struct {
	Timestamp     time.Time `json:"timestamp"`
	SimulatedTime time.Time `json:"simulated_time"`
	IntervalSec   int       `json:"interval_sec"`
	Multiplier    float64   `json:"multiplier"`
	TargetQPS     int       `json:"target_qps"`
	ActualQPS     float64   `json:"actual_qps"`
	TotalQueries  int64     `json:"total_queries"`
	TotalErrors   int64     `json:"total_errors"`
	ActiveWorkers int       `json:"active_workers"`
	AvgLatencyUs  int64     `json:"avg_latency_us"`
	P50LatencyUs  int64     `json:"p50_latency_us"`
	P95LatencyUs  int64     `json:"p95_latency_us"`
	P99LatencyUs  int64     `json:"p99_latency_us"`
	ReadQueries   int64     `json:"read_queries"`
	WriteQueries  int64     `json:"write_queries"`
}

// NewTimeline creates a new Timeline with the specified interval.
func NewTimeline(interval time.Duration) *Timeline {
	if interval <= 0 {
		interval = 1 * time.Minute
	}

	return &Timeline{
		Interval: interval,
		Entries:  make([]TimelineEntry, 0, 1440), // Pre-allocate for 24h at 1min
	}
}

// AddEntry adds a new entry to the timeline.
func (t *Timeline) AddEntry(entry TimelineEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Update start/end times
	if t.StartTime.IsZero() || entry.Timestamp.Before(t.StartTime) {
		t.StartTime = entry.Timestamp
	}
	if entry.Timestamp.After(t.EndTime) {
		t.EndTime = entry.Timestamp
	}

	t.Entries = append(t.Entries, entry)
}

// GetEntries returns a copy of all entries.
func (t *Timeline) GetEntries() []TimelineEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]TimelineEntry, len(t.Entries))
	copy(result, t.Entries)
	return result
}

// GetEntry returns the entry at the specified index.
func (t *Timeline) GetEntry(index int) (TimelineEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if index < 0 || index >= len(t.Entries) {
		return TimelineEntry{}, false
	}
	return t.Entries[index], true
}

// Len returns the number of entries.
func (t *Timeline) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Entries)
}

// Duration returns the total duration covered by the timeline.
func (t *Timeline) Duration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.StartTime.IsZero() || t.EndTime.IsZero() {
		return 0
	}
	return t.EndTime.Sub(t.StartTime)
}

// GetEntriesInRange returns entries within the specified time range.
func (t *Timeline) GetEntriesInRange(start, end time.Time) []TimelineEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []TimelineEntry
	for _, e := range t.Entries {
		if (e.Timestamp.Equal(start) || e.Timestamp.After(start)) &&
			(e.Timestamp.Equal(end) || e.Timestamp.Before(end)) {
			result = append(result, e)
		}
	}
	return result
}

// GetLastN returns the last n entries.
func (t *Timeline) GetLastN(n int) []TimelineEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if n >= len(t.Entries) {
		result := make([]TimelineEntry, len(t.Entries))
		copy(result, t.Entries)
		return result
	}

	result := make([]TimelineEntry, n)
	copy(result, t.Entries[len(t.Entries)-n:])
	return result
}

// Clear removes all entries.
func (t *Timeline) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Entries = t.Entries[:0]
	t.StartTime = time.Time{}
	t.EndTime = time.Time{}
}

// GetSummary calculates and returns a summary of the timeline.
func (t *Timeline) GetSummary() *TimelineSummary {
	return t.CalculateSummary()
}
