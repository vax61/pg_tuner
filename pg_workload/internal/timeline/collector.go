package timeline

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// IntervalCollector collects metrics during a time interval.
type IntervalCollector struct {
	interval      time.Duration
	startTime     time.Time
	simulatedTime time.Time

	// Configuration
	multiplier    float64
	targetQPS     int
	activeWorkers int

	// Counters
	totalQueries atomic.Int64
	totalErrors  atomic.Int64
	readQueries  atomic.Int64
	writeQueries atomic.Int64

	// Latency histogram
	latencyHist *hdrhistogram.Histogram
	histMu      sync.Mutex
}

// NewIntervalCollector creates a new interval collector.
func NewIntervalCollector(interval time.Duration, simulatedTime time.Time) *IntervalCollector {
	return &IntervalCollector{
		interval:      interval,
		startTime:     time.Now(),
		simulatedTime: simulatedTime,
		latencyHist:   hdrhistogram.New(1, 60000000, 3), // 1us to 60s
	}
}

// SetConfig sets the configuration for this interval.
func (ic *IntervalCollector) SetConfig(multiplier float64, targetQPS int, activeWorkers int) {
	ic.multiplier = multiplier
	ic.targetQPS = targetQPS
	ic.activeWorkers = activeWorkers
}

// RecordQuery records a query execution.
func (ic *IntervalCollector) RecordQuery(latencyUs int64, isRead bool, isError bool) {
	ic.totalQueries.Add(1)

	if isError {
		ic.totalErrors.Add(1)
	}

	if isRead {
		ic.readQueries.Add(1)
	} else {
		ic.writeQueries.Add(1)
	}

	// Record latency
	ic.histMu.Lock()
	ic.latencyHist.RecordValue(latencyUs)
	ic.histMu.Unlock()
}

// RecordLatency records just the latency.
func (ic *IntervalCollector) RecordLatency(latencyUs int64) {
	ic.histMu.Lock()
	ic.latencyHist.RecordValue(latencyUs)
	ic.histMu.Unlock()
}

// ToEntry converts the collected metrics to a TimelineEntry.
func (ic *IntervalCollector) ToEntry() TimelineEntry {
	ic.histMu.Lock()
	defer ic.histMu.Unlock()

	elapsed := time.Since(ic.startTime).Seconds()
	totalQueries := ic.totalQueries.Load()

	var actualQPS float64
	if elapsed > 0 {
		actualQPS = float64(totalQueries) / elapsed
	}

	var avgLatency int64
	if totalQueries > 0 {
		avgLatency = int64(ic.latencyHist.Mean())
	}

	return TimelineEntry{
		Timestamp:     ic.startTime,
		SimulatedTime: ic.simulatedTime,
		IntervalSec:   int(ic.interval.Seconds()),
		Multiplier:    ic.multiplier,
		TargetQPS:     ic.targetQPS,
		ActualQPS:     actualQPS,
		TotalQueries:  totalQueries,
		TotalErrors:   ic.totalErrors.Load(),
		ActiveWorkers: ic.activeWorkers,
		AvgLatencyUs:  avgLatency,
		P50LatencyUs:  ic.latencyHist.ValueAtQuantile(50),
		P95LatencyUs:  ic.latencyHist.ValueAtQuantile(95),
		P99LatencyUs:  ic.latencyHist.ValueAtQuantile(99),
		ReadQueries:   ic.readQueries.Load(),
		WriteQueries:  ic.writeQueries.Load(),
	}
}

// Reset resets the collector for a new interval.
func (ic *IntervalCollector) Reset(simulatedTime time.Time) {
	ic.histMu.Lock()
	defer ic.histMu.Unlock()

	ic.startTime = time.Now()
	ic.simulatedTime = simulatedTime
	ic.totalQueries.Store(0)
	ic.totalErrors.Store(0)
	ic.readQueries.Store(0)
	ic.writeQueries.Store(0)
	ic.latencyHist.Reset()
}

// GetTotalQueries returns the total queries count.
func (ic *IntervalCollector) GetTotalQueries() int64 {
	return ic.totalQueries.Load()
}

// GetTotalErrors returns the total errors count.
func (ic *IntervalCollector) GetTotalErrors() int64 {
	return ic.totalErrors.Load()
}

// TimelineCollector manages interval collection for a streaming timeline.
type TimelineCollector struct {
	streaming *StreamingTimeline
	collector *IntervalCollector
	interval  time.Duration
	mu        sync.Mutex
}

// NewTimelineCollector creates a new timeline collector.
func NewTimelineCollector(path string, interval time.Duration, flushEvery int) (*TimelineCollector, error) {
	streaming, err := NewStreamingTimeline(path, interval, flushEvery)
	if err != nil {
		return nil, err
	}

	return &TimelineCollector{
		streaming: streaming,
		collector: NewIntervalCollector(interval, time.Now()),
		interval:  interval,
	}, nil
}

// SetConfig sets the configuration for the current interval.
func (tc *TimelineCollector) SetConfig(multiplier float64, targetQPS int, activeWorkers int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.collector.SetConfig(multiplier, targetQPS, activeWorkers)
}

// RecordQuery records a query execution.
func (tc *TimelineCollector) RecordQuery(latencyUs int64, isRead bool, isError bool) {
	tc.collector.RecordQuery(latencyUs, isRead, isError)
}

// Snapshot takes a snapshot of the current interval and resets the collector.
func (tc *TimelineCollector) Snapshot(simulatedTime time.Time) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	entry := tc.collector.ToEntry()
	tc.collector.Reset(simulatedTime)

	return tc.streaming.Record(entry)
}

// Flush flushes the streaming timeline.
func (tc *TimelineCollector) Flush() error {
	return tc.streaming.Flush()
}

// Close closes the timeline collector.
func (tc *TimelineCollector) Close() (int64, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Record final interval if it has any data
	if tc.collector.GetTotalQueries() > 0 {
		entry := tc.collector.ToEntry()
		tc.streaming.Record(entry)
	}

	return tc.streaming.Close()
}

// GetTimeline returns the underlying timeline.
func (tc *TimelineCollector) GetTimeline() *Timeline {
	return tc.streaming.GetTimeline()
}

// GetSummary returns a summary of the timeline.
func (tc *TimelineCollector) GetSummary() *TimelineSummary {
	return tc.streaming.GetSummary()
}
