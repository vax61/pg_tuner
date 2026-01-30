package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// Histogram range: 1 microsecond to 60 seconds
	minLatencyUs = 1
	maxLatencyUs = 60_000_000
	sigFigs      = 3
)

// opMetrics holds metrics for a single operation type.
type opMetrics struct {
	mu        sync.Mutex
	histogram *hdrhistogram.Histogram
	count     atomic.Int64
	errors    atomic.Int64
	errorMap  map[string]int64
}

func newOpMetrics() *opMetrics {
	return &opMetrics{
		histogram: hdrhistogram.New(minLatencyUs, maxLatencyUs, sigFigs),
		errorMap:  make(map[string]int64),
	}
}

// Collector aggregates metrics for multiple operation types.
type Collector struct {
	mu        sync.RWMutex
	ops       map[string]*opMetrics
	startTime time.Time
}

// NewCollector creates a new metrics Collector.
func NewCollector() *Collector {
	return &Collector{
		ops:       make(map[string]*opMetrics),
		startTime: time.Now(),
	}
}

// getOrCreateOp returns metrics for an operation type, creating if needed.
func (c *Collector) getOrCreateOp(opType string) *opMetrics {
	c.mu.RLock()
	op, exists := c.ops[opType]
	c.mu.RUnlock()

	if exists {
		return op
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if op, exists = c.ops[opType]; exists {
		return op
	}

	op = newOpMetrics()
	c.ops[opType] = op
	return op
}

// RecordLatency records a query latency in nanoseconds.
func (c *Collector) RecordLatency(opType string, latencyNs int64) {
	op := c.getOrCreateOp(opType)

	// Convert to microseconds for histogram
	latencyUs := latencyNs / 1000
	if latencyUs < minLatencyUs {
		latencyUs = minLatencyUs
	}
	if latencyUs > maxLatencyUs {
		latencyUs = maxLatencyUs
	}

	op.mu.Lock()
	op.histogram.RecordValue(latencyUs)
	op.mu.Unlock()

	op.count.Add(1)
}

// IncrementCount increments the operation count without recording latency.
func (c *Collector) IncrementCount(opType string) {
	op := c.getOrCreateOp(opType)
	op.count.Add(1)
}

// IncrementError increments the error count for an operation type.
func (c *Collector) IncrementError(opType string, errType string) {
	op := c.getOrCreateOp(opType)
	op.errors.Add(1)

	op.mu.Lock()
	op.errorMap[errType]++
	op.mu.Unlock()
}

// GetSnapshot returns a point-in-time snapshot of all metrics.
func (c *Collector) GetSnapshot() *Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	duration := time.Since(c.startTime)
	snap := &Snapshot{
		StartTime:  c.startTime,
		Duration:   duration,
		Operations: make(map[string]*OperationStats),
	}

	var totalQueries, totalErrors int64

	for opType, op := range c.ops {
		count := op.count.Load()
		errors := op.errors.Load()

		totalQueries += count
		totalErrors += errors

		op.mu.Lock()
		hist := op.histogram.Export()
		errorMapCopy := make(map[string]int64)
		for k, v := range op.errorMap {
			errorMapCopy[k] = v
		}
		op.mu.Unlock()

		imported := hdrhistogram.Import(hist)

		opStats := &OperationStats{
			Count:  count,
			Errors: errors,
			Latency: LatencyStats{
				Min:    time.Duration(imported.Min()) * time.Microsecond,
				Max:    time.Duration(imported.Max()) * time.Microsecond,
				Mean:   time.Duration(imported.Mean()) * time.Microsecond,
				StdDev: time.Duration(imported.StdDev()) * time.Microsecond,
				P50:    time.Duration(imported.ValueAtQuantile(50)) * time.Microsecond,
				P90:    time.Duration(imported.ValueAtQuantile(90)) * time.Microsecond,
				P95:    time.Duration(imported.ValueAtQuantile(95)) * time.Microsecond,
				P99:    time.Duration(imported.ValueAtQuantile(99)) * time.Microsecond,
				P999:   time.Duration(imported.ValueAtQuantile(99.9)) * time.Microsecond,
			},
			ErrorTypes: errorMapCopy,
		}

		if duration.Seconds() > 0 {
			opStats.QPS = float64(count) / duration.Seconds()
		}

		snap.Operations[opType] = opStats
	}

	snap.TotalQueries = totalQueries
	snap.TotalErrors = totalErrors

	if duration.Seconds() > 0 {
		snap.QPS = float64(totalQueries) / duration.Seconds()
	}

	return snap
}

// Reset clears all collected metrics and resets the start time.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ops = make(map[string]*opMetrics)
	c.startTime = time.Now()
}
