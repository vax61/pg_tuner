package storage

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Aggregator aggregates raw data into time buckets.
type Aggregator struct {
	interval time.Duration
	buckets  map[time.Time]*AggregateBucket
	mu       sync.RWMutex
}

// AggregateBucket contains aggregated statistics for a time interval.
type AggregateBucket struct {
	Timestamp  time.Time                  `json:"timestamp"`
	Duration   time.Duration              `json:"duration"`
	Operations map[string]*OperationStats `json:"operations"`
}

// OperationStats contains statistics for a single operation type.
type OperationStats struct {
	Count      int64 `json:"count"`
	Errors     int64 `json:"errors"`
	Min        int64 `json:"min_ns"`
	Max        int64 `json:"max_ns"`
	Sum        int64 `json:"sum_ns"`
	SumSquares int64 `json:"sum_squares"` // For stddev calculation

	// Compact histogram for percentiles
	histogram *hdrhistogram.Histogram
}

// NewAggregator creates a new Aggregator.
func NewAggregator(interval time.Duration) *Aggregator {
	if interval <= 0 {
		interval = 1 * time.Minute
	}

	return &Aggregator{
		interval: interval,
		buckets:  make(map[time.Time]*AggregateBucket),
	}
}

// Record records a measurement into the appropriate bucket.
func (a *Aggregator) Record(timestamp time.Time, op string, latencyNs int64, err error) {
	// Calculate bucket timestamp (floor to interval)
	bucketTime := timestamp.Truncate(a.interval)

	a.mu.Lock()
	defer a.mu.Unlock()

	// Get or create bucket
	bucket, ok := a.buckets[bucketTime]
	if !ok {
		bucket = &AggregateBucket{
			Timestamp:  bucketTime,
			Duration:   a.interval,
			Operations: make(map[string]*OperationStats),
		}
		a.buckets[bucketTime] = bucket
	}

	// Get or create operation stats
	stats, ok := bucket.Operations[op]
	if !ok {
		stats = &OperationStats{
			Min:       math.MaxInt64,
			Max:       0,
			histogram: hdrhistogram.New(1, 60000000000, 3), // 1ns to 60s, 3 sig figs
		}
		bucket.Operations[op] = stats
	}

	// Update stats
	stats.Count++
	stats.Sum += latencyNs
	stats.SumSquares += latencyNs * latencyNs

	if latencyNs < stats.Min {
		stats.Min = latencyNs
	}
	if latencyNs > stats.Max {
		stats.Max = latencyNs
	}

	if err != nil {
		stats.Errors++
	}

	// Record in histogram
	stats.histogram.RecordValue(latencyNs)
}

// FlushBucket returns and removes a specific bucket.
func (a *Aggregator) FlushBucket(t time.Time) *AggregateBucket {
	bucketTime := t.Truncate(a.interval)

	a.mu.Lock()
	defer a.mu.Unlock()

	bucket, ok := a.buckets[bucketTime]
	if !ok {
		return nil
	}

	delete(a.buckets, bucketTime)
	return bucket
}

// FlushCompletedBuckets returns and removes all completed buckets.
// A bucket is completed if its end time is before now.
func (a *Aggregator) FlushCompletedBuckets() []*AggregateBucket {
	now := time.Now()
	cutoff := now.Truncate(a.interval) // Current bucket is not completed

	a.mu.Lock()
	defer a.mu.Unlock()

	var completed []*AggregateBucket

	for bucketTime, bucket := range a.buckets {
		if bucketTime.Add(a.interval).Before(cutoff) || bucketTime.Add(a.interval).Equal(cutoff) {
			completed = append(completed, bucket)
			delete(a.buckets, bucketTime)
		}
	}

	// Sort by timestamp
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].Timestamp.Before(completed[j].Timestamp)
	})

	return completed
}

// GetAllBuckets returns all buckets without removing them.
func (a *Aggregator) GetAllBuckets() []*AggregateBucket {
	a.mu.RLock()
	defer a.mu.RUnlock()

	buckets := make([]*AggregateBucket, 0, len(a.buckets))
	for _, bucket := range a.buckets {
		buckets = append(buckets, bucket)
	}

	// Sort by timestamp
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp.Before(buckets[j].Timestamp)
	})

	return buckets
}

// GetBucket returns a specific bucket without removing it.
func (a *Aggregator) GetBucket(t time.Time) *AggregateBucket {
	bucketTime := t.Truncate(a.interval)

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.buckets[bucketTime]
}

// BucketCount returns the number of buckets.
func (a *Aggregator) BucketCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.buckets)
}

// Interval returns the aggregation interval.
func (a *Aggregator) Interval() time.Duration {
	return a.interval
}

// Clear removes all buckets.
func (a *Aggregator) Clear() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buckets = make(map[time.Time]*AggregateBucket)
}

// OperationStats methods

// Mean returns the mean latency in nanoseconds.
func (s *OperationStats) Mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.Sum) / float64(s.Count)
}

// StdDev returns the standard deviation in nanoseconds.
func (s *OperationStats) StdDev() float64 {
	if s.Count < 2 {
		return 0
	}

	mean := s.Mean()
	variance := (float64(s.SumSquares) / float64(s.Count)) - (mean * mean)
	if variance < 0 {
		variance = 0 // Numerical stability
	}
	return math.Sqrt(variance)
}

// Percentile returns the latency at the given percentile.
func (s *OperationStats) Percentile(pct float64) int64 {
	if s.histogram == nil {
		return 0
	}
	return s.histogram.ValueAtQuantile(pct)
}

// P50 returns the 50th percentile latency.
func (s *OperationStats) P50() int64 {
	return s.Percentile(50.0)
}

// P90 returns the 90th percentile latency.
func (s *OperationStats) P90() int64 {
	return s.Percentile(90.0)
}

// P95 returns the 95th percentile latency.
func (s *OperationStats) P95() int64 {
	return s.Percentile(95.0)
}

// P99 returns the 99th percentile latency.
func (s *OperationStats) P99() int64 {
	return s.Percentile(99.0)
}

// ErrorRate returns the error rate as a percentage.
func (s *OperationStats) ErrorRate() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Count) * 100.0
}

// ToSummary returns a serializable summary without the histogram.
func (s *OperationStats) ToSummary() OperationStatsSummary {
	return OperationStatsSummary{
		Count:     s.Count,
		Errors:    s.Errors,
		ErrorRate: s.ErrorRate(),
		MinNs:     s.Min,
		MaxNs:     s.Max,
		MeanNs:    s.Mean(),
		StdDevNs:  s.StdDev(),
		P50Ns:     s.P50(),
		P90Ns:     s.P90(),
		P95Ns:     s.P95(),
		P99Ns:     s.P99(),
	}
}

// OperationStatsSummary is a JSON-serializable summary of OperationStats.
type OperationStatsSummary struct {
	Count     int64   `json:"count"`
	Errors    int64   `json:"errors"`
	ErrorRate float64 `json:"error_rate_pct"`
	MinNs     int64   `json:"min_ns"`
	MaxNs     int64   `json:"max_ns"`
	MeanNs    float64 `json:"mean_ns"`
	StdDevNs  float64 `json:"stddev_ns"`
	P50Ns     int64   `json:"p50_ns"`
	P90Ns     int64   `json:"p90_ns"`
	P95Ns     int64   `json:"p95_ns"`
	P99Ns     int64   `json:"p99_ns"`
}

// AggregateBucketSummary is a JSON-serializable summary of a bucket.
type AggregateBucketSummary struct {
	Timestamp  time.Time                        `json:"timestamp"`
	Duration   string                           `json:"duration"`
	Operations map[string]OperationStatsSummary `json:"operations"`
}

// ToSummary returns a serializable summary of the bucket.
func (b *AggregateBucket) ToSummary() AggregateBucketSummary {
	summary := AggregateBucketSummary{
		Timestamp:  b.Timestamp,
		Duration:   b.Duration.String(),
		Operations: make(map[string]OperationStatsSummary),
	}

	for op, stats := range b.Operations {
		summary.Operations[op] = stats.ToSummary()
	}

	return summary
}
