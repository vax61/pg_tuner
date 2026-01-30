package metrics

import (
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"
)

func TestNewCollector(t *testing.T) {
	c := NewCollector()
	if c == nil {
		t.Fatal("NewCollector returned nil")
	}
	if c.ops == nil {
		t.Error("ops map not initialized")
	}
}

func TestRecordLatency(t *testing.T) {
	c := NewCollector()

	// Record some latencies (in nanoseconds)
	c.RecordLatency("select", 1_000_000)  // 1ms
	c.RecordLatency("select", 2_000_000)  // 2ms
	c.RecordLatency("select", 3_000_000)  // 3ms
	c.RecordLatency("insert", 5_000_000)  // 5ms

	snap := c.GetSnapshot()

	if snap.TotalQueries != 4 {
		t.Errorf("expected 4 total queries, got %d", snap.TotalQueries)
	}

	selectStats, ok := snap.Operations["select"]
	if !ok {
		t.Fatal("select operation not found")
	}
	if selectStats.Count != 3 {
		t.Errorf("expected 3 select operations, got %d", selectStats.Count)
	}

	insertStats, ok := snap.Operations["insert"]
	if !ok {
		t.Fatal("insert operation not found")
	}
	if insertStats.Count != 1 {
		t.Errorf("expected 1 insert operation, got %d", insertStats.Count)
	}
}

func TestIncrementCount(t *testing.T) {
	c := NewCollector()

	c.IncrementCount("commit")
	c.IncrementCount("commit")
	c.IncrementCount("rollback")

	snap := c.GetSnapshot()

	if snap.TotalQueries != 3 {
		t.Errorf("expected 3 total queries, got %d", snap.TotalQueries)
	}

	commitStats := snap.Operations["commit"]
	if commitStats.Count != 2 {
		t.Errorf("expected 2 commits, got %d", commitStats.Count)
	}
}

func TestIncrementError(t *testing.T) {
	c := NewCollector()

	c.RecordLatency("select", 1_000_000)
	c.IncrementError("select", "timeout")
	c.IncrementError("select", "timeout")
	c.IncrementError("select", "connection_lost")

	snap := c.GetSnapshot()

	if snap.TotalErrors != 3 {
		t.Errorf("expected 3 total errors, got %d", snap.TotalErrors)
	}

	selectStats := snap.Operations["select"]
	if selectStats.Errors != 3 {
		t.Errorf("expected 3 select errors, got %d", selectStats.Errors)
	}
	if selectStats.ErrorTypes["timeout"] != 2 {
		t.Errorf("expected 2 timeout errors, got %d", selectStats.ErrorTypes["timeout"])
	}
	if selectStats.ErrorTypes["connection_lost"] != 1 {
		t.Errorf("expected 1 connection_lost error, got %d", selectStats.ErrorTypes["connection_lost"])
	}
}

func TestLatencyPercentiles(t *testing.T) {
	c := NewCollector()

	// Record 100 latencies from 1ms to 100ms
	for i := 1; i <= 100; i++ {
		c.RecordLatency("query", int64(i)*1_000_000) // i ms in nanoseconds
	}

	snap := c.GetSnapshot()
	stats := snap.Operations["query"]

	// Check percentiles are in expected ranges
	// P50 should be around 50ms
	if stats.Latency.P50 < 45*time.Millisecond || stats.Latency.P50 > 55*time.Millisecond {
		t.Errorf("P50 out of range: got %v, expected ~50ms", stats.Latency.P50)
	}

	// P90 should be around 90ms
	if stats.Latency.P90 < 85*time.Millisecond || stats.Latency.P90 > 95*time.Millisecond {
		t.Errorf("P90 out of range: got %v, expected ~90ms", stats.Latency.P90)
	}

	// P99 should be around 99ms
	if stats.Latency.P99 < 95*time.Millisecond || stats.Latency.P99 > 100*time.Millisecond {
		t.Errorf("P99 out of range: got %v, expected ~99ms", stats.Latency.P99)
	}

	// Min should be ~1ms
	if stats.Latency.Min < 900*time.Microsecond || stats.Latency.Min > 1100*time.Microsecond {
		t.Errorf("Min out of range: got %v, expected ~1ms", stats.Latency.Min)
	}

	// Max should be ~100ms
	if stats.Latency.Max < 99*time.Millisecond || stats.Latency.Max > 101*time.Millisecond {
		t.Errorf("Max out of range: got %v, expected ~100ms", stats.Latency.Max)
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := NewCollector()

	const numGoroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			opType := "op"
			if id%2 == 0 {
				opType = "op_even"
			} else {
				opType = "op_odd"
			}

			for j := 0; j < opsPerGoroutine; j++ {
				c.RecordLatency(opType, int64((j+1)*1_000_000))
				if j%100 == 0 {
					c.IncrementError(opType, "test_error")
				}
			}
		}(i)
	}

	wg.Wait()

	snap := c.GetSnapshot()

	expectedTotal := int64(numGoroutines * opsPerGoroutine)
	if snap.TotalQueries != expectedTotal {
		t.Errorf("expected %d total queries, got %d", expectedTotal, snap.TotalQueries)
	}

	// Each goroutine records 10 errors (1000/100)
	expectedErrors := int64(numGoroutines * 10)
	if snap.TotalErrors != expectedErrors {
		t.Errorf("expected %d total errors, got %d", expectedErrors, snap.TotalErrors)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	c := NewCollector()

	const numWriters = 10
	const numReaders = 5
	const duration = 100 * time.Millisecond

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start writers
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.RecordLatency("concurrent", 1_000_000)
				}
			}
		}()
	}

	// Start readers
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.GetSnapshot()
				}
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	snap := c.GetSnapshot()
	if snap.TotalQueries == 0 {
		t.Error("expected some queries to be recorded")
	}
}

func TestReset(t *testing.T) {
	c := NewCollector()

	c.RecordLatency("select", 1_000_000)
	c.IncrementError("select", "error")

	snap := c.GetSnapshot()
	if snap.TotalQueries != 1 {
		t.Errorf("expected 1 query before reset, got %d", snap.TotalQueries)
	}

	c.Reset()

	snap = c.GetSnapshot()
	if snap.TotalQueries != 0 {
		t.Errorf("expected 0 queries after reset, got %d", snap.TotalQueries)
	}
	if snap.TotalErrors != 0 {
		t.Errorf("expected 0 errors after reset, got %d", snap.TotalErrors)
	}
}

func TestSnapshotToJSON(t *testing.T) {
	c := NewCollector()

	c.RecordLatency("select", 1_000_000)
	c.RecordLatency("select", 2_000_000)
	c.IncrementError("select", "timeout")

	snap := c.GetSnapshot()
	jsonData, err := snap.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if parsed["total_queries"].(float64) != 2 {
		t.Errorf("expected total_queries=2 in JSON")
	}
}

func TestSnapshotErrorRate(t *testing.T) {
	c := NewCollector()

	for i := 0; i < 100; i++ {
		c.RecordLatency("op", 1_000_000)
	}
	for i := 0; i < 10; i++ {
		c.IncrementError("op", "error")
	}

	snap := c.GetSnapshot()

	expectedRate := 10.0
	if math.Abs(snap.ErrorRate()-expectedRate) > 0.01 {
		t.Errorf("expected error rate %.2f%%, got %.2f%%", expectedRate, snap.ErrorRate())
	}

	expectedSuccess := 90.0
	if math.Abs(snap.SuccessRate()-expectedSuccess) > 0.01 {
		t.Errorf("expected success rate %.2f%%, got %.2f%%", expectedSuccess, snap.SuccessRate())
	}
}

func TestQPSCalculation(t *testing.T) {
	c := NewCollector()

	// Record 1000 operations
	for i := 0; i < 1000; i++ {
		c.RecordLatency("op", 1_000_000)
	}

	// Wait a bit to get measurable duration
	time.Sleep(100 * time.Millisecond)

	snap := c.GetSnapshot()

	if snap.QPS <= 0 {
		t.Errorf("expected positive QPS, got %f", snap.QPS)
	}

	// QPS should be roughly 1000 / duration
	expectedQPS := 1000.0 / snap.Duration.Seconds()
	tolerance := expectedQPS * 0.1 // 10% tolerance
	if math.Abs(snap.QPS-expectedQPS) > tolerance {
		t.Errorf("QPS %f not close to expected %f", snap.QPS, expectedQPS)
	}
}

func TestMinLatencyClamp(t *testing.T) {
	c := NewCollector()

	// Record very small latency (100ns = 0.1µs, should clamp to 1µs)
	c.RecordLatency("fast", 100)

	snap := c.GetSnapshot()
	stats := snap.Operations["fast"]

	// Should be clamped to minimum (1µs)
	if stats.Latency.Min != 1*time.Microsecond {
		t.Errorf("expected min latency 1µs, got %v", stats.Latency.Min)
	}
}

func BenchmarkRecordLatency(b *testing.B) {
	c := NewCollector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RecordLatency("bench", 1_000_000)
	}
}

func BenchmarkRecordLatencyParallel(b *testing.B) {
	c := NewCollector()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.RecordLatency("bench", 1_000_000)
		}
	})
}

func BenchmarkGetSnapshot(b *testing.B) {
	c := NewCollector()

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		c.RecordLatency("op1", 1_000_000)
		c.RecordLatency("op2", 2_000_000)
		c.RecordLatency("op3", 3_000_000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetSnapshot()
	}
}
