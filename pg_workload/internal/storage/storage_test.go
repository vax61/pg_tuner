package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRawBuffer_AddAndSize tests basic add functionality.
func TestRawBuffer_AddAndSize(t *testing.T) {
	rb := NewRawBuffer(5*time.Minute, 1<<20) // 1MB

	if rb.Size() != 0 {
		t.Errorf("Initial size = %d, want 0", rb.Size())
	}

	// Add entries
	for i := 0; i < 100; i++ {
		rb.Add(RawEntry{
			Timestamp: time.Now(),
			Operation: "test",
			LatencyNs: int64(i * 1000),
			Success:   true,
		})
	}

	if rb.Size() != 100 {
		t.Errorf("Size after 100 adds = %d, want 100", rb.Size())
	}
}

// TestRawBuffer_FIFO tests that oldest entries are overwritten when full.
func TestRawBuffer_FIFO(t *testing.T) {
	// Create small buffer
	rb := &RawBuffer{
		entries:    make([]RawEntry, 10),
		maxEntries: 10,
		retention:  5 * time.Minute,
	}

	// Add 15 entries
	for i := 0; i < 15; i++ {
		rb.Add(RawEntry{
			Timestamp: time.Now(),
			Operation: "test",
			LatencyNs: int64(i),
			Success:   true,
		})
	}

	// Should only have 10 entries
	if rb.Size() != 10 {
		t.Errorf("Size = %d, want 10 (capacity)", rb.Size())
	}

	// Should have the newest entries (5-14)
	entries := rb.GetAll()
	for i, e := range entries {
		expected := int64(i + 5)
		if e.LatencyNs != expected {
			t.Errorf("Entry %d has LatencyNs = %d, want %d", i, e.LatencyNs, expected)
		}
	}
}

// TestRawBuffer_Prune tests retention-based pruning.
func TestRawBuffer_Prune(t *testing.T) {
	rb := NewRawBuffer(100*time.Millisecond, 1<<20)

	// Add old entry
	rb.Add(RawEntry{
		Timestamp: time.Now().Add(-200 * time.Millisecond),
		Operation: "old",
		LatencyNs: 1000,
		Success:   true,
	})

	// Add recent entry
	rb.Add(RawEntry{
		Timestamp: time.Now(),
		Operation: "new",
		LatencyNs: 2000,
		Success:   true,
	})

	if rb.Size() != 2 {
		t.Errorf("Size before prune = %d, want 2", rb.Size())
	}

	// Prune old entries
	pruned := rb.Prune()

	if pruned != 1 {
		t.Errorf("Pruned = %d, want 1", pruned)
	}

	if rb.Size() != 1 {
		t.Errorf("Size after prune = %d, want 1", rb.Size())
	}

	// Remaining entry should be the new one
	entries := rb.GetAll()
	if len(entries) != 1 || entries[0].Operation != "new" {
		t.Error("Wrong entry remained after prune")
	}
}

// TestRawBuffer_Flush tests flush functionality.
func TestRawBuffer_Flush(t *testing.T) {
	rb := NewRawBuffer(100*time.Millisecond, 1<<20)

	// Add entries at different times
	rb.Add(RawEntry{
		Timestamp: time.Now().Add(-200 * time.Millisecond),
		Operation: "expired1",
		LatencyNs: 1000,
		Success:   true,
	})
	rb.Add(RawEntry{
		Timestamp: time.Now().Add(-150 * time.Millisecond),
		Operation: "expired2",
		LatencyNs: 2000,
		Success:   true,
	})
	rb.Add(RawEntry{
		Timestamp: time.Now(),
		Operation: "current",
		LatencyNs: 3000,
		Success:   true,
	})

	// Flush expired entries
	flushed := rb.Flush()

	if len(flushed) != 2 {
		t.Errorf("Flushed %d entries, want 2", len(flushed))
	}

	if rb.Size() != 1 {
		t.Errorf("Size after flush = %d, want 1", rb.Size())
	}
}

// TestRawBuffer_GetRecent tests getting recent entries.
func TestRawBuffer_GetRecent(t *testing.T) {
	rb := NewRawBuffer(5*time.Minute, 1<<20)

	for i := 0; i < 10; i++ {
		rb.Add(RawEntry{
			Timestamp: time.Now(),
			Operation: "test",
			LatencyNs: int64(i),
			Success:   true,
		})
	}

	recent := rb.GetRecent(3)
	if len(recent) != 3 {
		t.Errorf("GetRecent(3) returned %d entries, want 3", len(recent))
	}

	// Should be the last 3 entries (7, 8, 9)
	for i, e := range recent {
		expected := int64(7 + i)
		if e.LatencyNs != expected {
			t.Errorf("Recent[%d].LatencyNs = %d, want %d", i, e.LatencyNs, expected)
		}
	}
}

// TestRawBuffer_GetInWindow tests window-based retrieval.
func TestRawBuffer_GetInWindow(t *testing.T) {
	rb := NewRawBuffer(5*time.Minute, 1<<20)

	baseTime := time.Now()

	for i := 0; i < 10; i++ {
		rb.Add(RawEntry{
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Operation: "test",
			LatencyNs: int64(i),
			Success:   true,
		})
	}

	// Get entries in window [3s, 7s]
	start := baseTime.Add(3 * time.Second)
	end := baseTime.Add(7 * time.Second)

	entries := rb.GetInWindow(start, end)

	if len(entries) != 5 {
		t.Errorf("GetInWindow returned %d entries, want 5", len(entries))
	}
}

// TestAggregator_Record tests basic recording.
func TestAggregator_Record(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	now := time.Now()

	// Record some entries
	agg.Record(now, "select", 1000000, nil)  // 1ms
	agg.Record(now, "select", 2000000, nil)  // 2ms
	agg.Record(now, "insert", 3000000, nil)  // 3ms

	buckets := agg.GetAllBuckets()
	if len(buckets) != 1 {
		t.Fatalf("Expected 1 bucket, got %d", len(buckets))
	}

	bucket := buckets[0]
	if len(bucket.Operations) != 2 {
		t.Errorf("Expected 2 operations, got %d", len(bucket.Operations))
	}

	selectStats := bucket.Operations["select"]
	if selectStats == nil {
		t.Fatal("Missing select stats")
	}

	if selectStats.Count != 2 {
		t.Errorf("Select count = %d, want 2", selectStats.Count)
	}
}

// TestAggregator_Bucketing tests that entries go to correct buckets.
func TestAggregator_Bucketing(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	// Record in different minutes
	t1 := time.Date(2024, 1, 1, 10, 0, 30, 0, time.UTC)
	t2 := time.Date(2024, 1, 1, 10, 1, 30, 0, time.UTC)
	t3 := time.Date(2024, 1, 1, 10, 2, 30, 0, time.UTC)

	agg.Record(t1, "op1", 1000, nil)
	agg.Record(t2, "op2", 2000, nil)
	agg.Record(t3, "op3", 3000, nil)

	if agg.BucketCount() != 3 {
		t.Errorf("BucketCount = %d, want 3", agg.BucketCount())
	}
}

// TestAggregator_Stats tests statistics calculation.
func TestAggregator_Stats(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	now := time.Now()

	// Record entries with known values
	latencies := []int64{100, 200, 300, 400, 500}
	for _, lat := range latencies {
		agg.Record(now, "test", lat, nil)
	}

	bucket := agg.GetBucket(now)
	stats := bucket.Operations["test"]

	if stats.Count != 5 {
		t.Errorf("Count = %d, want 5", stats.Count)
	}

	if stats.Min != 100 {
		t.Errorf("Min = %d, want 100", stats.Min)
	}

	if stats.Max != 500 {
		t.Errorf("Max = %d, want 500", stats.Max)
	}

	expectedMean := 300.0
	if stats.Mean() != expectedMean {
		t.Errorf("Mean = %f, want %f", stats.Mean(), expectedMean)
	}
}

// TestAggregator_Errors tests error counting.
func TestAggregator_Errors(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	now := time.Now()

	agg.Record(now, "test", 1000, nil)
	agg.Record(now, "test", 2000, nil)
	agg.Record(now, "test", 3000, os.ErrNotExist)

	stats := agg.GetBucket(now).Operations["test"]

	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}

	expectedErrorRate := 100.0 / 3.0
	if stats.ErrorRate() < expectedErrorRate-1 || stats.ErrorRate() > expectedErrorRate+1 {
		t.Errorf("ErrorRate = %f, want ~%f", stats.ErrorRate(), expectedErrorRate)
	}
}

// TestAggregator_FlushCompletedBuckets tests flushing completed buckets.
func TestAggregator_FlushCompletedBuckets(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	// Create an old bucket
	oldTime := time.Now().Add(-5 * time.Minute)
	agg.Record(oldTime, "old", 1000, nil)

	// Create a current bucket
	agg.Record(time.Now(), "current", 2000, nil)

	if agg.BucketCount() != 2 {
		t.Errorf("BucketCount before flush = %d, want 2", agg.BucketCount())
	}

	// Flush completed buckets
	flushed := agg.FlushCompletedBuckets()

	if len(flushed) != 1 {
		t.Errorf("Flushed %d buckets, want 1", len(flushed))
	}

	if agg.BucketCount() != 1 {
		t.Errorf("BucketCount after flush = %d, want 1", agg.BucketCount())
	}
}

// TestFileWriter_WriteAggregate tests writing aggregate buckets.
func TestFileWriter_WriteAggregate(t *testing.T) {
	tmpDir := t.TempDir()

	fw, err := NewFileWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileWriter: %v", err)
	}
	defer fw.Close()

	bucket := &AggregateBucket{
		Timestamp: time.Now(),
		Duration:  1 * time.Minute,
		Operations: map[string]*OperationStats{
			"select": {
				Count:  100,
				Errors: 5,
				Min:    1000,
				Max:    10000,
				Sum:    500000,
			},
		},
	}

	if err := fw.WriteAggregate(bucket); err != nil {
		t.Fatalf("WriteAggregate failed: %v", err)
	}

	fw.Flush()

	// Read and verify file
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	var foundAggregate bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "aggregate_") {
			foundAggregate = true

			data, err := os.ReadFile(filepath.Join(tmpDir, e.Name()))
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			// Verify JSON is valid
			var parsed AggregateBucketSummary
			if err := json.Unmarshal(data[:len(data)-1], &parsed); err != nil { // -1 for newline
				t.Fatalf("Invalid JSON: %v", err)
			}

			if parsed.Operations["select"].Count != 100 {
				t.Errorf("Parsed count = %d, want 100", parsed.Operations["select"].Count)
			}
		}
	}

	if !foundAggregate {
		t.Error("No aggregate file created")
	}
}

// TestFileWriter_Rotate tests file rotation.
func TestFileWriter_Rotate(t *testing.T) {
	tmpDir := t.TempDir()

	fw, err := NewFileWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileWriter: %v", err)
	}
	defer fw.Close()

	// Write first bucket
	bucket1 := &AggregateBucket{
		Timestamp: time.Now(),
		Duration:  1 * time.Minute,
		Operations: map[string]*OperationStats{
			"test": {Count: 100, Min: 1000, Max: 2000, Sum: 150000},
		},
	}
	fw.WriteAggregate(bucket1)
	fw.Flush()

	// Force rotation
	time.Sleep(1100 * time.Millisecond) // Ensure different filename
	if err := fw.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Write second bucket
	bucket2 := &AggregateBucket{
		Timestamp: time.Now(),
		Duration:  1 * time.Minute,
		Operations: map[string]*OperationStats{
			"test": {Count: 200, Min: 1000, Max: 3000, Sum: 300000},
		},
	}
	fw.WriteAggregate(bucket2)
	fw.Close()

	// Should have 2 files
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	aggregateCount := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "aggregate_") {
			aggregateCount++
		}
	}

	if aggregateCount < 2 {
		t.Errorf("Expected at least 2 aggregate files after rotation, got %d", aggregateCount)
	}
}

// TestStorageManager_Creation tests basic creation.
func TestStorageManager_Creation(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewStorageManager(tmpDir, 100<<20, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	if sm.GetBasePath() != tmpDir {
		t.Errorf("BasePath = %s, want %s", sm.GetBasePath(), tmpDir)
	}

	current, max, pct := sm.GetUsage()
	if max != 100<<20 {
		t.Errorf("Max = %d, want %d", max, 100<<20)
	}
	if current < 0 {
		t.Errorf("Current usage is negative: %d", current)
	}
	if pct < 0 || pct > 100 {
		t.Errorf("Percentage out of range: %f", pct)
	}
}

// TestStorageManager_Record tests recording data.
func TestStorageManager_Record(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewStorageManager(tmpDir, 100<<20, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	// Record some data
	for i := 0; i < 100; i++ {
		sm.Record(time.Now(), "test", int64(i*1000), true, "")
	}

	// Check raw buffer
	if sm.GetRawBuffer().Size() != 100 {
		t.Errorf("RawBuffer size = %d, want 100", sm.GetRawBuffer().Size())
	}

	// Check aggregator
	if sm.GetAggregator().BucketCount() < 1 {
		t.Error("Expected at least 1 aggregate bucket")
	}
}

// TestStorageManager_Limits tests limit detection.
func TestStorageManager_Limits(t *testing.T) {
	tmpDir := t.TempDir()

	// Create with very small max storage
	sm, err := NewStorageManager(tmpDir, 1000, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	// Initially not at limit
	if sm.IsAtLimit() {
		t.Error("Should not be at limit initially")
	}

	// Write some data to exceed limit
	for i := 0; i < 100; i++ {
		bucket := &AggregateBucket{
			Timestamp: time.Now(),
			Duration:  1 * time.Minute,
			Operations: map[string]*OperationStats{
				"test": {Count: int64(i), Min: 1000, Max: 2000, Sum: 100000},
			},
		}
		sm.fileWriter.WriteAggregate(bucket)
	}
	sm.fileWriter.Flush()

	// Update usage
	usage, _ := sm.calculateDiskUsage()
	sm.currentUsage.Store(usage)

	// Should be at limit now
	if !sm.IsAtLimit() {
		current, max, _ := sm.GetUsage()
		t.Errorf("Should be at limit: current=%d, max=%d", current, max)
	}
}

// TestStorageManager_Callbacks tests limit callbacks.
func TestStorageManager_Callbacks(t *testing.T) {
	tmpDir := t.TempDir()

	// Create with small max storage
	cfg := StorageManagerConfig{
		BasePath:        tmpDir,
		MaxStorage:      1000, // 1KB max
		RawRetention:    5 * time.Minute,
		AggregateInt:    1 * time.Minute,
		CleanupInterval: 1 * time.Hour, // Don't auto-cleanup
		FlushInterval:   1 * time.Hour, // Don't auto-flush
		MaxRawMemory:    1 << 20,
	}

	sm, err := NewStorageManagerWithConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	atLimitCalled := false

	sm.SetOnAtLimit(func() {
		atLimitCalled = true
	})

	// Write enough data to exceed limit
	for i := 0; i < 20; i++ {
		bucket := &AggregateBucket{
			Timestamp: time.Now(),
			Duration:  1 * time.Minute,
			Operations: map[string]*OperationStats{
				"test": {Count: int64(i * 100), Min: 1000, Max: 10000, Sum: 500000},
			},
		}
		sm.fileWriter.WriteAggregate(bucket)
	}
	sm.fileWriter.Flush()

	// Call cleanup which will recalculate usage and trigger callback
	ctx := context.Background()
	sm.cleanup(ctx)

	if !atLimitCalled {
		current, max, _ := sm.GetUsage()
		t.Errorf("OnAtLimit callback not called (current=%d, max=%d)", current, max)
	}
}

// TestStorageManager_IsAtLimit tests limit detection.
func TestStorageManager_IsAtLimit(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewStorageManager(tmpDir, 1000, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	// Manually set usage to test IsAtLimit logic
	sm.currentUsage.Store(500)
	if sm.IsAtLimit() {
		t.Error("Should not be at limit at 50%")
	}
	if sm.IsNearLimit() {
		t.Error("Should not be near limit at 50%")
	}

	sm.currentUsage.Store(910) // 91%
	if sm.IsAtLimit() {
		t.Error("Should not be at limit at 91%")
	}
	if !sm.IsNearLimit() {
		t.Error("Should be near limit at 91%")
	}

	sm.currentUsage.Store(1000) // 100%
	if !sm.IsAtLimit() {
		t.Error("Should be at limit at 100%")
	}

	sm.currentUsage.Store(1100) // 110%
	if !sm.IsAtLimit() {
		t.Error("Should be at limit at 110%")
	}
}

// TestStorageManager_StartStop tests start/stop lifecycle.
func TestStorageManager_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewStorageManager(tmpDir, 100<<20, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := sm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Record some data
	sm.Record(time.Now(), "test", 1000, true, "")

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		sm.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out")
	}
}

// TestCleanup_OldFiles tests cleanup of old files.
func TestCleanup_OldFiles(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewStorageManager(tmpDir, 100<<20, 5*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}
	defer sm.Stop()

	// Create some old files manually
	oldFile := filepath.Join(tmpDir, "aggregate_old.jsonl")
	if err := os.WriteFile(oldFile, []byte(`{"test": "data"}\n`), 0644); err != nil {
		t.Fatalf("Failed to create old file: %v", err)
	}

	// Set modification time to past
	oldTime := time.Now().Add(-24 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	// Cleanup files older than 1 hour
	count, bytes, err := sm.CleanupOlderThan(1 * time.Hour)
	if err != nil {
		t.Fatalf("CleanupOlderThan failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Removed %d files, want 1", count)
	}

	if bytes <= 0 {
		t.Error("Expected positive bytes removed")
	}

	// File should be gone
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("Old file should have been removed")
	}
}

// TestOperationStats_Percentiles tests percentile calculations.
func TestOperationStats_Percentiles(t *testing.T) {
	agg := NewAggregator(1 * time.Minute)

	now := time.Now()

	// Record many entries to get good percentile data
	for i := 1; i <= 100; i++ {
		agg.Record(now, "test", int64(i*1000), nil)
	}

	stats := agg.GetBucket(now).Operations["test"]

	p50 := stats.P50()
	p99 := stats.P99()

	// P50 should be around 50000 (50ms)
	if p50 < 40000 || p50 > 60000 {
		t.Errorf("P50 = %d, expected ~50000", p50)
	}

	// P99 should be around 99000 (99ms)
	if p99 < 90000 || p99 > 100000 {
		t.Errorf("P99 = %d, expected ~99000", p99)
	}
}

// TestFileWriter_JSONValidity tests that written JSON is valid.
func TestFileWriter_JSONValidity(t *testing.T) {
	tmpDir := t.TempDir()

	fw, err := NewFileWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileWriter: %v", err)
	}
	defer fw.Close()

	// Write multiple buckets
	for i := 0; i < 5; i++ {
		bucket := &AggregateBucket{
			Timestamp: time.Now().Add(time.Duration(i) * time.Minute),
			Duration:  1 * time.Minute,
			Operations: map[string]*OperationStats{
				"select": {Count: int64(i * 10), Min: 1000, Max: 5000, Sum: int64(i * 30000)},
				"insert": {Count: int64(i * 5), Min: 2000, Max: 8000, Sum: int64(i * 25000)},
			},
		}
		fw.WriteAggregate(bucket)
	}

	fw.Close()

	// Read file and verify each line is valid JSON
	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "aggregate_") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(tmpDir, e.Name()))
		if err != nil {
			t.Fatalf("Failed to read file: %v", err)
		}

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		for i, line := range lines {
			if line == "" {
				continue
			}

			var parsed map[string]interface{}
			if err := json.Unmarshal([]byte(line), &parsed); err != nil {
				t.Errorf("Line %d is invalid JSON: %v", i, err)
			}
		}
	}
}

// TestRawBuffer_Stats tests buffer statistics.
func TestRawBuffer_Stats(t *testing.T) {
	rb := NewRawBuffer(5*time.Minute, 1<<20)

	for i := 0; i < 50; i++ {
		rb.Add(RawEntry{
			Timestamp: time.Now(),
			Operation: "test",
			LatencyNs: int64(i),
			Success:   true,
		})
	}

	stats := rb.Stats()

	if stats.Size != 50 {
		t.Errorf("Stats.Size = %d, want 50", stats.Size)
	}

	if stats.Capacity <= 0 {
		t.Error("Stats.Capacity should be positive")
	}

	if stats.MemoryUsed <= 0 {
		t.Error("Stats.MemoryUsed should be positive")
	}
}
