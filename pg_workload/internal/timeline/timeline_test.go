package timeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewTimeline(t *testing.T) {
	tl := NewTimeline(time.Minute)

	if tl.Interval != time.Minute {
		t.Errorf("expected interval 1m, got %v", tl.Interval)
	}

	if tl.Len() != 0 {
		t.Errorf("expected empty timeline, got %d entries", tl.Len())
	}
}

func TestNewTimeline_DefaultInterval(t *testing.T) {
	tl := NewTimeline(0)

	if tl.Interval != time.Minute {
		t.Errorf("expected default interval 1m, got %v", tl.Interval)
	}
}

func TestTimeline_AddEntry(t *testing.T) {
	tl := NewTimeline(time.Minute)

	now := time.Now()
	entry := TimelineEntry{
		Timestamp:    now,
		TotalQueries: 100,
	}

	tl.AddEntry(entry)

	if tl.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", tl.Len())
	}

	if !tl.StartTime.Equal(now) {
		t.Errorf("expected start time %v, got %v", now, tl.StartTime)
	}

	if !tl.EndTime.Equal(now) {
		t.Errorf("expected end time %v, got %v", now, tl.EndTime)
	}
}

func TestTimeline_GetEntry(t *testing.T) {
	tl := NewTimeline(time.Minute)

	entry := TimelineEntry{
		Timestamp:    time.Now(),
		TotalQueries: 42,
	}

	tl.AddEntry(entry)

	// Valid index
	got, ok := tl.GetEntry(0)
	if !ok {
		t.Error("expected to get entry at index 0")
	}
	if got.TotalQueries != 42 {
		t.Errorf("expected 42 queries, got %d", got.TotalQueries)
	}

	// Invalid index
	_, ok = tl.GetEntry(1)
	if ok {
		t.Error("expected no entry at index 1")
	}

	_, ok = tl.GetEntry(-1)
	if ok {
		t.Error("expected no entry at index -1")
	}
}

func TestTimeline_GetEntries(t *testing.T) {
	tl := NewTimeline(time.Minute)

	for i := 0; i < 5; i++ {
		tl.AddEntry(TimelineEntry{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Minute),
			TotalQueries: int64(i + 1),
		})
	}

	entries := tl.GetEntries()

	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}

	// Verify it's a copy
	entries[0].TotalQueries = 999
	original, _ := tl.GetEntry(0)
	if original.TotalQueries == 999 {
		t.Error("GetEntries should return a copy")
	}
}

func TestTimeline_GetLastN(t *testing.T) {
	tl := NewTimeline(time.Minute)

	for i := 0; i < 10; i++ {
		tl.AddEntry(TimelineEntry{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Minute),
			TotalQueries: int64(i + 1),
		})
	}

	// Get last 3
	last3 := tl.GetLastN(3)
	if len(last3) != 3 {
		t.Errorf("expected 3 entries, got %d", len(last3))
	}
	if last3[0].TotalQueries != 8 {
		t.Errorf("expected first of last 3 to have 8 queries, got %d", last3[0].TotalQueries)
	}

	// Get more than available
	all := tl.GetLastN(20)
	if len(all) != 10 {
		t.Errorf("expected 10 entries, got %d", len(all))
	}
}

func TestTimeline_GetEntriesInRange(t *testing.T) {
	tl := NewTimeline(time.Minute)
	base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		tl.AddEntry(TimelineEntry{
			Timestamp:    base.Add(time.Duration(i) * time.Minute),
			TotalQueries: int64(i + 1),
		})
	}

	start := base.Add(2 * time.Minute)
	end := base.Add(5 * time.Minute)

	entries := tl.GetEntriesInRange(start, end)

	if len(entries) != 4 { // Minutes 2, 3, 4, 5
		t.Errorf("expected 4 entries in range, got %d", len(entries))
	}
}

func TestTimeline_Duration(t *testing.T) {
	tl := NewTimeline(time.Minute)

	// Empty timeline
	if tl.Duration() != 0 {
		t.Error("empty timeline should have 0 duration")
	}

	base := time.Now()
	tl.AddEntry(TimelineEntry{Timestamp: base})
	tl.AddEntry(TimelineEntry{Timestamp: base.Add(5 * time.Minute)})

	if tl.Duration() != 5*time.Minute {
		t.Errorf("expected 5m duration, got %v", tl.Duration())
	}
}

func TestTimeline_Clear(t *testing.T) {
	tl := NewTimeline(time.Minute)

	tl.AddEntry(TimelineEntry{Timestamp: time.Now()})
	tl.Clear()

	if tl.Len() != 0 {
		t.Errorf("expected empty timeline after clear, got %d", tl.Len())
	}

	if !tl.StartTime.IsZero() {
		t.Error("expected zero start time after clear")
	}
}

func TestCSVWriter_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.csv")

	writer, err := NewCSVWriter(path)
	if err != nil {
		t.Fatalf("failed to create CSV writer: %v", err)
	}

	// Write header
	if err := writer.WriteHeader(); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Write entries
	now := time.Now().Truncate(time.Second)
	entries := []TimelineEntry{
		{
			Timestamp:     now,
			SimulatedTime: now.Add(time.Hour),
			IntervalSec:   60,
			Multiplier:    1.5,
			TargetQPS:     100,
			ActualQPS:     95.5,
			TotalQueries:  1000,
			TotalErrors:   5,
			ActiveWorkers: 10,
			AvgLatencyUs:  5000,
			P50LatencyUs:  4000,
			P95LatencyUs:  8000,
			P99LatencyUs:  12000,
			ReadQueries:   800,
			WriteQueries:  200,
		},
		{
			Timestamp:     now.Add(time.Minute),
			SimulatedTime: now.Add(time.Hour + time.Minute),
			IntervalSec:   60,
			Multiplier:    2.0,
			TargetQPS:     200,
			ActualQPS:     190.0,
			TotalQueries:  2000,
			TotalErrors:   10,
			ActiveWorkers: 20,
			AvgLatencyUs:  6000,
			P50LatencyUs:  5000,
			P95LatencyUs:  10000,
			P99LatencyUs:  15000,
			ReadQueries:   1600,
			WriteQueries:  400,
		},
	}

	if err := writer.WriteAll(entries); err != nil {
		t.Fatalf("failed to write entries: %v", err)
	}

	if writer.Written() != 2 {
		t.Errorf("expected 2 written, got %d", writer.Written())
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back
	readEntries, err := ReadCSV(path)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if len(readEntries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(readEntries))
	}

	// Verify first entry
	if readEntries[0].TotalQueries != 1000 {
		t.Errorf("expected 1000 queries, got %d", readEntries[0].TotalQueries)
	}
	if readEntries[0].TargetQPS != 100 {
		t.Errorf("expected target QPS 100, got %d", readEntries[0].TargetQPS)
	}
	if readEntries[0].Multiplier != 1.5 {
		t.Errorf("expected multiplier 1.5, got %f", readEntries[0].Multiplier)
	}
}

func TestCSVWriter_Append(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "append.csv")

	// Create initial file
	writer, _ := NewCSVWriter(path)
	writer.WriteHeader()
	writer.WriteEntry(TimelineEntry{
		Timestamp:    time.Now(),
		TotalQueries: 100,
	})
	writer.Close()

	// Append
	appender, err := NewCSVWriterAppend(path)
	if err != nil {
		t.Fatalf("failed to open for append: %v", err)
	}
	appender.WriteEntry(TimelineEntry{
		Timestamp:    time.Now().Add(time.Minute),
		TotalQueries: 200,
	})
	appender.Close()

	// Read back
	entries, _ := ReadCSV(path)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries after append, got %d", len(entries))
	}
}

func TestCSVWriter_GetHeaders(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "headers.csv")

	writer, _ := NewCSVWriter(path)
	defer writer.Close()

	headers := writer.GetHeaders()
	if len(headers) != 15 {
		t.Errorf("expected 15 headers, got %d", len(headers))
	}

	if headers[0] != "timestamp" {
		t.Errorf("expected first header 'timestamp', got '%s'", headers[0])
	}
}

func TestStreamingTimeline(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "streaming.csv")

	st, err := NewStreamingTimeline(path, time.Minute, 5)
	if err != nil {
		t.Fatalf("failed to create streaming timeline: %v", err)
	}

	// Record entries
	now := time.Now()
	for i := 0; i < 10; i++ {
		err := st.Record(TimelineEntry{
			Timestamp:    now.Add(time.Duration(i) * time.Minute),
			TotalQueries: int64((i + 1) * 100),
		})
		if err != nil {
			t.Fatalf("failed to record entry %d: %v", i, err)
		}
	}

	// Check in-memory timeline
	if st.Len() != 10 {
		t.Errorf("expected 10 entries in timeline, got %d", st.Len())
	}

	// Check CSV written
	if st.Written() != 10 {
		t.Errorf("expected 10 written to CSV, got %d", st.Written())
	}

	written, err := st.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	if written != 10 {
		t.Errorf("Close returned %d written, expected 10", written)
	}

	// Verify file
	entries, _ := ReadCSV(path)
	if len(entries) != 10 {
		t.Errorf("expected 10 entries in CSV, got %d", len(entries))
	}
}

func TestStreamingTimeline_Flush(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "flush.csv")

	st, _ := NewStreamingTimeline(path, time.Minute, 100) // High flush threshold

	st.Record(TimelineEntry{Timestamp: time.Now(), TotalQueries: 1})

	// Manual flush
	if err := st.Flush(); err != nil {
		t.Errorf("flush failed: %v", err)
	}

	st.Close()
}

func TestIntervalCollector(t *testing.T) {
	ic := NewIntervalCollector(time.Minute, time.Now())
	ic.SetConfig(1.5, 100, 10)

	// Record some queries
	for i := 0; i < 100; i++ {
		isRead := i%3 != 0
		isError := i%20 == 0
		ic.RecordQuery(int64(1000+i*10), isRead, isError)
	}

	entry := ic.ToEntry()

	if entry.TotalQueries != 100 {
		t.Errorf("expected 100 queries, got %d", entry.TotalQueries)
	}

	if entry.TotalErrors != 5 { // 0, 20, 40, 60, 80
		t.Errorf("expected 5 errors, got %d", entry.TotalErrors)
	}

	if entry.Multiplier != 1.5 {
		t.Errorf("expected multiplier 1.5, got %f", entry.Multiplier)
	}

	if entry.TargetQPS != 100 {
		t.Errorf("expected target QPS 100, got %d", entry.TargetQPS)
	}

	if entry.ActiveWorkers != 10 {
		t.Errorf("expected 10 workers, got %d", entry.ActiveWorkers)
	}

	// Check read/write split
	// i%3 == 0 for i in 0-99: 0,3,6,...,99 = 34 values (writes)
	// i%3 != 0 for i in 0-99: 100 - 34 = 66 values (reads)
	expectedReads := int64(66)
	expectedWrites := int64(34)
	if entry.ReadQueries != expectedReads {
		t.Errorf("expected %d reads, got %d", expectedReads, entry.ReadQueries)
	}
	if entry.WriteQueries != expectedWrites {
		t.Errorf("expected %d writes, got %d", expectedWrites, entry.WriteQueries)
	}
}

func TestIntervalCollector_Reset(t *testing.T) {
	ic := NewIntervalCollector(time.Minute, time.Now())

	ic.RecordQuery(1000, true, false)
	ic.RecordQuery(2000, false, true)

	newSimTime := time.Now().Add(time.Hour)
	ic.Reset(newSimTime)

	if ic.GetTotalQueries() != 0 {
		t.Error("queries not reset")
	}

	if ic.GetTotalErrors() != 0 {
		t.Error("errors not reset")
	}
}

func TestTimelineCollector(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "collector.csv")

	tc, err := NewTimelineCollector(path, time.Minute, 5)
	if err != nil {
		t.Fatalf("failed to create collector: %v", err)
	}

	tc.SetConfig(1.0, 50, 5)

	// Record some queries
	for i := 0; i < 50; i++ {
		tc.RecordQuery(int64(1000+i*10), i%2 == 0, false)
	}

	// Take snapshot
	if err := tc.Snapshot(time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	// Record more for second interval
	tc.SetConfig(1.5, 75, 7)
	for i := 0; i < 75; i++ {
		tc.RecordQuery(int64(2000+i*10), true, false)
	}

	written, err := tc.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Should have 2 entries: one from snapshot, one from close
	if written != 2 {
		t.Errorf("expected 2 entries written, got %d", written)
	}

	// Verify file
	entries, _ := ReadCSV(path)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries in CSV, got %d", len(entries))
	}

	if entries[0].TotalQueries != 50 {
		t.Errorf("first interval should have 50 queries, got %d", entries[0].TotalQueries)
	}

	if entries[1].TotalQueries != 75 {
		t.Errorf("second interval should have 75 queries, got %d", entries[1].TotalQueries)
	}
}

func TestTimelineSummary(t *testing.T) {
	tl := NewTimeline(time.Minute)
	base := time.Now()

	entries := []TimelineEntry{
		{
			Timestamp:     base,
			TotalQueries:  100,
			TotalErrors:   2,
			ReadQueries:   80,
			WriteQueries:  20,
			ActualQPS:     10.0,
			TargetQPS:     10,
			AvgLatencyUs:  5000,
			P50LatencyUs:  4000,
			P95LatencyUs:  8000,
			P99LatencyUs:  12000,
			ActiveWorkers: 5,
		},
		{
			Timestamp:     base.Add(time.Minute),
			TotalQueries:  200,
			TotalErrors:   4,
			ReadQueries:   160,
			WriteQueries:  40,
			ActualQPS:     20.0,
			TargetQPS:     20,
			AvgLatencyUs:  6000,
			P50LatencyUs:  5000,
			P95LatencyUs:  10000,
			P99LatencyUs:  15000,
			ActiveWorkers: 10,
		},
		{
			Timestamp:     base.Add(2 * time.Minute),
			TotalQueries:  150,
			TotalErrors:   3,
			ReadQueries:   120,
			WriteQueries:  30,
			ActualQPS:     15.0,
			TargetQPS:     18, // Actual 15 is 83% of target, below 90%
			AvgLatencyUs:  5500,
			P50LatencyUs:  4500,
			P95LatencyUs:  9000,
			P99LatencyUs:  13500,
			ActiveWorkers: 8,
		},
	}

	for _, e := range entries {
		tl.AddEntry(e)
	}

	summary := tl.GetSummary()

	// Check totals
	if summary.TotalQueries != 450 {
		t.Errorf("expected 450 total queries, got %d", summary.TotalQueries)
	}

	if summary.TotalErrors != 9 {
		t.Errorf("expected 9 total errors, got %d", summary.TotalErrors)
	}

	if summary.TotalReadQueries != 360 {
		t.Errorf("expected 360 read queries, got %d", summary.TotalReadQueries)
	}

	if summary.TotalWriteQueries != 90 {
		t.Errorf("expected 90 write queries, got %d", summary.TotalWriteQueries)
	}

	// Check QPS stats
	expectedAvgQPS := 15.0 // (10 + 20 + 15) / 3
	if summary.AvgQPS != expectedAvgQPS {
		t.Errorf("expected avg QPS %f, got %f", expectedAvgQPS, summary.AvgQPS)
	}

	if summary.MinQPS != 10.0 {
		t.Errorf("expected min QPS 10, got %f", summary.MinQPS)
	}

	if summary.MaxQPS != 20.0 {
		t.Errorf("expected max QPS 20, got %f", summary.MaxQPS)
	}

	// Check worker stats
	if summary.MinWorkers != 5 {
		t.Errorf("expected min workers 5, got %d", summary.MinWorkers)
	}

	if summary.MaxWorkers != 10 {
		t.Errorf("expected max workers 10, got %d", summary.MaxWorkers)
	}

	// Check target hit rate (2 out of 3 hit target >= 90%)
	expectedHitRate := 66.66666666666666
	if summary.TargetHitRate < 66.0 || summary.TargetHitRate > 67.0 {
		t.Errorf("expected target hit rate ~66.67%%, got %f", summary.TargetHitRate)
	}
	_ = expectedHitRate

	// Check error rate
	expectedErrorRate := float64(9) / float64(450) * 100
	if summary.ErrorRate != expectedErrorRate {
		t.Errorf("expected error rate %f%%, got %f%%", expectedErrorRate, summary.ErrorRate)
	}
}

func TestTimelineSummary_Empty(t *testing.T) {
	tl := NewTimeline(time.Minute)
	summary := tl.GetSummary()

	if summary.Intervals != 0 {
		t.Errorf("expected 0 intervals, got %d", summary.Intervals)
	}

	if summary.TotalQueries != 0 {
		t.Errorf("expected 0 queries, got %d", summary.TotalQueries)
	}
}

func TestTimelineSummary_Format(t *testing.T) {
	tl := NewTimeline(time.Minute)
	tl.AddEntry(TimelineEntry{
		Timestamp:     time.Now(),
		TotalQueries:  1000,
		TotalErrors:   10,
		ActualQPS:     100.0,
		TargetQPS:     100,
		AvgLatencyUs:  5000,
		P50LatencyUs:  4000,
		P95LatencyUs:  8000,
		P99LatencyUs:  12000,
		ActiveWorkers: 10,
		ReadQueries:   800,
		WriteQueries:  200,
	})
	tl.AddEntry(TimelineEntry{
		Timestamp:     time.Now().Add(time.Minute),
		TotalQueries:  1000,
		TotalErrors:   10,
		ActualQPS:     100.0,
		TargetQPS:     100,
		AvgLatencyUs:  5000,
		P50LatencyUs:  4000,
		P95LatencyUs:  8000,
		P99LatencyUs:  12000,
		ActiveWorkers: 10,
		ReadQueries:   800,
		WriteQueries:  200,
	})

	summary := tl.GetSummary()
	formatted := summary.Format()

	if formatted == "" {
		t.Error("format returned empty string")
	}

	// Check it contains key info
	if !containsSubstring(formatted, "Total Queries") {
		t.Error("format missing Total Queries")
	}
	if !containsSubstring(formatted, "QPS") {
		t.Error("format missing QPS")
	}
	if !containsSubstring(formatted, "Latency") {
		t.Error("format missing Latency")
	}
}

func TestTimelineSummary_Format_Empty(t *testing.T) {
	tl := NewTimeline(time.Minute)
	summary := tl.GetSummary()
	formatted := summary.Format()

	if formatted != "No data collected" {
		t.Errorf("expected 'No data collected', got '%s'", formatted)
	}
}

func TestReadCSV_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.csv")

	// Create empty file
	f, _ := os.Create(path)
	f.Close()

	entries, err := ReadCSV(path)
	if err != nil {
		t.Errorf("unexpected error reading empty CSV: %v", err)
	}

	if entries != nil && len(entries) != 0 {
		t.Errorf("expected nil or empty entries for empty file, got %d", len(entries))
	}
}

func TestReadCSV_HeaderOnly(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "header.csv")

	writer, _ := NewCSVWriter(path)
	writer.WriteHeader()
	writer.Close()

	entries, err := ReadCSV(path)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if entries != nil && len(entries) != 0 {
		t.Errorf("expected nil or empty for header-only file")
	}
}

func TestReadCSV_InvalidRow(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.csv")

	f, _ := os.Create(path)
	f.WriteString("timestamp,simulated_time,interval_sec\n")
	f.WriteString("invalid,data,here\n")
	f.Close()

	_, err := ReadCSV(path)
	if err == nil {
		t.Error("expected error for invalid CSV row")
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
