package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/metrics"
)

func createTestSnapshot() *metrics.Snapshot {
	collector := metrics.NewCollector()

	// Record some test data
	for i := 0; i < 1000; i++ {
		collector.RecordLatency("point_select", int64(i+1)*1_000_000) // 1-1000ms
	}
	for i := 0; i < 500; i++ {
		collector.RecordLatency("range_select", int64(i+1)*2_000_000) // 2-1000ms
	}
	for i := 0; i < 300; i++ {
		collector.RecordLatency("insert_tx", int64(i+1)*3_000_000) // 3-900ms
	}
	for i := 0; i < 200; i++ {
		collector.RecordLatency("update_balance", int64(i+1)*4_000_000) // 4-800ms
	}

	// Record some errors
	for i := 0; i < 10; i++ {
		collector.IncrementError("insert_tx", "timeout")
	}
	for i := 0; i < 5; i++ {
		collector.IncrementError("update_balance", "deadlock")
	}

	// Wait a bit to get duration
	time.Sleep(10 * time.Millisecond)

	return collector.GetSnapshot()
}

func TestGenerateReport(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime:   time.Now().Add(-1 * time.Minute),
		EndTime:     time.Now(),
		Duration:    1 * time.Minute,
		Mode:        "burst",
		Profile:     "oltp_standard",
		Seed:        42,
		Workers:     4,
		Connections: 10,
	}

	report := GenerateReport(runInfo, snapshot)

	// Verify basic structure
	if report.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", report.Version)
	}

	// Verify run info
	if report.RunInfo.Mode != "burst" {
		t.Errorf("expected mode burst, got %s", report.RunInfo.Mode)
	}
	if report.RunInfo.Seed != 42 {
		t.Errorf("expected seed 42, got %d", report.RunInfo.Seed)
	}

	// Verify summary
	if report.Summary.TotalQueries != 2000 {
		t.Errorf("expected 2000 queries, got %d", report.Summary.TotalQueries)
	}
	if report.Summary.TotalErrors != 15 {
		t.Errorf("expected 15 errors, got %d", report.Summary.TotalErrors)
	}

	// Verify latencies exist
	if len(report.Latencies) != 4 {
		t.Errorf("expected 4 operations in latencies, got %d", len(report.Latencies))
	}

	// Verify specific operation
	pointSelect, ok := report.Latencies["point_select"]
	if !ok {
		t.Fatal("point_select not found in latencies")
	}
	if pointSelect.Count != 1000 {
		t.Errorf("expected 1000 point_select queries, got %d", pointSelect.Count)
	}
	if pointSelect.Type != "read" {
		t.Errorf("expected point_select type read, got %s", pointSelect.Type)
	}

	// Verify errors
	if report.Errors == nil {
		t.Fatal("expected errors to be populated")
	}
	if len(report.Errors) != 2 {
		t.Errorf("expected 2 operations with errors, got %d", len(report.Errors))
	}
}

func TestReportToJSON(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime:   time.Now().Add(-1 * time.Minute),
		EndTime:     time.Now(),
		Duration:    1 * time.Minute,
		Mode:        "burst",
		Profile:     "oltp_standard",
		Seed:        42,
		Workers:     4,
		Connections: 10,
	}

	report := GenerateReport(runInfo, snapshot)

	jsonData, err := report.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Verify expected fields exist
	if _, ok := parsed["version"]; !ok {
		t.Error("version field missing")
	}
	if _, ok := parsed["run_info"]; !ok {
		t.Error("run_info field missing")
	}
	if _, ok := parsed["summary"]; !ok {
		t.Error("summary field missing")
	}
	if _, ok := parsed["latencies"]; !ok {
		t.Error("latencies field missing")
	}
}

func TestReportToJSONCompact(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime:   time.Now(),
		EndTime:     time.Now(),
		Duration:    1 * time.Minute,
		Mode:        "burst",
		Profile:     "test",
		Seed:        1,
		Workers:     1,
		Connections: 1,
	}

	report := GenerateReport(runInfo, snapshot)

	compactJSON, err := report.ToJSONCompact()
	if err != nil {
		t.Fatalf("ToJSONCompact failed: %v", err)
	}

	indentedJSON, err := report.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Compact should be smaller
	if len(compactJSON) >= len(indentedJSON) {
		t.Errorf("compact JSON (%d bytes) should be smaller than indented (%d bytes)",
			len(compactJSON), len(indentedJSON))
	}

	// Both should parse to same structure
	var compact, indented map[string]interface{}
	json.Unmarshal(compactJSON, &compact)
	json.Unmarshal(indentedJSON, &indented)

	if compact["version"] != indented["version"] {
		t.Error("version mismatch between compact and indented")
	}
}

func TestReportWriteToFile(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime:   time.Now(),
		EndTime:     time.Now(),
		Duration:    1 * time.Minute,
		Mode:        "burst",
		Profile:     "test",
		Seed:        1,
		Workers:     1,
		Connections: 1,
	}

	report := GenerateReport(runInfo, snapshot)

	tmpFile := filepath.Join(t.TempDir(), "report.json")

	if err := report.WriteToFile(tmpFile); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	// Read and verify
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("file contains invalid JSON: %v", err)
	}
}

func TestReportWithSystemInfo(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Duration:  1 * time.Minute,
		Mode:      "burst",
		Profile:   "test",
	}

	report := GenerateReport(runInfo, snapshot)

	// Initially no system info
	if report.System != nil {
		t.Error("expected no system info initially")
	}

	// Add system info
	report.WithSystemInfo(&SystemInfo{
		PostgresVersion: "16.1",
		DatabaseName:    "testdb",
		HostInfo:        "localhost",
	})

	if report.System == nil {
		t.Fatal("expected system info after WithSystemInfo")
	}
	if report.System.PostgresVersion != "16.1" {
		t.Errorf("expected postgres version 16.1, got %s", report.System.PostgresVersion)
	}

	// Verify JSON includes system info
	jsonData, _ := report.ToJSON()
	var parsed map[string]interface{}
	json.Unmarshal(jsonData, &parsed)

	if _, ok := parsed["system"]; !ok {
		t.Error("system field missing from JSON")
	}
}

func TestReportString(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime: time.Now().Add(-1 * time.Minute),
		EndTime:   time.Now(),
		Duration:  1 * time.Minute,
		Mode:      "burst",
		Profile:   "test",
	}

	report := GenerateReport(runInfo, snapshot)

	str := report.String()
	if str == "" {
		t.Error("String() returned empty")
	}

	// Should contain key info
	if !containsStr(str, "queries") {
		t.Error("String() should mention queries")
	}
	if !containsStr(str, "QPS") {
		t.Error("String() should mention QPS")
	}
}

func TestReadWriteClassification(t *testing.T) {
	tests := []struct {
		op       string
		expected string
	}{
		{"point_select", "read"},
		{"range_select", "read"},
		{"complex_join", "read"},
		{"insert_tx", "write"},
		{"update_balance", "write"},
		{"unknown_op", "write"}, // default to write for safety
	}

	for _, tt := range tests {
		result := classifyOperation(tt.op)
		if result != tt.expected {
			t.Errorf("classifyOperation(%s) = %s, expected %s", tt.op, result, tt.expected)
		}
	}
}

func TestSummaryReadWriteRatio(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Duration:  1 * time.Minute,
	}

	report := GenerateReport(runInfo, snapshot)

	// We have 1000 point_select + 500 range_select = 1500 reads
	// And 300 insert_tx + 200 update_balance = 500 writes
	if report.Summary.ReadQueries != 1500 {
		t.Errorf("expected 1500 read queries, got %d", report.Summary.ReadQueries)
	}
	if report.Summary.WriteQueries != 500 {
		t.Errorf("expected 500 write queries, got %d", report.Summary.WriteQueries)
	}

	expectedRatio := 1500.0 / 500.0 // 3.0
	if report.Summary.ReadWriteRatio != expectedRatio {
		t.Errorf("expected read/write ratio %.2f, got %.2f", expectedRatio, report.Summary.ReadWriteRatio)
	}
}

func TestJSONLatencyValues(t *testing.T) {
	snapshot := createTestSnapshot()

	runInfo := RunInfo{
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Duration:  1 * time.Minute,
	}

	report := GenerateReport(runInfo, snapshot)

	jsonData, _ := report.ToJSON()

	var parsed struct {
		Latencies map[string]struct {
			Operation string  `json:"operation"`
			P50Ms     float64 `json:"p50_ms"`
			P99Ms     float64 `json:"p99_ms"`
		} `json:"latencies"`
	}

	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Verify latency values exist for point_select
	pointSelect, ok := parsed.Latencies["point_select"]
	if !ok {
		t.Fatal("point_select latencies missing")
	}
	if pointSelect.Operation != "point_select" {
		t.Errorf("expected operation 'point_select', got '%s'", pointSelect.Operation)
	}
	if pointSelect.P50Ms == 0 {
		t.Error("p50_ms value missing or zero")
	}
	if pointSelect.P99Ms == 0 {
		t.Error("p99_ms value missing or zero")
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
