package report

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{12345678, "12,345,678"},
		{123456789, "123,456,789"},
		{45230, "45,230"},
		{1000000, "1,000,000"},
		{-1234, "-1,234"},
		{-123456789, "-123,456,789"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatNumber(tt.input)
			if result != tt.expected {
				t.Errorf("formatNumber(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatNumberInt(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1234, "1,234"},
		{-5678, "-5,678"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatNumber(tt.input)
			if result != tt.expected {
				t.Errorf("formatNumber(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "500ms"},
		{1 * time.Second, "1s"},
		{30 * time.Second, "30s"},
		{60 * time.Second, "1m"},
		{90 * time.Second, "1m30s"},
		{5 * time.Minute, "5m"},
		{5*time.Minute + 30*time.Second, "5m30s"},
		{1 * time.Hour, "1h"},
		{1*time.Hour + 30*time.Minute, "1h30m"},
		{2*time.Hour + 15*time.Minute + 30*time.Second, "2h15m30s"},
		{24 * time.Hour, "24h"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatDuration(tt.input)
			if result != tt.expected {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.input)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"exactly10!", 10, "exactly10!"},
		{"this is too long", 10, "this is..."},
		{"ab", 2, "ab"},
		{"abc", 2, "ab"},
		{"abcd", 3, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := truncateString(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncateString(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
		})
	}
}

func TestConsoleFormatterNoColor(t *testing.T) {
	// Set NO_COLOR env var
	os.Setenv("NO_COLOR", "1")
	defer os.Unsetenv("NO_COLOR")

	formatter := NewConsoleFormatter()

	// Test that colors are disabled
	result := formatter.bold("test")
	if strings.Contains(result, "\033[") {
		t.Errorf("Expected no ANSI codes with NO_COLOR, got %q", result)
	}
	if result != "test" {
		t.Errorf("Expected 'test', got %q", result)
	}

	result = formatter.red("error")
	if strings.Contains(result, "\033[") {
		t.Errorf("Expected no ANSI codes with NO_COLOR, got %q", result)
	}
	if result != "error" {
		t.Errorf("Expected 'error', got %q", result)
	}
}

func TestConsoleFormatterWithColor(t *testing.T) {
	// Ensure NO_COLOR is not set
	os.Unsetenv("NO_COLOR")

	formatter := NewConsoleFormatter()

	// Test that colors are enabled
	result := formatter.bold("test")
	if !strings.Contains(result, "\033[1m") {
		t.Errorf("Expected bold ANSI code, got %q", result)
	}

	result = formatter.red("error")
	if !strings.Contains(result, "\033[31m") {
		t.Errorf("Expected red ANSI code, got %q", result)
	}
}

func TestConsoleFormatterColorizeErrorRate(t *testing.T) {
	os.Unsetenv("NO_COLOR")
	formatter := NewConsoleFormatter()

	tests := []struct {
		rate          float64
		expectedColor string
	}{
		{0.0, colorGreen},
		{0.05, colorGreen},
		{0.09, colorGreen},
		{0.1, colorYellow},
		{0.5, colorYellow},
		{0.99, colorYellow},
		{1.0, colorRed},
		{5.0, colorRed},
		{50.0, colorRed},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(formatNumber(int64(tt.rate*100)), ",", ""), func(t *testing.T) {
			result := formatter.colorizeErrorRate("test", tt.rate)
			if !strings.Contains(result, tt.expectedColor) {
				t.Errorf("colorizeErrorRate(%.2f) expected color %q, got %q", tt.rate, tt.expectedColor, result)
			}
		})
	}
}

func TestConsoleFormatterPrintSummaryNilReport(t *testing.T) {
	var buf bytes.Buffer
	formatter := NewConsoleFormatter().WithWriter(&buf).WithNoColor(true)

	// Should not panic with nil report
	formatter.PrintSummary(nil)

	if buf.Len() != 0 {
		t.Errorf("Expected no output for nil report, got %q", buf.String())
	}
}

func TestConsoleFormatterPrintSummaryEmptyReport(t *testing.T) {
	var buf bytes.Buffer
	formatter := NewConsoleFormatter().WithWriter(&buf).WithNoColor(true)

	// Create empty report
	report := &Report{
		Version: "1.0",
		RunInfo: RunInfo{
			StartTime: time.Now(),
			EndTime:   time.Now(),
			Duration:  5 * time.Minute,
			Mode:      "burst",
			Profile:   "oltp",
			Seed:      42,
			Workers:   4,
		},
		Summary: Summary{
			TotalQueries: 0,
			TotalErrors:  0,
			QPS:          0,
		},
		Latencies: make(map[string]*LatencyReport),
	}

	// Should not panic
	formatter.PrintSummary(report)

	output := buf.String()
	if !strings.Contains(output, "burst") {
		t.Errorf("Expected 'burst' in output, got %q", output)
	}
	if !strings.Contains(output, "oltp") {
		t.Errorf("Expected 'oltp' in output, got %q", output)
	}
}

func TestConsoleFormatterPrintSummaryWithData(t *testing.T) {
	var buf bytes.Buffer
	formatter := NewConsoleFormatter().
		WithWriter(&buf).
		WithNoColor(true).
		WithReportPath("/tmp/report.json")

	// Create report with data
	report := &Report{
		Version: "1.0",
		RunInfo: RunInfo{
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(5 * time.Minute),
			Duration:    5 * time.Minute,
			Mode:        "burst",
			Profile:     "oltp",
			Seed:        42,
			Workers:     4,
			Connections: 10,
		},
		Summary: Summary{
			TotalQueries:   45230,
			TotalErrors:    5,
			QPS:            150.77,
			ErrorRate:      0.011,
			SuccessRate:    99.989,
			ReadQueries:    30000,
			WriteQueries:   15230,
			ReadWriteRatio: 1.97,
		},
		Latencies: map[string]*LatencyReport{
			"select_account": {
				Operation: "select_account",
				Type:      "read",
				Count:     30000,
				QPS:       100.0,
				Mean:      time.Microsecond * 500,
				P50:       time.Microsecond * 450,
				P95:       time.Microsecond * 800,
				P99:       time.Microsecond * 1200,
				Max:       time.Microsecond * 5000,
			},
			"insert_transaction": {
				Operation: "insert_transaction",
				Type:      "write",
				Count:     15230,
				QPS:       50.77,
				Mean:      time.Microsecond * 800,
				P50:       time.Microsecond * 700,
				P95:       time.Microsecond * 1500,
				P99:       time.Microsecond * 2500,
				Max:       time.Microsecond * 10000,
			},
		},
	}

	formatter.PrintSummary(report)

	output := buf.String()

	// Check header
	if !strings.Contains(output, "pg_workload") {
		t.Error("Expected 'pg_workload' in output")
	}
	if !strings.Contains(output, "burst") {
		t.Error("Expected 'burst' in output")
	}
	if !strings.Contains(output, "oltp") {
		t.Error("Expected 'oltp' in output")
	}

	// Check summary values
	if !strings.Contains(output, "45,230") {
		t.Error("Expected formatted total queries '45,230' in output")
	}
	if !strings.Contains(output, "150.7") || !strings.Contains(output, "150.8") {
		// Allow some rounding
		if !strings.Contains(output, "150") {
			t.Error("Expected QPS around 150 in output")
		}
	}

	// Check latency table
	if !strings.Contains(output, "select_account") {
		t.Error("Expected 'select_account' in latency table")
	}
	if !strings.Contains(output, "insert_transac") || !strings.Contains(output, "insert_transaction") {
		// Might be truncated
		if !strings.Contains(output, "insert") {
			t.Error("Expected 'insert' operation in latency table")
		}
	}

	// Check footer
	if !strings.Contains(output, "/tmp/report.json") {
		t.Error("Expected report path in footer")
	}

	// Check box characters
	if !strings.Contains(output, "┌") {
		t.Error("Expected top-left box character")
	}
	if !strings.Contains(output, "└") {
		t.Error("Expected bottom-left box character")
	}
}

func TestConsoleFormatterVisibleLength(t *testing.T) {
	formatter := NewConsoleFormatter()

	tests := []struct {
		input    string
		expected int
	}{
		{"hello", 5},
		{"", 0},
		{"\033[1mhello\033[0m", 5},     // bold "hello"
		{"\033[31mred\033[0m", 3},       // red "red"
		{"\033[1m\033[31mbold red\033[0m", 8}, // bold red "bold red"
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := formatter.visibleLength(tt.input)
			if result != tt.expected {
				t.Errorf("visibleLength(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestConsoleFormatterSimulationReport(t *testing.T) {
	var buf bytes.Buffer
	formatter := NewConsoleFormatter().
		WithWriter(&buf).
		WithNoColor(true)

	report := &SimulationReport{
		Version: "1.0",
		RunInfo: RunInfo{
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(5 * time.Minute),
			Duration:    5 * time.Minute,
			Mode:        "simulation",
			Profile:     "oltp",
			Seed:        42,
			Workers:     4,
			Connections: 10,
		},
		SimulationInfo: SimulationInfo{
			TimeScale:         12,
			StartSimTime:      time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			EndSimTime:        time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			SimulatedDuration: 1 * time.Hour,
			RealDuration:      5 * time.Minute,
			ProfileUsed:       "oltp",
			ClockMode:         "simulated",
		},
		Summary: Summary{
			TotalQueries: 30000,
			TotalErrors:  10,
			QPS:          100.0,
			ErrorRate:    0.033,
		},
		Latencies:   make(map[string]*LatencyReport),
		StorageUsed: 1048576, // 1 MB
	}

	formatter.PrintSimulationSummary(report)

	output := buf.String()

	// Check simulation-specific info
	if !strings.Contains(output, "simulation") {
		t.Error("Expected 'simulation' in output")
	}
	if !strings.Contains(output, "12x") {
		t.Error("Expected '12x' time scale in output")
	}
	if !strings.Contains(output, "simulated") {
		t.Error("Expected 'simulated' clock mode in output")
	}
	if !strings.Contains(output, "1.0 MB") {
		t.Error("Expected storage used '1.0 MB' in output")
	}
}
