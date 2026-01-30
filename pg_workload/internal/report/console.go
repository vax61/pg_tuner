package report

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

// Box-drawing Unicode characters
const (
	boxHorizontal    = "─"
	boxVertical      = "│"
	boxTopLeft       = "┌"
	boxTopRight      = "┐"
	boxBottomLeft    = "└"
	boxBottomRight   = "┘"
	boxVerticalRight = "├"
	boxVerticalLeft  = "┤"
	boxHorizontalUp  = "┴"
	boxHorizontalDown = "┬"
	boxCross         = "┼"
)

// ConsoleFormatter formats reports for console output.
type ConsoleFormatter struct {
	writer      io.Writer
	noColor     bool
	reportPath  string
}

// NewConsoleFormatter creates a new console formatter.
func NewConsoleFormatter() *ConsoleFormatter {
	return &ConsoleFormatter{
		writer:  os.Stdout,
		noColor: os.Getenv("NO_COLOR") != "",
	}
}

// WithWriter sets a custom writer (useful for testing).
func (cf *ConsoleFormatter) WithWriter(w io.Writer) *ConsoleFormatter {
	cf.writer = w
	return cf
}

// WithReportPath sets the path to the JSON report file.
func (cf *ConsoleFormatter) WithReportPath(path string) *ConsoleFormatter {
	cf.reportPath = path
	return cf
}

// WithNoColor disables color output.
func (cf *ConsoleFormatter) WithNoColor(noColor bool) *ConsoleFormatter {
	cf.noColor = noColor
	return cf
}

// PrintSummary prints a formatted summary of the report.
func (cf *ConsoleFormatter) PrintSummary(report *Report) {
	if report == nil {
		return
	}

	cf.printHeader(report)
	cf.printSummarySection(report)
	cf.printLatencyTable(report)
	cf.printFooter()
}

// PrintSimulationSummary prints a formatted summary of a simulation report.
func (cf *ConsoleFormatter) PrintSimulationSummary(report *SimulationReport) {
	if report == nil {
		return
	}

	cf.printSimulationHeader(report)
	cf.printSimulationSummarySection(report)
	cf.printSimulationLatencyTable(report)
	cf.printFooter()
}

func (cf *ConsoleFormatter) printHeader(report *Report) {
	width := 70

	// Top border
	cf.println(cf.boxLine(boxTopLeft, boxHorizontal, boxTopRight, width))

	// Title
	title := " pg_workload - Benchmark Results "
	cf.println(cf.boxRow(cf.bold(cf.cyan(title)), width))

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Header info
	cf.println(cf.boxRow(fmt.Sprintf("  Mode: %s    Profile: %s",
		cf.bold(report.RunInfo.Mode),
		cf.bold(report.RunInfo.Profile)), width))

	cf.println(cf.boxRow(fmt.Sprintf("  Duration: %s    Seed: %d",
		cf.bold(formatDuration(report.RunInfo.Duration)),
		report.RunInfo.Seed), width))

	cf.println(cf.boxRow(fmt.Sprintf("  Workers: %d    Connections: %d",
		report.RunInfo.Workers,
		report.RunInfo.Connections), width))
}

func (cf *ConsoleFormatter) printSimulationHeader(report *SimulationReport) {
	width := 70

	// Top border
	cf.println(cf.boxLine(boxTopLeft, boxHorizontal, boxTopRight, width))

	// Title
	title := " pg_workload - Simulation Results "
	cf.println(cf.boxRow(cf.bold(cf.cyan(title)), width))

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Header info
	cf.println(cf.boxRow(fmt.Sprintf("  Mode: %s    Profile: %s",
		cf.bold(report.RunInfo.Mode),
		cf.bold(report.SimulationInfo.ProfileUsed)), width))

	cf.println(cf.boxRow(fmt.Sprintf("  Real Duration: %s    Simulated: %s",
		cf.bold(formatDuration(report.SimulationInfo.RealDuration)),
		cf.bold(formatDuration(report.SimulationInfo.SimulatedDuration))), width))

	cf.println(cf.boxRow(fmt.Sprintf("  Time Scale: %dx    Clock: %s",
		report.SimulationInfo.TimeScale,
		report.SimulationInfo.ClockMode), width))

	cf.println(cf.boxRow(fmt.Sprintf("  Workers: %d    Connections: %d    Seed: %d",
		report.RunInfo.Workers,
		report.RunInfo.Connections,
		report.RunInfo.Seed), width))
}

func (cf *ConsoleFormatter) printSummarySection(report *Report) {
	width := 70

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Section title
	cf.println(cf.boxRow(cf.bold("  Summary"), width))
	cf.println(cf.boxRow("", width))

	// Queries
	cf.println(cf.boxRow(fmt.Sprintf("  Total Queries:  %s",
		cf.bold(formatNumber(report.Summary.TotalQueries))), width))

	// Errors with color
	errorPct := report.Summary.ErrorRate
	errorStr := fmt.Sprintf("%.3f%%", errorPct)
	coloredError := cf.colorizeErrorRate(errorStr, errorPct)
	cf.println(cf.boxRow(fmt.Sprintf("  Total Errors:   %s (%s)",
		formatNumber(report.Summary.TotalErrors),
		coloredError), width))

	// Throughput
	cf.println(cf.boxRow(fmt.Sprintf("  Throughput:     %s QPS",
		cf.bold(fmt.Sprintf("%.1f", report.Summary.QPS))), width))

	// Read/Write breakdown
	cf.println(cf.boxRow(fmt.Sprintf("  Read Queries:   %s",
		formatNumber(report.Summary.ReadQueries)), width))
	cf.println(cf.boxRow(fmt.Sprintf("  Write Queries:  %s",
		formatNumber(report.Summary.WriteQueries)), width))

	if report.Summary.ReadWriteRatio > 0 {
		cf.println(cf.boxRow(fmt.Sprintf("  R/W Ratio:      %.2f:1",
			report.Summary.ReadWriteRatio), width))
	}
}

func (cf *ConsoleFormatter) printSimulationSummarySection(report *SimulationReport) {
	width := 70

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Section title
	cf.println(cf.boxRow(cf.bold("  Summary"), width))
	cf.println(cf.boxRow("", width))

	// Queries
	cf.println(cf.boxRow(fmt.Sprintf("  Total Queries:  %s",
		cf.bold(formatNumber(report.Summary.TotalQueries))), width))

	// Errors with color
	errorPct := report.Summary.ErrorRate
	errorStr := fmt.Sprintf("%.3f%%", errorPct)
	coloredError := cf.colorizeErrorRate(errorStr, errorPct)
	cf.println(cf.boxRow(fmt.Sprintf("  Total Errors:   %s (%s)",
		formatNumber(report.Summary.TotalErrors),
		coloredError), width))

	// Throughput
	cf.println(cf.boxRow(fmt.Sprintf("  Throughput:     %s QPS",
		cf.bold(fmt.Sprintf("%.1f", report.Summary.QPS))), width))

	// Read/Write breakdown
	cf.println(cf.boxRow(fmt.Sprintf("  Read Queries:   %s",
		formatNumber(report.Summary.ReadQueries)), width))
	cf.println(cf.boxRow(fmt.Sprintf("  Write Queries:  %s",
		formatNumber(report.Summary.WriteQueries)), width))

	// Events triggered
	if len(report.EventsTriggered) > 0 {
		cf.println(cf.boxRow(fmt.Sprintf("  Events:         %d triggered",
			len(report.EventsTriggered)), width))
	}

	// Storage used
	if report.StorageUsed > 0 {
		cf.println(cf.boxRow(fmt.Sprintf("  Storage Used:   %s",
			formatBytes(report.StorageUsed)), width))
	}
}

func (cf *ConsoleFormatter) printLatencyTable(report *Report) {
	cf.printLatencyTableFromMap(report.Latencies)
}

func (cf *ConsoleFormatter) printSimulationLatencyTable(report *SimulationReport) {
	cf.printLatencyTableFromMap(report.Latencies)
}

func (cf *ConsoleFormatter) printLatencyTableFromMap(latencies map[string]*LatencyReport) {
	width := 70

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Section title
	cf.println(cf.boxRow(cf.bold("  Latency (µs)"), width))
	cf.println(cf.boxRow("", width))

	if len(latencies) == 0 {
		cf.println(cf.boxRow("  No latency data available", width))
		return
	}

	// Table header
	header := fmt.Sprintf("  %-14s %8s %8s %8s %8s %8s",
		"Operation", "Avg", "p50", "p95", "p99", "Max")
	cf.println(cf.boxRow(cf.dim(header), width))

	// Separator line
	cf.println(cf.boxRow("  "+strings.Repeat("─", 62), width))

	// Sort operations for consistent output
	ops := make([]string, 0, len(latencies))
	for op := range latencies {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	// Table rows
	for _, op := range ops {
		lat := latencies[op]
		if lat == nil {
			continue
		}

		// Convert durations to microseconds
		avgUs := lat.Mean.Microseconds()
		p50Us := lat.P50.Microseconds()
		p95Us := lat.P95.Microseconds()
		p99Us := lat.P99.Microseconds()
		maxUs := lat.Max.Microseconds()

		row := fmt.Sprintf("  %-14s %8s %8s %8s %8s %8s",
			truncateString(op, 14),
			formatNumber(avgUs),
			formatNumber(p50Us),
			formatNumber(p95Us),
			formatNumber(p99Us),
			formatNumber(maxUs))
		cf.println(cf.boxRow(row, width))
	}
}

func (cf *ConsoleFormatter) printFooter() {
	width := 70

	// Separator
	cf.println(cf.boxLine(boxVerticalRight, boxHorizontal, boxVerticalLeft, width))

	// Report path
	if cf.reportPath != "" {
		cf.println(cf.boxRow(fmt.Sprintf("  Full report: %s", cf.dim(cf.reportPath)), width))
	}

	// Timestamp
	cf.println(cf.boxRow(fmt.Sprintf("  Generated: %s",
		cf.dim(time.Now().Format("2006-01-02 15:04:05"))), width))

	// Bottom border
	cf.println(cf.boxLine(boxBottomLeft, boxHorizontal, boxBottomRight, width))
}

// Helper methods for box drawing

func (cf *ConsoleFormatter) boxLine(left, fill, right string, width int) string {
	return left + strings.Repeat(fill, width-2) + right
}

func (cf *ConsoleFormatter) boxRow(content string, width int) string {
	// Calculate visible length (excluding ANSI codes)
	visibleLen := cf.visibleLength(content)
	padding := width - 2 - visibleLen
	if padding < 0 {
		padding = 0
	}
	return boxVertical + content + strings.Repeat(" ", padding) + boxVertical
}

func (cf *ConsoleFormatter) visibleLength(s string) int {
	// Remove ANSI escape sequences to calculate visible length
	inEscape := false
	length := 0
	for _, r := range s {
		if r == '\033' {
			inEscape = true
			continue
		}
		if inEscape {
			if r == 'm' {
				inEscape = false
			}
			continue
		}
		length++
	}
	return length
}

// Color helper methods

func (cf *ConsoleFormatter) colorize(s string, color string) string {
	if cf.noColor {
		return s
	}
	return color + s + colorReset
}

func (cf *ConsoleFormatter) bold(s string) string {
	return cf.colorize(s, colorBold)
}

func (cf *ConsoleFormatter) dim(s string) string {
	return cf.colorize(s, colorDim)
}

func (cf *ConsoleFormatter) green(s string) string {
	return cf.colorize(s, colorGreen)
}

func (cf *ConsoleFormatter) yellow(s string) string {
	return cf.colorize(s, colorYellow)
}

func (cf *ConsoleFormatter) red(s string) string {
	return cf.colorize(s, colorRed)
}

func (cf *ConsoleFormatter) cyan(s string) string {
	return cf.colorize(s, colorCyan)
}

func (cf *ConsoleFormatter) colorizeErrorRate(s string, rate float64) string {
	if rate < 0.1 {
		return cf.green(s)
	} else if rate < 1.0 {
		return cf.yellow(s)
	}
	return cf.red(s)
}

func (cf *ConsoleFormatter) println(s string) {
	fmt.Fprintln(cf.writer, s)
}

// Formatting helper functions

// formatNumber formats an integer with thousands separators.
// Example: 45230 -> "45,230"
func formatNumber[T int | int64](n T) string {
	if n < 0 {
		return "-" + formatNumber(-n)
	}

	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	remainder := len(str) % 3
	if remainder > 0 {
		result.WriteString(str[:remainder])
		if len(str) > remainder {
			result.WriteString(",")
		}
	}

	for i := remainder; i < len(str); i += 3 {
		if i > remainder {
			result.WriteString(",")
		}
		result.WriteString(str[i : i+3])
	}

	return result.String()
}

// formatDuration formats a duration in a human-readable way.
// Example: 5m0s, 1h30m, 2h0m0s -> "2h"
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Round(time.Millisecond).String()
	}

	d = d.Round(time.Second)

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		if minutes == 0 && seconds == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		if seconds == 0 {
			return fmt.Sprintf("%dh%dm", hours, minutes)
		}
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
	}

	if minutes > 0 {
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}

	return fmt.Sprintf("%ds", seconds)
}

// formatBytes formats bytes in human-readable format.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// truncateString truncates a string to maxLen, adding ellipsis if needed.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
