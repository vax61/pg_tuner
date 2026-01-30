package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/myorg/pg_tuner/pg_workload/internal/report"
)

var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Report analysis commands",
	Long:  "Analyze and compare benchmark reports: show formatted output, compare runs, export to different formats.",
}

// Report command flags
var reportCfg struct {
	Format   string // output format: text, json, yaml
	Output   string // output file
	Detailed bool   // show detailed breakdown
}

var reportShowCmd = &cobra.Command{
	Use:   "show <file>",
	Short: "Show formatted report",
	Long: `Display a benchmark report in formatted output.

Reads a JSON report file and displays it in a human-readable format.

Examples:
  pg_workload report show results.json
  pg_workload report show results.json --detailed
`,
	Args: cobra.ExactArgs(1),
	RunE: runReportShow,
}

var reportCompareCmd = &cobra.Command{
	Use:   "compare <file1> <file2>",
	Short: "Compare two reports",
	Long: `Compare two benchmark reports side by side.

Shows differences in throughput, latency, and error rates between runs.
Useful for A/B testing configuration changes.

Examples:
  pg_workload report compare baseline.json optimized.json
  pg_workload report compare run1.json run2.json --detailed
`,
	Args: cobra.ExactArgs(2),
	RunE: runReportCompare,
}

var reportExportCmd = &cobra.Command{
	Use:   "export <file>",
	Short: "Export report to different format",
	Long: `Export a report to CSV or HTML format.

Converts a JSON report to a format suitable for spreadsheets or web viewing.

Examples:
  pg_workload report export results.json --format csv --output results.csv
  pg_workload report export results.json --format html --output results.html
`,
	Args: cobra.ExactArgs(1),
	RunE: runReportExport,
}

func init() {
	// Add subcommands to report
	reportCmd.AddCommand(reportShowCmd)
	reportCmd.AddCommand(reportCompareCmd)
	reportCmd.AddCommand(reportExportCmd)

	// Show flags
	reportShowCmd.Flags().BoolVar(&reportCfg.Detailed, "detailed", false, "show detailed breakdown")

	// Compare flags
	reportCompareCmd.Flags().BoolVar(&reportCfg.Detailed, "detailed", false, "show detailed comparison")

	// Export flags
	reportExportCmd.Flags().StringVar(&reportCfg.Format, "format", "csv", "export format: csv, html")
	reportExportCmd.Flags().StringVarP(&reportCfg.Output, "output", "o", "", "output file (required)")
	reportExportCmd.MarkFlagRequired("output")
}

func runReportShow(cmd *cobra.Command, args []string) error {
	filename := args[0]

	rpt, err := loadReport(filename)
	if err != nil {
		return err
	}

	// Use the built-in console formatter
	formatter := report.NewConsoleFormatter().WithReportPath(filename)
	formatter.PrintSummary(rpt)

	if reportCfg.Detailed {
		fmt.Println()
		printDetailedReport(rpt)
	}

	return nil
}

// jsonReport mirrors the JSON structure with string durations
type jsonReport struct {
	Version   string                       `json:"version"`
	RunInfo   jsonRunInfo                  `json:"run_info"`
	Summary   report.Summary               `json:"summary"`
	Latencies map[string]*jsonLatencyEntry `json:"latencies"`
	Errors    map[string]*report.ErrorReport `json:"errors,omitempty"`
	System    *report.SystemInfo           `json:"system,omitempty"`
}

type jsonRunInfo struct {
	StartTime   string  `json:"start_time"`
	EndTime     string  `json:"end_time"`
	Duration    string  `json:"duration"`
	DurationSec float64 `json:"duration_sec"`
	Mode        string  `json:"mode"`
	Profile     string  `json:"profile"`
	Seed        int64   `json:"seed"`
	Workers     int     `json:"workers"`
	Connections int     `json:"connections"`
}

type jsonLatencyEntry struct {
	Operation string  `json:"operation"`
	Type      string  `json:"type"`
	Count     int64   `json:"count"`
	QPS       float64 `json:"qps"`
	MinMs     float64 `json:"min_ms"`
	MaxMs     float64 `json:"max_ms"`
	MeanMs    float64 `json:"mean_ms"`
	StdDevMs  float64 `json:"std_dev_ms"`
	P50Ms     float64 `json:"p50_ms"`
	P90Ms     float64 `json:"p90_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	P999Ms    float64 `json:"p999_ms"`
}

func loadReport(filename string) (*report.Report, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var jr jsonReport
	if err := json.Unmarshal(data, &jr); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}

	// Convert to report.Report
	rpt := &report.Report{
		Version: jr.Version,
		RunInfo: report.RunInfo{
			Mode:        jr.RunInfo.Mode,
			Profile:     jr.RunInfo.Profile,
			Seed:        jr.RunInfo.Seed,
			Workers:     jr.RunInfo.Workers,
			Connections: jr.RunInfo.Connections,
			Duration:    time.Duration(jr.RunInfo.DurationSec * float64(time.Second)),
		},
		Summary:   jr.Summary,
		Latencies: make(map[string]*report.LatencyReport),
		Errors:    jr.Errors,
		System:    jr.System,
	}

	// Parse times
	if jr.RunInfo.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, jr.RunInfo.StartTime); err == nil {
			rpt.RunInfo.StartTime = t
		}
	}
	if jr.RunInfo.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, jr.RunInfo.EndTime); err == nil {
			rpt.RunInfo.EndTime = t
		}
	}

	// Convert latencies from ms to Duration
	for name, jl := range jr.Latencies {
		rpt.Latencies[name] = &report.LatencyReport{
			Operation: jl.Operation,
			Type:      jl.Type,
			Count:     jl.Count,
			QPS:       jl.QPS,
			Min:       time.Duration(jl.MinMs * float64(time.Millisecond)),
			Max:       time.Duration(jl.MaxMs * float64(time.Millisecond)),
			Mean:      time.Duration(jl.MeanMs * float64(time.Millisecond)),
			StdDev:    time.Duration(jl.StdDevMs * float64(time.Millisecond)),
			P50:       time.Duration(jl.P50Ms * float64(time.Millisecond)),
			P90:       time.Duration(jl.P90Ms * float64(time.Millisecond)),
			P95:       time.Duration(jl.P95Ms * float64(time.Millisecond)),
			P99:       time.Duration(jl.P99Ms * float64(time.Millisecond)),
			P999:      time.Duration(jl.P999Ms * float64(time.Millisecond)),
		}
	}

	return rpt, nil
}

func printDetailedReport(rpt *report.Report) {
	fmt.Println("Detailed Latency Breakdown")
	fmt.Println("==========================")
	fmt.Println()

	// Sort operations by count
	type opLatency struct {
		name    string
		latency *report.LatencyReport
	}
	ops := make([]opLatency, 0, len(rpt.Latencies))
	for name, lat := range rpt.Latencies {
		ops = append(ops, opLatency{name, lat})
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].latency.Count > ops[j].latency.Count })

	for _, op := range ops {
		if op.latency.Count == 0 {
			continue
		}
		lat := op.latency
		fmt.Printf("%s (%s, %d queries, %.1f QPS)\n", op.name, lat.Type, lat.Count, lat.QPS)
		fmt.Printf("  Min:    %v\n", lat.Min)
		fmt.Printf("  Max:    %v\n", lat.Max)
		fmt.Printf("  Mean:   %v\n", lat.Mean)
		fmt.Printf("  StdDev: %v\n", lat.StdDev)
		fmt.Printf("  P50:    %v\n", lat.P50)
		fmt.Printf("  P90:    %v\n", lat.P90)
		fmt.Printf("  P95:    %v\n", lat.P95)
		fmt.Printf("  P99:    %v\n", lat.P99)
		fmt.Printf("  P999:   %v\n", lat.P999)
		fmt.Println()
	}

	// Errors
	if len(rpt.Errors) > 0 {
		fmt.Println("Error Breakdown")
		fmt.Println("===============")
		fmt.Println()

		for opName, errRpt := range rpt.Errors {
			fmt.Printf("%s: %d total errors\n", opName, errRpt.TotalCount)
			for errType, count := range errRpt.ByType {
				fmt.Printf("  - %s: %d\n", errType, count)
			}
		}
		fmt.Println()
	}
}

func runReportCompare(cmd *cobra.Command, args []string) error {
	file1, file2 := args[0], args[1]

	rpt1, err := loadReport(file1)
	if err != nil {
		return fmt.Errorf("loading %s: %w", file1, err)
	}

	rpt2, err := loadReport(file2)
	if err != nil {
		return fmt.Errorf("loading %s: %w", file2, err)
	}

	fmt.Println("Report Comparison")
	fmt.Println("=================")
	fmt.Println()
	fmt.Printf("Baseline: %s\n", filepath.Base(file1))
	fmt.Printf("Compare:  %s\n", filepath.Base(file2))
	fmt.Println()

	// Summary comparison
	fmt.Println("Summary")
	fmt.Println("-------")
	fmt.Printf("%-20s %15s %15s %15s\n", "Metric", "Baseline", "Compare", "Change")
	fmt.Println(strings.Repeat("-", 70))

	printCompareRow("Total Queries", float64(rpt1.Summary.TotalQueries), float64(rpt2.Summary.TotalQueries), "")
	printCompareRow("QPS", rpt1.Summary.QPS, rpt2.Summary.QPS, "")
	printCompareRow("Error Rate", rpt1.Summary.ErrorRate, rpt2.Summary.ErrorRate, "%")
	printCompareRow("Success Rate", rpt1.Summary.SuccessRate, rpt2.Summary.SuccessRate, "%")
	printCompareRow("Read Queries", float64(rpt1.Summary.ReadQueries), float64(rpt2.Summary.ReadQueries), "")
	printCompareRow("Write Queries", float64(rpt1.Summary.WriteQueries), float64(rpt2.Summary.WriteQueries), "")
	fmt.Println()

	// Latency comparison
	fmt.Println("Latency Comparison (P50)")
	fmt.Println("------------------------")
	fmt.Printf("%-25s %12s %12s %12s\n", "Operation", "Baseline", "Compare", "Change")
	fmt.Println(strings.Repeat("-", 65))

	// Get all operations from both reports
	allOps := make(map[string]bool)
	for name := range rpt1.Latencies {
		allOps[name] = true
	}
	for name := range rpt2.Latencies {
		allOps[name] = true
	}

	// Sort operation names
	opNames := make([]string, 0, len(allOps))
	for name := range allOps {
		opNames = append(opNames, name)
	}
	sort.Strings(opNames)

	for _, name := range opNames {
		lat1 := rpt1.Latencies[name]
		lat2 := rpt2.Latencies[name]

		var p50_1, p50_2 float64
		if lat1 != nil {
			p50_1 = float64(lat1.P50.Milliseconds())
		}
		if lat2 != nil {
			p50_2 = float64(lat2.P50.Milliseconds())
		}

		if p50_1 > 0 || p50_2 > 0 {
			printLatencyCompare(name, p50_1, p50_2)
		}
	}
	fmt.Println()

	if reportCfg.Detailed {
		// Detailed P99 comparison
		fmt.Println("Latency Comparison (P99)")
		fmt.Println("------------------------")
		fmt.Printf("%-25s %12s %12s %12s\n", "Operation", "Baseline", "Compare", "Change")
		fmt.Println(strings.Repeat("-", 65))

		for _, name := range opNames {
			lat1 := rpt1.Latencies[name]
			lat2 := rpt2.Latencies[name]

			var p99_1, p99_2 float64
			if lat1 != nil {
				p99_1 = float64(lat1.P99.Milliseconds())
			}
			if lat2 != nil {
				p99_2 = float64(lat2.P99.Milliseconds())
			}

			if p99_1 > 0 || p99_2 > 0 {
				printLatencyCompare(name, p99_1, p99_2)
			}
		}
		fmt.Println()
	}

	return nil
}

func printCompareRow(metric string, val1, val2 float64, suffix string) {
	var change string
	if val1 > 0 {
		pctChange := ((val2 - val1) / val1) * 100
		if pctChange > 0 {
			change = fmt.Sprintf("+%.1f%%", pctChange)
		} else {
			change = fmt.Sprintf("%.1f%%", pctChange)
		}
	} else {
		change = "N/A"
	}

	fmt.Printf("%-20s %15.2f%s %15.2f%s %15s\n", metric, val1, suffix, val2, suffix, change)
}

func printLatencyCompare(op string, val1, val2 float64) {
	var change string
	if val1 > 0 {
		pctChange := ((val2 - val1) / val1) * 100
		if pctChange > 0 {
			change = fmt.Sprintf("+%.1f%%", pctChange)
		} else {
			change = fmt.Sprintf("%.1f%%", pctChange)
		}
	} else if val2 > 0 {
		change = "NEW"
	} else {
		change = "N/A"
	}

	// Truncate operation name if too long
	if len(op) > 24 {
		op = op[:21] + "..."
	}

	fmt.Printf("%-25s %10.1fms %10.1fms %12s\n", op, val1, val2, change)
}

func runReportExport(cmd *cobra.Command, args []string) error {
	filename := args[0]

	rpt, err := loadReport(filename)
	if err != nil {
		return err
	}

	switch reportCfg.Format {
	case "csv":
		return exportCSV(rpt, reportCfg.Output)
	case "html":
		return exportHTML(rpt, reportCfg.Output)
	default:
		return fmt.Errorf("unsupported format: %s", reportCfg.Format)
	}
}

func exportCSV(rpt *report.Report, output string) error {
	f, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header
	headers := []string{
		"operation", "type", "count", "qps",
		"min_ms", "max_ms", "mean_ms", "stddev_ms",
		"p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms",
	}
	w.Write(headers)

	// Sort operations
	opNames := make([]string, 0, len(rpt.Latencies))
	for name := range rpt.Latencies {
		opNames = append(opNames, name)
	}
	sort.Strings(opNames)

	// Write data
	for _, name := range opNames {
		lat := rpt.Latencies[name]
		row := []string{
			name,
			lat.Type,
			fmt.Sprintf("%d", lat.Count),
			fmt.Sprintf("%.2f", lat.QPS),
			fmt.Sprintf("%.3f", float64(lat.Min.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.Max.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.Mean.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.StdDev.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.P50.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.P90.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.P95.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.P99.Microseconds())/1000),
			fmt.Sprintf("%.3f", float64(lat.P999.Microseconds())/1000),
		}
		w.Write(row)
	}

	fmt.Printf("Report exported to %s\n", output)
	return nil
}

func exportHTML(rpt *report.Report, output string) error {
	f, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()

	// Generate HTML
	html := `<!DOCTYPE html>
<html>
<head>
    <title>pg_workload Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px; }
        h1 { color: #333; }
        h2 { color: #666; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%%; margin: 10px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4a90d9; color: white; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #f5f5f5; }
        .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }
        .summary-card { background: #f8f9fa; border-radius: 8px; padding: 15px; text-align: center; }
        .summary-value { font-size: 24px; font-weight: bold; color: #333; }
        .summary-label { font-size: 12px; color: #666; }
        .read { color: #28a745; }
        .write { color: #dc3545; }
    </style>
</head>
<body>
    <h1>pg_workload Benchmark Report</h1>

    <h2>Run Information</h2>
    <table>
        <tr><th>Property</th><th>Value</th></tr>
        <tr><td>Mode</td><td>%s</td></tr>
        <tr><td>Profile</td><td>%s</td></tr>
        <tr><td>Duration</td><td>%s</td></tr>
        <tr><td>Workers</td><td>%d</td></tr>
        <tr><td>Connections</td><td>%d</td></tr>
    </table>

    <h2>Summary</h2>
    <div class="summary">
        <div class="summary-card">
            <div class="summary-value">%d</div>
            <div class="summary-label">Total Queries</div>
        </div>
        <div class="summary-card">
            <div class="summary-value">%.1f</div>
            <div class="summary-label">QPS</div>
        </div>
        <div class="summary-card">
            <div class="summary-value">%.2f%%</div>
            <div class="summary-label">Success Rate</div>
        </div>
        <div class="summary-card">
            <div class="summary-value">%.2f</div>
            <div class="summary-label">R/W Ratio</div>
        </div>
    </div>

    <h2>Latency by Operation</h2>
    <table>
        <tr>
            <th>Operation</th>
            <th>Type</th>
            <th>Count</th>
            <th>QPS</th>
            <th>Mean (ms)</th>
            <th>P50 (ms)</th>
            <th>P95 (ms)</th>
            <th>P99 (ms)</th>
            <th>Max (ms)</th>
        </tr>
`

	// Write header with run info
	fmt.Fprintf(f, html,
		rpt.RunInfo.Mode,
		rpt.RunInfo.Profile,
		rpt.RunInfo.Duration.String(),
		rpt.RunInfo.Workers,
		rpt.RunInfo.Connections,
		rpt.Summary.TotalQueries,
		rpt.Summary.QPS,
		rpt.Summary.SuccessRate,
		rpt.Summary.ReadWriteRatio,
	)

	// Sort operations
	opNames := make([]string, 0, len(rpt.Latencies))
	for name := range rpt.Latencies {
		opNames = append(opNames, name)
	}
	sort.Strings(opNames)

	// Write latency rows
	for _, name := range opNames {
		lat := rpt.Latencies[name]
		typeClass := "read"
		if lat.Type == "write" {
			typeClass = "write"
		}
		fmt.Fprintf(f, `        <tr>
            <td>%s</td>
            <td class="%s">%s</td>
            <td>%d</td>
            <td>%.2f</td>
            <td>%.2f</td>
            <td>%.2f</td>
            <td>%.2f</td>
            <td>%.2f</td>
            <td>%.2f</td>
        </tr>
`,
			name,
			typeClass, lat.Type,
			lat.Count,
			lat.QPS,
			float64(lat.Mean.Microseconds())/1000,
			float64(lat.P50.Microseconds())/1000,
			float64(lat.P95.Microseconds())/1000,
			float64(lat.P99.Microseconds())/1000,
			float64(lat.Max.Microseconds())/1000,
		)
	}

	// Close HTML
	fmt.Fprintln(f, `    </table>
</body>
</html>`)

	fmt.Printf("Report exported to %s\n", output)
	return nil
}
