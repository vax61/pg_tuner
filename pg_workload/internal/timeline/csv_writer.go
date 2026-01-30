package timeline

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// CSVWriter writes timeline entries to a CSV file.
type CSVWriter struct {
	file    *os.File
	writer  *csv.Writer
	headers []string
	written int64
	mu      sync.Mutex
}

// CSV headers for timeline export.
var defaultHeaders = []string{
	"timestamp",
	"simulated_time",
	"interval_sec",
	"multiplier",
	"target_qps",
	"actual_qps",
	"total_queries",
	"total_errors",
	"active_workers",
	"avg_latency_us",
	"p50_latency_us",
	"p95_latency_us",
	"p99_latency_us",
	"read_queries",
	"write_queries",
}

// NewCSVWriter creates a new CSV writer for the specified path.
func NewCSVWriter(path string) (*CSVWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	return &CSVWriter{
		file:    f,
		writer:  csv.NewWriter(f),
		headers: defaultHeaders,
	}, nil
}

// NewCSVWriterAppend opens an existing CSV file for appending.
func NewCSVWriterAppend(path string) (*CSVWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file for append: %w", err)
	}

	return &CSVWriter{
		file:    f,
		writer:  csv.NewWriter(f),
		headers: defaultHeaders,
	}, nil
}

// WriteHeader writes the CSV header row.
func (w *CSVWriter) WriteHeader() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Write(w.headers); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	w.writer.Flush()
	return w.writer.Error()
}

// WriteEntry writes a single timeline entry as a CSV row.
func (w *CSVWriter) WriteEntry(entry TimelineEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	row := entryToRow(entry)
	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	w.written++
	return nil
}

// WriteAll writes multiple timeline entries.
func (w *CSVWriter) WriteAll(entries []TimelineEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		row := entryToRow(entry)
		if err := w.writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
		w.written++
	}

	w.writer.Flush()
	return w.writer.Error()
}

// Flush flushes the CSV writer buffer to disk.
func (w *CSVWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close flushes and closes the CSV file.
func (w *CSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// Written returns the number of entries written.
func (w *CSVWriter) Written() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

// GetHeaders returns the CSV headers.
func (w *CSVWriter) GetHeaders() []string {
	return w.headers
}

// entryToRow converts a TimelineEntry to a CSV row.
func entryToRow(e TimelineEntry) []string {
	return []string{
		e.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
		e.SimulatedTime.Format("2006-01-02T15:04:05Z07:00"),
		strconv.Itoa(e.IntervalSec),
		strconv.FormatFloat(e.Multiplier, 'f', 4, 64),
		strconv.Itoa(e.TargetQPS),
		strconv.FormatFloat(e.ActualQPS, 'f', 2, 64),
		strconv.FormatInt(e.TotalQueries, 10),
		strconv.FormatInt(e.TotalErrors, 10),
		strconv.Itoa(e.ActiveWorkers),
		strconv.FormatInt(e.AvgLatencyUs, 10),
		strconv.FormatInt(e.P50LatencyUs, 10),
		strconv.FormatInt(e.P95LatencyUs, 10),
		strconv.FormatInt(e.P99LatencyUs, 10),
		strconv.FormatInt(e.ReadQueries, 10),
		strconv.FormatInt(e.WriteQueries, 10),
	}
}

// rowToEntry parses a CSV row back to a TimelineEntry.
func rowToEntry(row []string) (TimelineEntry, error) {
	if len(row) < 15 {
		return TimelineEntry{}, fmt.Errorf("row has %d columns, need 15", len(row))
	}

	var e TimelineEntry
	var err error

	e.Timestamp, err = parseTime(row[0])
	if err != nil {
		return e, fmt.Errorf("invalid timestamp: %w", err)
	}

	e.SimulatedTime, err = parseTime(row[1])
	if err != nil {
		return e, fmt.Errorf("invalid simulated_time: %w", err)
	}

	e.IntervalSec, err = strconv.Atoi(row[2])
	if err != nil {
		return e, fmt.Errorf("invalid interval_sec: %w", err)
	}

	e.Multiplier, err = strconv.ParseFloat(row[3], 64)
	if err != nil {
		return e, fmt.Errorf("invalid multiplier: %w", err)
	}

	e.TargetQPS, err = strconv.Atoi(row[4])
	if err != nil {
		return e, fmt.Errorf("invalid target_qps: %w", err)
	}

	e.ActualQPS, err = strconv.ParseFloat(row[5], 64)
	if err != nil {
		return e, fmt.Errorf("invalid actual_qps: %w", err)
	}

	e.TotalQueries, err = strconv.ParseInt(row[6], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid total_queries: %w", err)
	}

	e.TotalErrors, err = strconv.ParseInt(row[7], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid total_errors: %w", err)
	}

	e.ActiveWorkers, err = strconv.Atoi(row[8])
	if err != nil {
		return e, fmt.Errorf("invalid active_workers: %w", err)
	}

	e.AvgLatencyUs, err = strconv.ParseInt(row[9], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid avg_latency_us: %w", err)
	}

	e.P50LatencyUs, err = strconv.ParseInt(row[10], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid p50_latency_us: %w", err)
	}

	e.P95LatencyUs, err = strconv.ParseInt(row[11], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid p95_latency_us: %w", err)
	}

	e.P99LatencyUs, err = strconv.ParseInt(row[12], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid p99_latency_us: %w", err)
	}

	e.ReadQueries, err = strconv.ParseInt(row[13], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid read_queries: %w", err)
	}

	e.WriteQueries, err = strconv.ParseInt(row[14], 10, 64)
	if err != nil {
		return e, fmt.Errorf("invalid write_queries: %w", err)
	}

	return e, nil
}

// parseTime tries multiple time formats.
func parseTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s", s)
}

// ReadCSV reads a CSV file and returns timeline entries.
func ReadCSV(path string) ([]TimelineEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 2 {
		return nil, nil // Empty file or header only
	}

	// Skip header
	records = records[1:]

	entries := make([]TimelineEntry, 0, len(records))
	for i, row := range records {
		entry, err := rowToEntry(row)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+2, err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
