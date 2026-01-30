package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileWriter writes aggregated and raw data to disk in JSON Lines format.
type FileWriter struct {
	basePath string

	aggregateFile   *os.File
	aggregateWriter *bufio.Writer
	aggregateBytes  int64

	rawFile   *os.File
	rawWriter *bufio.Writer
	rawBytes  int64

	maxFileSize int64 // Max size before rotation

	mu sync.Mutex
}

// DefaultMaxFileSize is the default maximum file size before rotation.
const DefaultMaxFileSize = 100 << 20 // 100MB

// NewFileWriter creates a new FileWriter.
func NewFileWriter(basePath string) (*FileWriter, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base path: %w", err)
	}

	fw := &FileWriter{
		basePath:    basePath,
		maxFileSize: DefaultMaxFileSize,
	}

	// Create initial aggregate file
	if err := fw.openAggregateFile(); err != nil {
		return nil, err
	}

	return fw, nil
}

// openAggregateFile opens a new aggregate file.
func (fw *FileWriter) openAggregateFile() error {
	filename := fmt.Sprintf("aggregate_%s.jsonl", time.Now().Format("20060102_150405"))
	path := filepath.Join(fw.basePath, filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open aggregate file: %w", err)
	}

	fw.aggregateFile = f
	fw.aggregateWriter = bufio.NewWriter(f)
	fw.aggregateBytes = 0

	return nil
}

// openRawFile opens a new raw file.
func (fw *FileWriter) openRawFile() error {
	filename := fmt.Sprintf("raw_%s.jsonl", time.Now().Format("20060102_150405"))
	path := filepath.Join(fw.basePath, filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open raw file: %w", err)
	}

	fw.rawFile = f
	fw.rawWriter = bufio.NewWriter(f)
	fw.rawBytes = 0

	return nil
}

// WriteAggregate writes an aggregate bucket to disk.
func (fw *FileWriter) WriteAggregate(bucket *AggregateBucket) error {
	if bucket == nil {
		return nil
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Check if rotation needed
	if fw.aggregateBytes >= fw.maxFileSize {
		if err := fw.rotateAggregate(); err != nil {
			return err
		}
	}

	// Convert to serializable format
	summary := bucket.ToSummary()

	// Serialize to JSON
	data, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregate: %w", err)
	}

	// Write line
	n, err := fw.aggregateWriter.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write aggregate: %w", err)
	}
	fw.aggregateBytes += int64(n)

	// Write newline
	if err := fw.aggregateWriter.WriteByte('\n'); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}
	fw.aggregateBytes++

	// Flush to ensure data is written
	return fw.aggregateWriter.Flush()
}

// WriteRaw writes raw entries to disk (optional, for debugging).
func (fw *FileWriter) WriteRaw(entries []RawEntry) error {
	if len(entries) == 0 {
		return nil
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Open raw file if needed
	if fw.rawFile == nil {
		if err := fw.openRawFile(); err != nil {
			return err
		}
	}

	// Check if rotation needed
	if fw.rawBytes >= fw.maxFileSize {
		if err := fw.rotateRaw(); err != nil {
			return err
		}
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			continue // Skip malformed entries
		}

		n, err := fw.rawWriter.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write raw entry: %w", err)
		}
		fw.rawBytes += int64(n)

		if err := fw.rawWriter.WriteByte('\n'); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		fw.rawBytes++
	}

	return fw.rawWriter.Flush()
}

// Rotate rotates both aggregate and raw files.
func (fw *FileWriter) Rotate() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if err := fw.rotateAggregate(); err != nil {
		return err
	}

	if fw.rawFile != nil {
		if err := fw.rotateRaw(); err != nil {
			return err
		}
	}

	return nil
}

// rotateAggregate rotates the aggregate file.
func (fw *FileWriter) rotateAggregate() error {
	// Flush and close current file
	if fw.aggregateWriter != nil {
		fw.aggregateWriter.Flush()
	}
	if fw.aggregateFile != nil {
		fw.aggregateFile.Close()
	}

	// Open new file
	return fw.openAggregateFile()
}

// rotateRaw rotates the raw file.
func (fw *FileWriter) rotateRaw() error {
	// Flush and close current file
	if fw.rawWriter != nil {
		fw.rawWriter.Flush()
	}
	if fw.rawFile != nil {
		fw.rawFile.Close()
	}

	// Open new file
	return fw.openRawFile()
}

// Close closes all open files.
func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	var errs []error

	if fw.aggregateWriter != nil {
		if err := fw.aggregateWriter.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if fw.aggregateFile != nil {
		if err := fw.aggregateFile.Close(); err != nil {
			errs = append(errs, err)
		}
		fw.aggregateFile = nil
	}

	if fw.rawWriter != nil {
		if err := fw.rawWriter.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if fw.rawFile != nil {
		if err := fw.rawFile.Close(); err != nil {
			errs = append(errs, err)
		}
		fw.rawFile = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Flush flushes all buffers to disk.
func (fw *FileWriter) Flush() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.aggregateWriter != nil {
		if err := fw.aggregateWriter.Flush(); err != nil {
			return err
		}
	}

	if fw.rawWriter != nil {
		if err := fw.rawWriter.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// GetStats returns file writer statistics.
func (fw *FileWriter) GetStats() FileWriterStats {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	stats := FileWriterStats{
		BasePath:        fw.basePath,
		MaxFileSize:     fw.maxFileSize,
		AggregateBytes:  fw.aggregateBytes,
		RawBytes:        fw.rawBytes,
		HasAggregateFile: fw.aggregateFile != nil,
		HasRawFile:      fw.rawFile != nil,
	}

	if fw.aggregateFile != nil {
		stats.AggregateFile = fw.aggregateFile.Name()
	}
	if fw.rawFile != nil {
		stats.RawFile = fw.rawFile.Name()
	}

	return stats
}

// SetMaxFileSize sets the maximum file size before rotation.
func (fw *FileWriter) SetMaxFileSize(size int64) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.maxFileSize = size
}

// FileWriterStats contains file writer statistics.
type FileWriterStats struct {
	BasePath         string
	MaxFileSize      int64
	AggregateFile    string
	AggregateBytes   int64
	RawFile          string
	RawBytes         int64
	HasAggregateFile bool
	HasRawFile       bool
}
