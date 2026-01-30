package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// StorageManager manages storage for long-running simulation data.
type StorageManager struct {
	basePath     string
	maxStorage   int64 // bytes max on disk
	currentUsage atomic.Int64

	rawRetention time.Duration // rolling window for raw data
	aggregateInt time.Duration // aggregation interval

	rawBuffer  *RawBuffer
	aggregator *Aggregator
	fileWriter *FileWriter

	cleanupInterval time.Duration
	flushInterval   time.Duration

	done    chan struct{}
	wg      sync.WaitGroup
	mu      sync.RWMutex
	started bool
	stopped bool

	// Callbacks for limit events
	onNearLimit func()
	onAtLimit   func()
}

// StorageManagerConfig contains configuration for StorageManager.
type StorageManagerConfig struct {
	BasePath        string
	MaxStorage      int64
	RawRetention    time.Duration
	AggregateInt    time.Duration
	CleanupInterval time.Duration
	FlushInterval   time.Duration
	MaxRawMemory    int64
}

// DefaultStorageManagerConfig returns default configuration.
func DefaultStorageManagerConfig() StorageManagerConfig {
	return StorageManagerConfig{
		BasePath:        "data/simulation",
		MaxStorage:      1 << 30, // 1GB
		RawRetention:    5 * time.Minute,
		AggregateInt:    1 * time.Minute,
		CleanupInterval: 30 * time.Second,
		FlushInterval:   10 * time.Second,
		MaxRawMemory:    64 << 20, // 64MB
	}
}

// NewStorageManager creates a new StorageManager.
func NewStorageManager(basePath string, maxStorage int64, rawRetention, aggInterval time.Duration) (*StorageManager, error) {
	cfg := DefaultStorageManagerConfig()
	cfg.BasePath = basePath
	cfg.MaxStorage = maxStorage
	cfg.RawRetention = rawRetention
	cfg.AggregateInt = aggInterval

	return NewStorageManagerWithConfig(cfg)
}

// NewStorageManagerWithConfig creates a new StorageManager with full configuration.
func NewStorageManagerWithConfig(cfg StorageManagerConfig) (*StorageManager, error) {
	// Validate config
	if cfg.MaxStorage <= 0 {
		return nil, fmt.Errorf("maxStorage must be positive")
	}
	if cfg.RawRetention <= 0 {
		cfg.RawRetention = 5 * time.Minute
	}
	if cfg.AggregateInt <= 0 {
		cfg.AggregateInt = 1 * time.Minute
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 30 * time.Second
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Second
	}
	if cfg.MaxRawMemory <= 0 {
		cfg.MaxRawMemory = 64 << 20
	}

	// Create base directory
	if err := os.MkdirAll(cfg.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Create file writer
	fw, err := NewFileWriter(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file writer: %w", err)
	}

	sm := &StorageManager{
		basePath:        cfg.BasePath,
		maxStorage:      cfg.MaxStorage,
		rawRetention:    cfg.RawRetention,
		aggregateInt:    cfg.AggregateInt,
		cleanupInterval: cfg.CleanupInterval,
		flushInterval:   cfg.FlushInterval,
		rawBuffer:       NewRawBuffer(cfg.RawRetention, cfg.MaxRawMemory),
		aggregator:      NewAggregator(cfg.AggregateInt),
		fileWriter:      fw,
		done:            make(chan struct{}),
	}

	// Calculate initial usage
	usage, err := sm.calculateDiskUsage()
	if err == nil {
		sm.currentUsage.Store(usage)
	}

	return sm, nil
}

// Start begins background goroutines for cleanup and flushing.
func (sm *StorageManager) Start(ctx context.Context) error {
	sm.mu.Lock()
	if sm.started || sm.stopped {
		sm.mu.Unlock()
		return nil
	}
	sm.started = true
	sm.mu.Unlock()

	// Start cleanup goroutine
	sm.wg.Add(1)
	go sm.cleanupLoop(ctx)

	// Start flush goroutine
	sm.wg.Add(1)
	go sm.flushLoop(ctx)

	return nil
}

// Stop stops the storage manager and flushes remaining data.
func (sm *StorageManager) Stop() error {
	sm.mu.Lock()
	if sm.stopped || !sm.started {
		sm.mu.Unlock()
		return nil
	}
	sm.stopped = true
	sm.mu.Unlock()

	// Signal goroutines to stop
	close(sm.done)

	// Wait for goroutines
	sm.wg.Wait()

	// Final flush
	sm.flushAll()

	// Close file writer
	return sm.fileWriter.Close()
}

// Record records a raw entry and aggregates it.
func (sm *StorageManager) Record(timestamp time.Time, operation string, latencyNs int64, success bool, errType string) {
	// Add to raw buffer
	entry := RawEntry{
		Timestamp: timestamp,
		Operation: operation,
		LatencyNs: latencyNs,
		Success:   success,
		ErrorType: errType,
	}
	sm.rawBuffer.Add(entry)

	// Record in aggregator
	var err error
	if !success {
		err = errors.New(errType)
	}
	sm.aggregator.Record(timestamp, operation, latencyNs, err)
}

// GetUsage returns current storage usage.
func (sm *StorageManager) GetUsage() (current int64, max int64, pct float64) {
	current = sm.currentUsage.Load()
	max = sm.maxStorage
	if max > 0 {
		pct = float64(current) / float64(max) * 100.0
	}
	return
}

// IsNearLimit returns true if usage is >90%.
func (sm *StorageManager) IsNearLimit() bool {
	current, max, _ := sm.GetUsage()
	return current > int64(float64(max)*0.9)
}

// IsAtLimit returns true if usage is >=100%.
func (sm *StorageManager) IsAtLimit() bool {
	current, max, _ := sm.GetUsage()
	return current >= max
}

// SetOnNearLimit sets callback for near limit event.
func (sm *StorageManager) SetOnNearLimit(fn func()) {
	sm.mu.Lock()
	sm.onNearLimit = fn
	sm.mu.Unlock()
}

// SetOnAtLimit sets callback for at limit event.
func (sm *StorageManager) SetOnAtLimit(fn func()) {
	sm.mu.Lock()
	sm.onAtLimit = fn
	sm.mu.Unlock()
}

// GetRawBuffer returns the raw buffer for direct access.
func (sm *StorageManager) GetRawBuffer() *RawBuffer {
	return sm.rawBuffer
}

// GetAggregator returns the aggregator for direct access.
func (sm *StorageManager) GetAggregator() *Aggregator {
	return sm.aggregator
}

// GetBasePath returns the base storage path.
func (sm *StorageManager) GetBasePath() string {
	return sm.basePath
}

// flushLoop periodically flushes data to disk.
func (sm *StorageManager) flushLoop(ctx context.Context) {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sm.done:
			return
		case <-ticker.C:
			sm.flushAll()
		}
	}
}

// flushAll flushes raw and aggregated data.
func (sm *StorageManager) flushAll() {
	// Prune old raw entries
	sm.rawBuffer.Prune()

	// Flush completed aggregate buckets
	buckets := sm.aggregator.FlushCompletedBuckets()
	for _, bucket := range buckets {
		if err := sm.fileWriter.WriteAggregate(bucket); err != nil {
			// Log error but continue
			continue
		}
	}

	// Update usage
	usage, err := sm.calculateDiskUsage()
	if err == nil {
		sm.currentUsage.Store(usage)
	}
}

// calculateDiskUsage calculates total disk usage in basePath.
func (sm *StorageManager) calculateDiskUsage() (int64, error) {
	var total int64

	err := filepath.Walk(sm.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})

	return total, err
}
