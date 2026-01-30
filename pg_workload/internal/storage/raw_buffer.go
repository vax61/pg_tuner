package storage

import (
	"sync"
	"time"
)

// RawEntry represents a single raw measurement.
type RawEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"operation"`
	LatencyNs int64     `json:"latency_ns"`
	Success   bool      `json:"success"`
	ErrorType string    `json:"error_type,omitempty"`
}

// RawBuffer is a rolling buffer for raw data entries.
// It maintains entries within a retention window and respects memory limits.
type RawBuffer struct {
	entries    []RawEntry
	maxEntries int
	retention  time.Duration

	head int // Next write position (circular buffer)
	tail int // Oldest entry position
	size int // Current number of entries

	mu sync.RWMutex
}

// estimatedEntrySize is the approximate memory size of a RawEntry.
const estimatedEntrySize = 128 // bytes

// NewRawBuffer creates a new RawBuffer.
func NewRawBuffer(retention time.Duration, maxMemory int64) *RawBuffer {
	if retention <= 0 {
		retention = 5 * time.Minute
	}
	if maxMemory <= 0 {
		maxMemory = 64 << 20 // 64MB default
	}

	maxEntries := int(maxMemory / estimatedEntrySize)
	if maxEntries < 1000 {
		maxEntries = 1000
	}

	return &RawBuffer{
		entries:    make([]RawEntry, maxEntries),
		maxEntries: maxEntries,
		retention:  retention,
	}
}

// Add adds a new entry to the buffer.
// If the buffer is full, the oldest entry is overwritten (FIFO).
func (rb *RawBuffer) Add(entry RawEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Write at head position
	rb.entries[rb.head] = entry
	rb.head = (rb.head + 1) % rb.maxEntries

	if rb.size < rb.maxEntries {
		rb.size++
	} else {
		// Buffer full, advance tail (overwrite oldest)
		rb.tail = (rb.tail + 1) % rb.maxEntries
	}
}

// Flush returns all entries that have exceeded the retention period and removes them.
func (rb *RawBuffer) Flush() []RawEntry {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return nil
	}

	cutoff := time.Now().Add(-rb.retention)
	var expired []RawEntry

	// Find and collect expired entries from tail
	for rb.size > 0 {
		entry := rb.entries[rb.tail]
		if entry.Timestamp.After(cutoff) {
			break // No more expired entries
		}

		expired = append(expired, entry)
		rb.tail = (rb.tail + 1) % rb.maxEntries
		rb.size--
	}

	return expired
}

// Prune removes entries older than retention period without returning them.
func (rb *RawBuffer) Prune() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return 0
	}

	cutoff := time.Now().Add(-rb.retention)
	pruned := 0

	for rb.size > 0 {
		entry := rb.entries[rb.tail]
		if entry.Timestamp.After(cutoff) {
			break
		}

		rb.tail = (rb.tail + 1) % rb.maxEntries
		rb.size--
		pruned++
	}

	return pruned
}

// GetAll returns all current entries (for reading, not removal).
func (rb *RawBuffer) GetAll() []RawEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	result := make([]RawEntry, rb.size)

	// Copy entries in order from tail to head
	for i := 0; i < rb.size; i++ {
		idx := (rb.tail + i) % rb.maxEntries
		result[i] = rb.entries[idx]
	}

	return result
}

// GetRecent returns the most recent n entries.
func (rb *RawBuffer) GetRecent(n int) []RawEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	if n > rb.size {
		n = rb.size
	}

	result := make([]RawEntry, n)

	// Start from head-n position
	start := (rb.head - n + rb.maxEntries) % rb.maxEntries

	for i := 0; i < n; i++ {
		idx := (start + i) % rb.maxEntries
		result[i] = rb.entries[idx]
	}

	return result
}

// GetInWindow returns entries within a time window.
func (rb *RawBuffer) GetInWindow(start, end time.Time) []RawEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	var result []RawEntry

	for i := 0; i < rb.size; i++ {
		idx := (rb.tail + i) % rb.maxEntries
		entry := rb.entries[idx]

		if (entry.Timestamp.Equal(start) || entry.Timestamp.After(start)) &&
			(entry.Timestamp.Equal(end) || entry.Timestamp.Before(end)) {
			result = append(result, entry)
		}
	}

	return result
}

// Size returns the current number of entries.
func (rb *RawBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the maximum number of entries.
func (rb *RawBuffer) Capacity() int {
	return rb.maxEntries
}

// IsFull returns true if the buffer is at capacity.
func (rb *RawBuffer) IsFull() bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size >= rb.maxEntries
}

// Clear removes all entries.
func (rb *RawBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.head = 0
	rb.tail = 0
	rb.size = 0
}

// Stats returns buffer statistics.
func (rb *RawBuffer) Stats() RawBufferStats {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	stats := RawBufferStats{
		Size:       rb.size,
		Capacity:   rb.maxEntries,
		Retention:  rb.retention,
		MemoryUsed: int64(rb.size * estimatedEntrySize),
	}

	if rb.size > 0 {
		stats.OldestEntry = rb.entries[rb.tail].Timestamp
		newestIdx := (rb.head - 1 + rb.maxEntries) % rb.maxEntries
		stats.NewestEntry = rb.entries[newestIdx].Timestamp
	}

	return stats
}

// RawBufferStats contains buffer statistics.
type RawBufferStats struct {
	Size        int
	Capacity    int
	Retention   time.Duration
	MemoryUsed  int64
	OldestEntry time.Time
	NewestEntry time.Time
}
