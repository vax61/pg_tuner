package timeline

import (
	"sync"
	"time"
)

// StreamingTimeline writes timeline entries incrementally to a CSV file.
type StreamingTimeline struct {
	timeline   *Timeline
	csvWriter  *CSVWriter
	flushEvery int
	unflushed  int
	mu         sync.Mutex
}

// NewStreamingTimeline creates a new streaming timeline writer.
func NewStreamingTimeline(path string, interval time.Duration, flushEvery int) (*StreamingTimeline, error) {
	csvWriter, err := NewCSVWriter(path)
	if err != nil {
		return nil, err
	}

	// Write header immediately
	if err := csvWriter.WriteHeader(); err != nil {
		csvWriter.Close()
		return nil, err
	}

	if flushEvery <= 0 {
		flushEvery = 10 // Default: flush every 10 entries
	}

	return &StreamingTimeline{
		timeline:   NewTimeline(interval),
		csvWriter:  csvWriter,
		flushEvery: flushEvery,
	}, nil
}

// Record adds a new entry to the timeline and writes it to the CSV file.
func (st *StreamingTimeline) Record(entry TimelineEntry) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Add to in-memory timeline
	st.timeline.AddEntry(entry)

	// Write to CSV
	if err := st.csvWriter.WriteEntry(entry); err != nil {
		return err
	}

	st.unflushed++

	// Flush periodically
	if st.unflushed >= st.flushEvery {
		if err := st.csvWriter.Flush(); err != nil {
			return err
		}
		st.unflushed = 0
	}

	return nil
}

// Flush forces a flush of the CSV writer.
func (st *StreamingTimeline) Flush() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.csvWriter.Flush(); err != nil {
		return err
	}
	st.unflushed = 0
	return nil
}

// Close flushes and closes the streaming timeline.
// Returns the number of entries written.
func (st *StreamingTimeline) Close() (int64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	written := st.csvWriter.Written()
	err := st.csvWriter.Close()

	return written, err
}

// GetTimeline returns the underlying timeline.
func (st *StreamingTimeline) GetTimeline() *Timeline {
	return st.timeline
}

// GetEntries returns all recorded entries.
func (st *StreamingTimeline) GetEntries() []TimelineEntry {
	return st.timeline.GetEntries()
}

// Len returns the number of entries.
func (st *StreamingTimeline) Len() int {
	return st.timeline.Len()
}

// Duration returns the total duration covered.
func (st *StreamingTimeline) Duration() time.Duration {
	return st.timeline.Duration()
}

// Written returns the number of entries written to the CSV file.
func (st *StreamingTimeline) Written() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.csvWriter.Written()
}

// GetSummary returns a summary of the timeline.
func (st *StreamingTimeline) GetSummary() *TimelineSummary {
	return st.timeline.GetSummary()
}
