package controller

import (
	"sync"
	"sync/atomic"
	"time"
)

// ControllerMetrics tracks controller behavior over time.
type ControllerMetrics struct {
	Adjustments atomic.Int64

	// Target history (circular buffer)
	history      []TargetSnapshot
	historySize  int
	historyIndex int
	historyMu    sync.RWMutex
}

// TargetSnapshot captures controller state at a point in time.
type TargetSnapshot struct {
	Timestamp   time.Time
	Multiplier  float64
	TargetQPS   int
	TargetWorkers int
}

// newControllerMetrics creates a new ControllerMetrics.
func newControllerMetrics() *ControllerMetrics {
	return &ControllerMetrics{
		history:     make([]TargetSnapshot, 0, 1440), // 24h at 1min intervals
		historySize: 1440,
	}
}

// recordTarget records a new target snapshot.
func (m *ControllerMetrics) recordTarget(t time.Time, mult float64, qps, workers int) {
	m.Adjustments.Add(1)

	m.historyMu.Lock()
	defer m.historyMu.Unlock()

	snapshot := TargetSnapshot{
		Timestamp:     t,
		Multiplier:    mult,
		TargetQPS:     qps,
		TargetWorkers: workers,
	}

	if len(m.history) < m.historySize {
		m.history = append(m.history, snapshot)
	} else {
		m.history[m.historyIndex] = snapshot
		m.historyIndex = (m.historyIndex + 1) % m.historySize
	}
}

// GetAdjustments returns the total number of adjustments.
func (m *ControllerMetrics) GetAdjustments() int64 {
	return m.Adjustments.Load()
}

// GetTargetHistory returns the target history.
// Returns snapshots in chronological order.
func (m *ControllerMetrics) GetTargetHistory() []TargetSnapshot {
	m.historyMu.RLock()
	defer m.historyMu.RUnlock()

	if len(m.history) < m.historySize {
		// Not wrapped yet, just copy
		result := make([]TargetSnapshot, len(m.history))
		copy(result, m.history)
		return result
	}

	// Wrapped, need to reorder
	result := make([]TargetSnapshot, m.historySize)
	// Copy from current index to end
	copy(result, m.history[m.historyIndex:])
	// Copy from start to current index
	copy(result[m.historySize-m.historyIndex:], m.history[:m.historyIndex])
	return result
}

// GetRecentHistory returns the last n snapshots.
func (m *ControllerMetrics) GetRecentHistory(n int) []TargetSnapshot {
	history := m.GetTargetHistory()
	if len(history) <= n {
		return history
	}
	return history[len(history)-n:]
}

// GetLastSnapshot returns the most recent snapshot.
func (m *ControllerMetrics) GetLastSnapshot() (TargetSnapshot, bool) {
	m.historyMu.RLock()
	defer m.historyMu.RUnlock()

	if len(m.history) == 0 {
		return TargetSnapshot{}, false
	}

	if len(m.history) < m.historySize {
		return m.history[len(m.history)-1], true
	}

	// Wrapped, last written is at index-1
	idx := m.historyIndex - 1
	if idx < 0 {
		idx = m.historySize - 1
	}
	return m.history[idx], true
}

// GetStats returns a summary of controller metrics.
func (m *ControllerMetrics) GetStats() MetricsStats {
	history := m.GetTargetHistory()

	stats := MetricsStats{
		TotalAdjustments: m.Adjustments.Load(),
		HistoryLength:    len(history),
	}

	if len(history) > 0 {
		var sumMult, sumQPS float64
		stats.MinMultiplier = history[0].Multiplier
		stats.MaxMultiplier = history[0].Multiplier
		stats.MinQPS = history[0].TargetQPS
		stats.MaxQPS = history[0].TargetQPS

		for _, s := range history {
			sumMult += s.Multiplier
			sumQPS += float64(s.TargetQPS)

			if s.Multiplier < stats.MinMultiplier {
				stats.MinMultiplier = s.Multiplier
			}
			if s.Multiplier > stats.MaxMultiplier {
				stats.MaxMultiplier = s.Multiplier
			}
			if s.TargetQPS < stats.MinQPS {
				stats.MinQPS = s.TargetQPS
			}
			if s.TargetQPS > stats.MaxQPS {
				stats.MaxQPS = s.TargetQPS
			}
		}

		stats.AvgMultiplier = sumMult / float64(len(history))
		stats.AvgQPS = sumQPS / float64(len(history))
	}

	return stats
}

// MetricsStats contains summarized metrics statistics.
type MetricsStats struct {
	TotalAdjustments int64
	HistoryLength    int
	MinMultiplier    float64
	MaxMultiplier    float64
	AvgMultiplier    float64
	MinQPS           int
	MaxQPS           int
	AvgQPS           float64
}

// Reset clears all metrics.
func (m *ControllerMetrics) Reset() {
	m.Adjustments.Store(0)

	m.historyMu.Lock()
	defer m.historyMu.Unlock()

	m.history = m.history[:0]
	m.historyIndex = 0
}
