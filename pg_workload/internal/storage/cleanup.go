package storage

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// cleanupLoop periodically monitors storage usage and cleans up old files.
func (sm *StorageManager) cleanupLoop(ctx context.Context) {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sm.done:
			return
		case <-ticker.C:
			sm.cleanup(ctx)
		}
	}
}

// cleanup checks storage usage and removes old files if necessary.
func (sm *StorageManager) cleanup(ctx context.Context) {
	// Update current usage
	usage, err := sm.calculateDiskUsage()
	if err != nil {
		return
	}
	sm.currentUsage.Store(usage)

	// Check limits and call callbacks
	sm.mu.RLock()
	onNearLimit := sm.onNearLimit
	onAtLimit := sm.onAtLimit
	sm.mu.RUnlock()

	if sm.IsAtLimit() {
		if onAtLimit != nil {
			onAtLimit()
		}
		// Must free space
		sm.freeSpace(ctx, 0.8) // Free down to 80%
	} else if sm.IsNearLimit() {
		if onNearLimit != nil {
			onNearLimit()
		}
		// Try to free some space proactively
		sm.freeSpace(ctx, 0.85) // Free down to 85%
	}
}

// freeSpace removes old files until usage is below targetPct of maxStorage.
func (sm *StorageManager) freeSpace(ctx context.Context, targetPct float64) {
	targetUsage := int64(float64(sm.maxStorage) * targetPct)

	// Get list of data files sorted by modification time (oldest first)
	files, err := sm.getDataFilesSorted()
	if err != nil {
		return
	}

	// Remove files until we're under target
	for _, fi := range files {
		select {
		case <-ctx.Done():
			return
		case <-sm.done:
			return
		default:
		}

		currentUsage := sm.currentUsage.Load()
		if currentUsage <= targetUsage {
			break
		}

		path := filepath.Join(sm.basePath, fi.name)

		// Don't delete the current active file
		if sm.isActiveFile(path) {
			continue
		}

		// Delete the file
		if err := os.Remove(path); err != nil {
			continue
		}

		// Update usage
		sm.currentUsage.Add(-fi.size)
	}
}

// fileInfo holds file information for sorting.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

// getDataFilesSorted returns data files sorted by modification time (oldest first).
func (sm *StorageManager) getDataFilesSorted() ([]fileInfo, error) {
	entries, err := os.ReadDir(sm.basePath)
	if err != nil {
		return nil, err
	}

	var files []fileInfo

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		// Only consider our data files
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, fileInfo{
			name:    name,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	// Sort by modification time, oldest first (LRU)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	return files, nil
}

// isActiveFile checks if a file is currently being written to.
func (sm *StorageManager) isActiveFile(path string) bool {
	stats := sm.fileWriter.GetStats()

	// Check aggregate file
	if stats.AggregateFile != "" {
		absAgg, _ := filepath.Abs(stats.AggregateFile)
		absPath, _ := filepath.Abs(path)
		if absAgg == absPath {
			return true
		}
	}

	// Check raw file
	if stats.RawFile != "" {
		absRaw, _ := filepath.Abs(stats.RawFile)
		absPath, _ := filepath.Abs(path)
		if absRaw == absPath {
			return true
		}
	}

	return false
}

// CleanupOlderThan removes files older than the specified duration.
func (sm *StorageManager) CleanupOlderThan(age time.Duration) (int, int64, error) {
	cutoff := time.Now().Add(-age)

	files, err := sm.getDataFilesSorted()
	if err != nil {
		return 0, 0, err
	}

	var removedCount int
	var removedBytes int64

	for _, fi := range files {
		if fi.modTime.After(cutoff) {
			continue // Keep files newer than cutoff
		}

		path := filepath.Join(sm.basePath, fi.name)

		if sm.isActiveFile(path) {
			continue
		}

		if err := os.Remove(path); err != nil {
			continue
		}

		removedCount++
		removedBytes += fi.size
		sm.currentUsage.Add(-fi.size)
	}

	return removedCount, removedBytes, nil
}

// ListDataFiles returns a list of all data files.
func (sm *StorageManager) ListDataFiles() ([]DataFileInfo, error) {
	files, err := sm.getDataFilesSorted()
	if err != nil {
		return nil, err
	}

	result := make([]DataFileInfo, len(files))
	for i, fi := range files {
		result[i] = DataFileInfo{
			Name:     fi.name,
			Path:     filepath.Join(sm.basePath, fi.name),
			Size:     fi.size,
			ModTime:  fi.modTime,
			IsActive: sm.isActiveFile(filepath.Join(sm.basePath, fi.name)),
		}
	}

	return result, nil
}

// DataFileInfo contains information about a data file.
type DataFileInfo struct {
	Name     string
	Path     string
	Size     int64
	ModTime  time.Time
	IsActive bool
}

// GetTotalFileCount returns the number of data files.
func (sm *StorageManager) GetTotalFileCount() int {
	files, err := sm.ListDataFiles()
	if err != nil {
		return 0
	}
	return len(files)
}

// ForceCleanup forces an immediate cleanup check.
func (sm *StorageManager) ForceCleanup(ctx context.Context) {
	sm.cleanup(ctx)
}
