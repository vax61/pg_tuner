package events

import (
	"fmt"
	"time"
)

// ScheduledEvent represents a scheduled workload event.
type ScheduledEvent struct {
	Name             string          `yaml:"name" json:"name"`
	Schedule         string          `yaml:"schedule" json:"schedule"`                     // cron expression
	Duration         time.Duration   `yaml:"duration" json:"duration"`                     // event duration
	WorkloadOverride string          `yaml:"workload_override" json:"workload_override"`   // alternative profile
	Multiplier       float64         `yaml:"multiplier" json:"multiplier"`                 // override multiplier (0 = use WorkloadOverride)
	ReadWriteRatio   *ReadWriteRatio `yaml:"read_write_ratio" json:"read_write_ratio"`     // override R/W ratio (nil = keep)
	Priority         int             `yaml:"priority" json:"priority"`                     // priority for overlapping events
	Enabled          bool            `yaml:"enabled" json:"enabled"`
}

// ReadWriteRatio defines the read/write distribution.
type ReadWriteRatio struct {
	Read  int `yaml:"read" json:"read"`
	Write int `yaml:"write" json:"write"`
}

// ActiveEvent represents a currently active event.
type ActiveEvent struct {
	Event     *ScheduledEvent
	StartTime time.Time
	EndTime   time.Time
}

// Validate checks that the event configuration is valid.
func (e *ScheduledEvent) Validate() error {
	if e.Name == "" {
		return fmt.Errorf("event name is required")
	}

	if e.Schedule == "" {
		return fmt.Errorf("event schedule is required")
	}

	// Validate cron expression
	_, err := ParseCron(e.Schedule)
	if err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}

	if e.Duration <= 0 {
		return fmt.Errorf("event duration must be positive")
	}

	if e.Multiplier < 0 {
		return fmt.Errorf("multiplier cannot be negative")
	}

	if e.ReadWriteRatio != nil {
		if e.ReadWriteRatio.Read < 0 || e.ReadWriteRatio.Write < 0 {
			return fmt.Errorf("read/write ratio values cannot be negative")
		}
		if e.ReadWriteRatio.Read+e.ReadWriteRatio.Write == 0 {
			return fmt.Errorf("read/write ratio sum cannot be zero")
		}
	}

	return nil
}

// IsActive checks if the event is currently active given a time.
func (ae *ActiveEvent) IsActive(t time.Time) bool {
	return !t.Before(ae.StartTime) && t.Before(ae.EndTime)
}

// RemainingDuration returns the remaining duration of the active event.
func (ae *ActiveEvent) RemainingDuration(t time.Time) time.Duration {
	if t.After(ae.EndTime) || t.Equal(ae.EndTime) {
		return 0
	}
	return ae.EndTime.Sub(t)
}

// Copy creates a copy of the scheduled event.
func (e *ScheduledEvent) Copy() *ScheduledEvent {
	cp := *e
	if e.ReadWriteRatio != nil {
		rw := *e.ReadWriteRatio
		cp.ReadWriteRatio = &rw
	}
	return &cp
}

// String returns a string representation of the event.
func (e *ScheduledEvent) String() string {
	status := "disabled"
	if e.Enabled {
		status = "enabled"
	}
	return fmt.Sprintf("Event{name=%s, schedule=%s, duration=%v, %s}",
		e.Name, e.Schedule, e.Duration, status)
}

// String returns a string representation of the active event.
func (ae *ActiveEvent) String() string {
	return fmt.Sprintf("ActiveEvent{name=%s, start=%s, end=%s}",
		ae.Event.Name,
		ae.StartTime.Format("15:04:05"),
		ae.EndTime.Format("15:04:05"))
}
