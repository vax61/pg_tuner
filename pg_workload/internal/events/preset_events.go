package events

import (
	"fmt"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
)

// Preset scheduled events for common scenarios.

// NightlyBatchEvent runs a batch workload every night at 02:00.
var NightlyBatchEvent = &ScheduledEvent{
	Name:             "nightly_batch",
	Schedule:         "0 2 * * *", // Every day at 02:00
	Duration:         45 * time.Minute,
	WorkloadOverride: "batch_heavy",
	Multiplier:       2.0,
	Priority:         50,
	Enabled:          true,
}

// WeeklyReportEvent runs a read-heavy workload for report generation.
var WeeklyReportEvent = &ScheduledEvent{
	Name:     "weekly_report",
	Schedule: "0 6 * * 1", // Monday at 06:00
	Duration: 30 * time.Minute,
	Multiplier: 1.5,
	ReadWriteRatio: &ReadWriteRatio{
		Read:  90,
		Write: 10,
	},
	Priority: 40,
	Enabled:  true,
}

// MonthEndClose simulates month-end processing.
var MonthEndClose = &ScheduledEvent{
	Name:       "month_end_close",
	Schedule:   "0 22 L * *", // Last day of month at 22:00
	Duration:   3 * time.Hour,
	Multiplier: 1.8,
	Priority:   100, // High priority
	Enabled:    true,
}

// MorningPeakEvent simulates the morning login rush.
var MorningPeakEvent = &ScheduledEvent{
	Name:       "morning_peak",
	Schedule:   "0 8 * * 1-5", // Weekdays at 08:00
	Duration:   1 * time.Hour,
	Multiplier: 2.5,
	Priority:   30,
	Enabled:    true,
}

// LunchDipEvent simulates reduced load during lunch.
var LunchDipEvent = &ScheduledEvent{
	Name:       "lunch_dip",
	Schedule:   "0 12 * * 1-5", // Weekdays at 12:00
	Duration:   1 * time.Hour,
	Multiplier: 0.6,
	Priority:   20,
	Enabled:    true,
}

// AfternoonPeakEvent simulates the afternoon peak.
var AfternoonPeakEvent = &ScheduledEvent{
	Name:       "afternoon_peak",
	Schedule:   "0 14 * * 1-5", // Weekdays at 14:00
	Duration:   2 * time.Hour,
	Multiplier: 2.0,
	Priority:   30,
	Enabled:    true,
}

// WeekendMaintenanceEvent simulates weekend maintenance window.
var WeekendMaintenanceEvent = &ScheduledEvent{
	Name:       "weekend_maintenance",
	Schedule:   "0 3 * * 0", // Sunday at 03:00
	Duration:   2 * time.Hour,
	Multiplier: 0.5,
	Priority:   80,
	Enabled:    true,
}

// QuarterEndEvent simulates quarter-end heavy processing.
var QuarterEndEvent = &ScheduledEvent{
	Name:       "quarter_end",
	Schedule:   "0 20 L 3,6,9,12 *", // Last day of Mar, Jun, Sep, Dec at 20:00
	Duration:   4 * time.Hour,
	Multiplier: 2.5,
	ReadWriteRatio: &ReadWriteRatio{
		Read:  60,
		Write: 40,
	},
	Priority: 90,
	Enabled:  true,
}

// BackupWindowEvent simulates database backup window.
var BackupWindowEvent = &ScheduledEvent{
	Name:       "backup_window",
	Schedule:   "0 4 * * *", // Every day at 04:00
	Duration:   30 * time.Minute,
	Multiplier: 0.3, // Reduce load during backup
	ReadWriteRatio: &ReadWriteRatio{
		Read:  95,
		Write: 5,
	},
	Priority: 70,
	Enabled:  true,
}

// IndexRebuildEvent simulates periodic index maintenance.
var IndexRebuildEvent = &ScheduledEvent{
	Name:       "index_rebuild",
	Schedule:   "0 5 * * 0", // Sunday at 05:00
	Duration:   1 * time.Hour,
	Multiplier: 0.4,
	ReadWriteRatio: &ReadWriteRatio{
		Read:  20,
		Write: 80,
	},
	Priority: 60,
	Enabled:  true,
}

// presetEvents maps preset names to event definitions.
var presetEvents = map[string]*ScheduledEvent{
	"nightly_batch":        NightlyBatchEvent,
	"weekly_report":        WeeklyReportEvent,
	"month_end_close":      MonthEndClose,
	"morning_peak":         MorningPeakEvent,
	"lunch_dip":            LunchDipEvent,
	"afternoon_peak":       AfternoonPeakEvent,
	"weekend_maintenance":  WeekendMaintenanceEvent,
	"quarter_end":          QuarterEndEvent,
	"backup_window":        BackupWindowEvent,
	"index_rebuild":        IndexRebuildEvent,
}

// GetPresetEvent returns a copy of a preset event by name.
func GetPresetEvent(name string) (*ScheduledEvent, error) {
	event, exists := presetEvents[name]
	if !exists {
		return nil, fmt.Errorf("preset event not found: %s", name)
	}
	return event.Copy(), nil
}

// ListPresetEvents returns the names of all available preset events.
func ListPresetEvents() []string {
	names := make([]string, 0, len(presetEvents))
	for name := range presetEvents {
		names = append(names, name)
	}
	return names
}

// GetAllPresetEvents returns copies of all preset events.
func GetAllPresetEvents() []*ScheduledEvent {
	events := make([]*ScheduledEvent, 0, len(presetEvents))
	for _, event := range presetEvents {
		events = append(events, event.Copy())
	}
	return events
}

// GetPresetEventsByCategory returns preset events grouped by category.
func GetPresetEventsByCategory() map[string][]*ScheduledEvent {
	return map[string][]*ScheduledEvent{
		"daily": {
			NightlyBatchEvent.Copy(),
			BackupWindowEvent.Copy(),
		},
		"weekly": {
			WeeklyReportEvent.Copy(),
			WeekendMaintenanceEvent.Copy(),
			IndexRebuildEvent.Copy(),
		},
		"monthly": {
			MonthEndClose.Copy(),
		},
		"quarterly": {
			QuarterEndEvent.Copy(),
		},
		"workday": {
			MorningPeakEvent.Copy(),
			LunchDipEvent.Copy(),
			AfternoonPeakEvent.Copy(),
		},
	}
}

// CreateWorkdayProfile creates a scheduler with typical workday events.
func CreateWorkdayProfile(clk clock.Clock) *EventScheduler {
	es := NewEventScheduler(clk)

	es.AddEvent(MorningPeakEvent.Copy())
	es.AddEvent(LunchDipEvent.Copy())
	es.AddEvent(AfternoonPeakEvent.Copy())

	return es
}

// CreateFullProfile creates a scheduler with all preset events.
func CreateFullProfile(clk clock.Clock) *EventScheduler {
	es := NewEventScheduler(clk)

	for _, event := range presetEvents {
		es.AddEvent(event.Copy())
	}

	return es
}

// CreateMinimalProfile creates a scheduler with only essential events.
func CreateMinimalProfile(clk clock.Clock) *EventScheduler {
	es := NewEventScheduler(clk)

	es.AddEvent(NightlyBatchEvent.Copy())
	es.AddEvent(BackupWindowEvent.Copy())

	return es
}
