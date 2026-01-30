package events

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// EventsConfig represents the YAML structure for scheduled events.
type EventsConfig struct {
	ScheduledEvents []EventYAML `yaml:"scheduled_events"`
}

// EventYAML represents a scheduled event in YAML format.
type EventYAML struct {
	Name             string         `yaml:"name"`
	Schedule         string         `yaml:"schedule"`
	Duration         string         `yaml:"duration"`
	WorkloadOverride string         `yaml:"workload_override,omitempty"`
	Multiplier       float64        `yaml:"multiplier,omitempty"`
	ReadWriteRatio   *RatioYAML     `yaml:"read_write_ratio,omitempty"`
	Priority         int            `yaml:"priority,omitempty"`
	Enabled          *bool          `yaml:"enabled,omitempty"`
}

// RatioYAML represents read/write ratio in YAML format.
type RatioYAML struct {
	Read  int `yaml:"read"`
	Write int `yaml:"write"`
}

// ParseEventsFromYAML parses scheduled events from YAML data.
func ParseEventsFromYAML(data []byte) ([]*ScheduledEvent, error) {
	var config EventsConfig

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	events := make([]*ScheduledEvent, 0, len(config.ScheduledEvents))

	for i, ey := range config.ScheduledEvents {
		event, err := eventYAMLToScheduledEvent(&ey)
		if err != nil {
			return nil, fmt.Errorf("event %d (%s): %w", i, ey.Name, err)
		}
		events = append(events, event)
	}

	return events, nil
}

// ParseEventsFromFile parses scheduled events from a YAML file.
func ParseEventsFromFile(path string) ([]*ScheduledEvent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return ParseEventsFromYAML(data)
}

// eventYAMLToScheduledEvent converts a YAML event to a ScheduledEvent.
func eventYAMLToScheduledEvent(ey *EventYAML) (*ScheduledEvent, error) {
	if ey.Name == "" {
		return nil, fmt.Errorf("event name is required")
	}

	if ey.Schedule == "" {
		return nil, fmt.Errorf("event schedule is required")
	}

	duration, err := parseDuration(ey.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid duration: %w", err)
	}

	event := &ScheduledEvent{
		Name:             ey.Name,
		Schedule:         ey.Schedule,
		Duration:         duration,
		WorkloadOverride: ey.WorkloadOverride,
		Multiplier:       ey.Multiplier,
		Priority:         ey.Priority,
		Enabled:          true, // Default to enabled
	}

	// Handle explicit enabled flag
	if ey.Enabled != nil {
		event.Enabled = *ey.Enabled
	}

	// Convert read/write ratio
	if ey.ReadWriteRatio != nil {
		event.ReadWriteRatio = &ReadWriteRatio{
			Read:  ey.ReadWriteRatio.Read,
			Write: ey.ReadWriteRatio.Write,
		}
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, err
	}

	return event, nil
}

// parseDuration parses a duration string with support for common formats.
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("duration is required")
	}

	// Try standard Go duration parsing
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}

	// Try parsing as minutes/hours if no unit
	return 0, fmt.Errorf("invalid duration format: %s", s)
}

// EventsToYAML converts scheduled events to YAML format.
func EventsToYAML(events []*ScheduledEvent) ([]byte, error) {
	config := EventsConfig{
		ScheduledEvents: make([]EventYAML, len(events)),
	}

	for i, event := range events {
		config.ScheduledEvents[i] = scheduledEventToYAML(event)
	}

	return yaml.Marshal(&config)
}

// scheduledEventToYAML converts a ScheduledEvent to YAML format.
func scheduledEventToYAML(event *ScheduledEvent) EventYAML {
	ey := EventYAML{
		Name:             event.Name,
		Schedule:         event.Schedule,
		Duration:         event.Duration.String(),
		WorkloadOverride: event.WorkloadOverride,
		Multiplier:       event.Multiplier,
		Priority:         event.Priority,
	}

	// Only include enabled if explicitly false
	if !event.Enabled {
		enabled := false
		ey.Enabled = &enabled
	}

	if event.ReadWriteRatio != nil {
		ey.ReadWriteRatio = &RatioYAML{
			Read:  event.ReadWriteRatio.Read,
			Write: event.ReadWriteRatio.Write,
		}
	}

	return ey
}

// ProfileWithEvents represents a simulation profile that includes scheduled events.
type ProfileWithEvents struct {
	// Standard profile fields would be embedded here
	ScheduledEvents []*ScheduledEvent `yaml:"scheduled_events"`
}

// ExtractEventsFromProfile extracts scheduled events from a profile YAML.
// This is useful when events are embedded in a larger profile document.
func ExtractEventsFromProfile(data []byte) ([]*ScheduledEvent, error) {
	var profile ProfileWithEvents

	if err := yaml.Unmarshal(data, &profile); err != nil {
		return nil, fmt.Errorf("failed to parse profile YAML: %w", err)
	}

	if len(profile.ScheduledEvents) == 0 {
		// Try parsing as direct events config
		return ParseEventsFromYAML(data)
	}

	// Validate all events
	for _, event := range profile.ScheduledEvents {
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("event %s: %w", event.Name, err)
		}
	}

	return profile.ScheduledEvents, nil
}

// MergeEventsFromYAML merges events from YAML into an existing scheduler.
func MergeEventsFromYAML(es *EventScheduler, data []byte) error {
	events, err := ParseEventsFromYAML(data)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := es.AddEvent(event); err != nil {
			return fmt.Errorf("failed to add event %s: %w", event.Name, err)
		}
	}

	return nil
}

// ExampleYAML returns an example YAML configuration for documentation.
func ExampleYAML() string {
	return `scheduled_events:
  - name: "nightly_batch"
    schedule: "0 2 * * *"
    duration: 45m
    workload_override: batch_heavy
    multiplier: 2.0

  - name: "weekend_maintenance"
    schedule: "0 3 * * 0"
    duration: 2h
    multiplier: 0.5
    enabled: true

  - name: "weekly_report"
    schedule: "0 6 * * 1"
    duration: 30m
    multiplier: 1.5
    read_write_ratio:
      read: 90
      write: 10

  - name: "month_end_close"
    schedule: "0 22 L * *"
    duration: 3h
    multiplier: 1.8
    priority: 100
`
}
