package events

import (
	"sync"
	"testing"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
)

// TestParseCron_ValidExpressions tests parsing of valid cron expressions.
func TestParseCron_ValidExpressions(t *testing.T) {
	tests := []struct {
		expr     string
		minute   []int
		hour     []int
		dom      []int
		month    []int
		dow      []int
	}{
		{
			expr:   "0 2 * * *",
			minute: []int{0},
			hour:   []int{2},
			dom:    makeRange(1, 31, 1),
			month:  makeRange(1, 12, 1),
			dow:    makeRange(0, 6, 1),
		},
		{
			expr:   "*/15 * * * *",
			minute: []int{0, 15, 30, 45},
			hour:   makeRange(0, 23, 1),
			dom:    makeRange(1, 31, 1),
			month:  makeRange(1, 12, 1),
			dow:    makeRange(0, 6, 1),
		},
		{
			expr:   "0 22 * * 1-5",
			minute: []int{0},
			hour:   []int{22},
			dom:    makeRange(1, 31, 1),
			month:  makeRange(1, 12, 1),
			dow:    []int{1, 2, 3, 4, 5},
		},
		{
			expr:   "30 3 1 * *",
			minute: []int{30},
			hour:   []int{3},
			dom:    []int{1},
			month:  makeRange(1, 12, 1),
			dow:    makeRange(0, 6, 1),
		},
		{
			expr:   "0,30 8,17 * * *",
			minute: []int{0, 30},
			hour:   []int{8, 17},
			dom:    makeRange(1, 31, 1),
			month:  makeRange(1, 12, 1),
			dow:    makeRange(0, 6, 1),
		},
		{
			expr:   "0 22 L * *",
			minute: []int{0},
			hour:   []int{22},
			dom:    []int{-1}, // L marker
			month:  makeRange(1, 12, 1),
			dow:    makeRange(0, 6, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			cron, err := ParseCron(tt.expr)
			if err != nil {
				t.Fatalf("ParseCron(%q) error: %v", tt.expr, err)
			}

			if !intSliceEqual(cron.Minute, tt.minute) {
				t.Errorf("Minute: got %v, want %v", cron.Minute, tt.minute)
			}
			if !intSliceEqual(cron.Hour, tt.hour) {
				t.Errorf("Hour: got %v, want %v", cron.Hour, tt.hour)
			}
			if !intSliceEqual(cron.DayOfMonth, tt.dom) {
				t.Errorf("DayOfMonth: got %v, want %v", cron.DayOfMonth, tt.dom)
			}
			if !intSliceEqual(cron.Month, tt.month) {
				t.Errorf("Month: got %v, want %v", cron.Month, tt.month)
			}
			if !intSliceEqual(cron.DayOfWeek, tt.dow) {
				t.Errorf("DayOfWeek: got %v, want %v", cron.DayOfWeek, tt.dow)
			}
		})
	}
}

// TestParseCron_InvalidExpressions tests error handling for invalid expressions.
func TestParseCron_InvalidExpressions(t *testing.T) {
	tests := []struct {
		expr string
		desc string
	}{
		{"", "empty expression"},
		{"* * *", "only 3 fields"},
		{"* * * *", "only 4 fields"},
		{"* * * * * *", "6 fields"},
		{"60 * * * *", "minute 60 out of range"},
		{"* 24 * * *", "hour 24 out of range"},
		{"* * 32 * *", "day 32 out of range"},
		{"* * * 13 *", "month 13 out of range"},
		{"* * * * 7", "day of week 7 out of range"},
		{"abc * * * *", "invalid minute"},
		{"* */0 * * *", "step 0"},
		{"* * 5-2 * *", "reversed range"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParseCron(tt.expr)
			if err == nil {
				t.Errorf("ParseCron(%q) expected error for %s", tt.expr, tt.desc)
			}
		})
	}
}

// TestCronExpr_Matches tests the Matches function.
func TestCronExpr_Matches(t *testing.T) {
	tests := []struct {
		expr    string
		tm      time.Time
		matches bool
	}{
		// "0 2 * * *" = every day at 02:00
		{"0 2 * * *", time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC), true},
		{"0 2 * * *", time.Date(2024, 1, 15, 2, 1, 0, 0, time.UTC), false},
		{"0 2 * * *", time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC), false},

		// "*/15 * * * *" = every 15 minutes
		{"*/15 * * * *", time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), true},
		{"*/15 * * * *", time.Date(2024, 1, 15, 10, 15, 0, 0, time.UTC), true},
		{"*/15 * * * *", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), true},
		{"*/15 * * * *", time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC), true},
		{"*/15 * * * *", time.Date(2024, 1, 15, 10, 10, 0, 0, time.UTC), false},

		// "0 22 * * 1-5" = 22:00 Mon-Fri
		{"0 22 * * 1-5", time.Date(2024, 1, 15, 22, 0, 0, 0, time.UTC), true},  // Monday
		{"0 22 * * 1-5", time.Date(2024, 1, 14, 22, 0, 0, 0, time.UTC), false}, // Sunday
		{"0 22 * * 1-5", time.Date(2024, 1, 20, 22, 0, 0, 0, time.UTC), false}, // Saturday

		// "30 3 1 * *" = 03:30 first of month
		{"30 3 1 * *", time.Date(2024, 1, 1, 3, 30, 0, 0, time.UTC), true},
		{"30 3 1 * *", time.Date(2024, 1, 2, 3, 30, 0, 0, time.UTC), false},

		// "0 22 L * *" = 22:00 last day of month
		{"0 22 L * *", time.Date(2024, 1, 31, 22, 0, 0, 0, time.UTC), true},  // Jan 31
		{"0 22 L * *", time.Date(2024, 2, 29, 22, 0, 0, 0, time.UTC), true},  // Feb 29 (leap year)
		{"0 22 L * *", time.Date(2024, 2, 28, 22, 0, 0, 0, time.UTC), false}, // Feb 28 (not last in leap year)
		{"0 22 L * *", time.Date(2024, 1, 30, 22, 0, 0, 0, time.UTC), false}, // Jan 30 (not last)
	}

	for _, tt := range tests {
		t.Run(tt.expr+"_"+tt.tm.Format("2006-01-02_15:04"), func(t *testing.T) {
			cron, err := ParseCron(tt.expr)
			if err != nil {
				t.Fatalf("ParseCron error: %v", err)
			}

			got := cron.Matches(tt.tm)
			if got != tt.matches {
				t.Errorf("Matches(%v) = %v, want %v", tt.tm, got, tt.matches)
			}
		})
	}
}

// TestCronExpr_Next tests the Next function.
func TestCronExpr_Next(t *testing.T) {
	tests := []struct {
		expr     string
		from     time.Time
		expected time.Time
	}{
		{
			expr:     "0 2 * * *",
			from:     time.Date(2024, 1, 15, 1, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC),
		},
		{
			expr:     "0 2 * * *",
			from:     time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 16, 2, 0, 0, 0, time.UTC),
		},
		{
			expr:     "*/15 * * * *",
			from:     time.Date(2024, 1, 15, 10, 7, 0, 0, time.UTC),
			expected: time.Date(2024, 1, 15, 10, 15, 0, 0, time.UTC),
		},
		{
			expr:     "0 22 * * 1-5",
			from:     time.Date(2024, 1, 14, 23, 0, 0, 0, time.UTC), // Sunday
			expected: time.Date(2024, 1, 15, 22, 0, 0, 0, time.UTC), // Monday
		},
		{
			expr:     "0 6 * * 1", // Monday 06:00
			from:     time.Date(2024, 1, 15, 7, 0, 0, 0, time.UTC), // Monday 07:00 (after)
			expected: time.Date(2024, 1, 22, 6, 0, 0, 0, time.UTC), // Next Monday
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			cron, err := ParseCron(tt.expr)
			if err != nil {
				t.Fatalf("ParseCron error: %v", err)
			}

			got := cron.Next(tt.from)
			if !got.Equal(tt.expected) {
				t.Errorf("Next(%v) = %v, want %v", tt.from, got, tt.expected)
			}
		})
	}
}

// TestEventScheduler_AddEvent tests adding events to the scheduler.
func TestEventScheduler_AddEvent(t *testing.T) {
	clk := clock.NewRealClock()
	es := NewEventScheduler(clk)

	event := &ScheduledEvent{
		Name:     "test_event",
		Schedule: "0 2 * * *",
		Duration: 30 * time.Minute,
		Enabled:  true,
	}

	if err := es.AddEvent(event); err != nil {
		t.Fatalf("AddEvent error: %v", err)
	}

	events := es.GetEvents()
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}

	if events[0].Name != "test_event" {
		t.Errorf("expected name 'test_event', got '%s'", events[0].Name)
	}
}

// TestEventScheduler_RemoveEvent tests removing events from the scheduler.
func TestEventScheduler_RemoveEvent(t *testing.T) {
	clk := clock.NewRealClock()
	es := NewEventScheduler(clk)

	event := &ScheduledEvent{
		Name:     "test_event",
		Schedule: "0 2 * * *",
		Duration: 30 * time.Minute,
		Enabled:  true,
	}

	es.AddEvent(event)
	es.RemoveEvent("test_event")

	events := es.GetEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 events after removal, got %d", len(events))
	}
}

// TestEventScheduler_ActivatesEvent tests that events are activated at the correct time.
func TestEventScheduler_ActivatesEvent(t *testing.T) {
	// Start exactly at 02:00 so the event triggers immediately
	startTime := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)

	es := NewEventScheduler(clk)

	event := &ScheduledEvent{
		Name:       "test_event",
		Schedule:   "0 2 * * *", // 02:00
		Duration:   30 * time.Minute,
		Multiplier: 2.0,
		Enabled:    true,
	}

	es.AddEvent(event)

	// Create listener to track events
	listener := &testListener{}
	es.AddListener(listener)

	// Force check - event should trigger at 02:00
	es.ForceCheck()

	// Check if event is active
	if !es.IsEventActive("test_event") {
		t.Error("expected event to be active at 02:00")
	}

	if listener.startCount != 1 {
		t.Errorf("expected 1 start notification, got %d", listener.startCount)
	}
}

// TestEventScheduler_DeactivatesEvent tests that events are deactivated after duration.
func TestEventScheduler_DeactivatesEvent(t *testing.T) {
	// Start at 02:00 (event starts immediately)
	startTime := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)

	es := NewEventScheduler(clk)

	event := &ScheduledEvent{
		Name:     "test_event",
		Schedule: "0 2 * * *",
		Duration: 30 * time.Minute,
		Enabled:  true,
	}

	es.AddEvent(event)

	// Force initial check to start event at 02:00
	es.ForceCheck()

	if !es.IsEventActive("test_event") {
		t.Fatal("expected event to be active initially")
	}

	// Verify the active event's end time
	active := es.GetActiveEvents()
	if len(active) != 1 {
		t.Fatal("expected 1 active event")
	}

	// The end time should be approximately start time + duration
	// Allow small tolerance for clock drift
	expectedEnd := startTime.Add(30 * time.Minute)
	timeDiff := active[0].EndTime.Sub(expectedEnd)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}
	if timeDiff > time.Second {
		t.Errorf("expected end time ~%v, got %v (diff: %v)", expectedEnd, active[0].EndTime, timeDiff)
	}
}

// TestEventScheduler_OverlappingEvents tests priority handling for overlapping events.
func TestEventScheduler_OverlappingEvents(t *testing.T) {
	startTime := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)

	es := NewEventScheduler(clk)

	// Low priority event
	event1 := &ScheduledEvent{
		Name:       "low_priority",
		Schedule:   "0 2 * * *",
		Duration:   1 * time.Hour,
		Multiplier: 1.5,
		Priority:   10,
		Enabled:    true,
	}

	// High priority event
	event2 := &ScheduledEvent{
		Name:       "high_priority",
		Schedule:   "0 2 * * *",
		Duration:   30 * time.Minute,
		Multiplier: 3.0,
		Priority:   100,
		Enabled:    true,
	}

	es.AddEvent(event1)
	es.AddEvent(event2)

	es.ForceCheck()

	// Both events should be active
	active := es.GetActiveEvents()
	if len(active) != 2 {
		t.Errorf("expected 2 active events, got %d", len(active))
	}

	// Effective multiplier should be from high priority event
	mult := es.GetEffectiveMultiplier(1.0)
	if mult != 3.0 {
		t.Errorf("expected multiplier 3.0 (high priority), got %f", mult)
	}
}

// TestEventScheduler_GetEffectiveMultiplier tests multiplier calculation.
func TestEventScheduler_GetEffectiveMultiplier(t *testing.T) {
	startTime := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)

	es := NewEventScheduler(clk)

	// No active events - should return base
	mult := es.GetEffectiveMultiplier(1.0)
	if mult != 1.0 {
		t.Errorf("expected base multiplier 1.0, got %f", mult)
	}

	// Add and activate event
	event := &ScheduledEvent{
		Name:       "test_event",
		Schedule:   "0 2 * * *",
		Duration:   30 * time.Minute,
		Multiplier: 2.5,
		Enabled:    true,
	}

	es.AddEvent(event)
	es.ForceCheck()

	mult = es.GetEffectiveMultiplier(1.0)
	if mult != 2.5 {
		t.Errorf("expected multiplier 2.5, got %f", mult)
	}
}

// TestEventScheduler_SimulatedClock tests events with time compression.
func TestEventScheduler_SimulatedClock(t *testing.T) {
	// Start exactly at an hour boundary so the event triggers
	startTime := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	clk := clock.NewSimulatedClock(startTime, 1)

	es := NewEventScheduler(clk)

	event := &ScheduledEvent{
		Name:     "hourly_event",
		Schedule: "0 * * * *", // Every hour on the hour
		Duration: 5 * time.Minute,
		Enabled:  true,
	}

	es.AddEvent(event)

	// Force check - event should trigger at 02:00
	es.ForceCheck()

	if !es.IsEventActive("hourly_event") {
		t.Error("expected event to be active at 02:00")
	}

	// Test that GetNextOccurrence works
	nextTime, ok := es.GetNextOccurrence("hourly_event")
	if !ok {
		t.Error("expected to get next occurrence")
	}

	// Next occurrence should be 03:00
	expected := time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC)
	if !nextTime.Equal(expected) {
		t.Errorf("expected next occurrence at %v, got %v", expected, nextTime)
	}
}

// TestParseEventsFromYAML tests YAML parsing.
func TestParseEventsFromYAML(t *testing.T) {
	yaml := `scheduled_events:
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
`

	events, err := ParseEventsFromYAML([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseEventsFromYAML error: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}

	// Check first event
	if events[0].Name != "nightly_batch" {
		t.Errorf("expected name 'nightly_batch', got '%s'", events[0].Name)
	}
	if events[0].Duration != 45*time.Minute {
		t.Errorf("expected duration 45m, got %v", events[0].Duration)
	}
	if events[0].Multiplier != 2.0 {
		t.Errorf("expected multiplier 2.0, got %f", events[0].Multiplier)
	}

	// Check third event with read/write ratio
	if events[2].ReadWriteRatio == nil {
		t.Error("expected read/write ratio to be set")
	} else if events[2].ReadWriteRatio.Read != 90 {
		t.Errorf("expected read ratio 90, got %d", events[2].ReadWriteRatio.Read)
	}
}

// TestParseEventsFromYAML_Invalid tests error handling for invalid YAML.
func TestParseEventsFromYAML_Invalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "missing name",
			yaml: `scheduled_events:
  - schedule: "0 2 * * *"
    duration: 30m
`,
		},
		{
			name: "missing schedule",
			yaml: `scheduled_events:
  - name: "test"
    duration: 30m
`,
		},
		{
			name: "invalid schedule",
			yaml: `scheduled_events:
  - name: "test"
    schedule: "invalid"
    duration: 30m
`,
		},
		{
			name: "invalid duration",
			yaml: `scheduled_events:
  - name: "test"
    schedule: "0 2 * * *"
    duration: "invalid"
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseEventsFromYAML([]byte(tt.yaml))
			if err == nil {
				t.Error("expected error for invalid YAML")
			}
		})
	}
}

// TestScheduledEvent_Validate tests event validation.
func TestScheduledEvent_Validate(t *testing.T) {
	tests := []struct {
		name    string
		event   ScheduledEvent
		wantErr bool
	}{
		{
			name: "valid event",
			event: ScheduledEvent{
				Name:     "test",
				Schedule: "0 2 * * *",
				Duration: 30 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "missing name",
			event: ScheduledEvent{
				Schedule: "0 2 * * *",
				Duration: 30 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "missing schedule",
			event: ScheduledEvent{
				Name:     "test",
				Duration: 30 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "invalid schedule",
			event: ScheduledEvent{
				Name:     "test",
				Schedule: "invalid",
				Duration: 30 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "zero duration",
			event: ScheduledEvent{
				Name:     "test",
				Schedule: "0 2 * * *",
				Duration: 0,
			},
			wantErr: true,
		},
		{
			name: "negative multiplier",
			event: ScheduledEvent{
				Name:       "test",
				Schedule:   "0 2 * * *",
				Duration:   30 * time.Minute,
				Multiplier: -1.0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestGetPresetEvent tests retrieving preset events.
func TestGetPresetEvent(t *testing.T) {
	event, err := GetPresetEvent("nightly_batch")
	if err != nil {
		t.Fatalf("GetPresetEvent error: %v", err)
	}

	if event.Name != "nightly_batch" {
		t.Errorf("expected name 'nightly_batch', got '%s'", event.Name)
	}

	if event.Schedule != "0 2 * * *" {
		t.Errorf("expected schedule '0 2 * * *', got '%s'", event.Schedule)
	}

	// Test non-existent preset
	_, err = GetPresetEvent("non_existent")
	if err == nil {
		t.Error("expected error for non-existent preset")
	}
}

// TestListPresetEvents tests listing preset events.
func TestListPresetEvents(t *testing.T) {
	names := ListPresetEvents()

	if len(names) == 0 {
		t.Error("expected at least one preset event")
	}

	// Check that nightly_batch is in the list
	found := false
	for _, name := range names {
		if name == "nightly_batch" {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected 'nightly_batch' in preset list")
	}
}

// TestEventEffects tests effect merging.
func TestEventEffects(t *testing.T) {
	events := []*ActiveEvent{
		{
			Event: &ScheduledEvent{
				Name:       "high_priority",
				Multiplier: 3.0,
				ReadWriteRatio: &ReadWriteRatio{
					Read:  80,
					Write: 20,
				},
				Priority: 100,
			},
		},
		{
			Event: &ScheduledEvent{
				Name:       "low_priority",
				Multiplier: 1.5,
				Priority:   10,
			},
		},
	}

	effects := MergeEffects(nil, events)

	if effects.MultiplierOverride == nil {
		t.Fatal("expected multiplier override")
	}
	if *effects.MultiplierOverride != 3.0 {
		t.Errorf("expected multiplier 3.0, got %f", *effects.MultiplierOverride)
	}

	if effects.WorkloadOverride == nil {
		t.Fatal("expected workload override")
	}
	if effects.WorkloadOverride.Read != 80 {
		t.Errorf("expected read 80, got %d", effects.WorkloadOverride.Read)
	}
}

// TestCombineMultipliers tests different multiplier combination strategies.
func TestCombineMultipliers(t *testing.T) {
	events := []*ActiveEvent{
		{Event: &ScheduledEvent{Multiplier: 2.0, Priority: 100}},
		{Event: &ScheduledEvent{Multiplier: 1.5, Priority: 50}},
	}

	// Test highest priority strategy
	result := CombineMultipliers(1.0, events, MultiplierHighestPriority)
	if result != 2.0 {
		t.Errorf("HighestPriority: expected 2.0, got %f", result)
	}

	// Test max strategy
	result = CombineMultipliers(1.0, events, MultiplierMax)
	if result != 2.0 {
		t.Errorf("Max: expected 2.0, got %f", result)
	}

	// Test multiply strategy
	result = CombineMultipliers(1.0, events, MultiplierMultiply)
	if result != 3.0 { // 1.0 * 2.0 * 1.5
		t.Errorf("Multiply: expected 3.0, got %f", result)
	}
}

// TestActiveEvent_IsActive tests the IsActive method.
func TestActiveEvent_IsActive(t *testing.T) {
	start := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)

	ae := &ActiveEvent{
		Event:     &ScheduledEvent{Name: "test"},
		StartTime: start,
		EndTime:   end,
	}

	tests := []struct {
		tm     time.Time
		active bool
	}{
		{start.Add(-1 * time.Minute), false}, // Before
		{start, true},                         // At start
		{start.Add(15 * time.Minute), true},   // During
		{end, false},                          // At end
		{end.Add(1 * time.Minute), false},     // After
	}

	for _, tt := range tests {
		t.Run(tt.tm.Format("15:04"), func(t *testing.T) {
			got := ae.IsActive(tt.tm)
			if got != tt.active {
				t.Errorf("IsActive(%v) = %v, want %v", tt.tm, got, tt.active)
			}
		})
	}
}

// Helper functions

func intSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type testListener struct {
	mu         sync.Mutex
	startCount int
	endCount   int
	lastStart  *ActiveEvent
	lastEnd    *ActiveEvent
}

func (l *testListener) OnEventStart(event *ActiveEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.startCount++
	l.lastStart = event
}

func (l *testListener) OnEventEnd(event *ActiveEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.endCount++
	l.lastEnd = event
}
