package events

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/myorg/pg_tuner/pg_workload/internal/clock"
)

// EventListener receives notifications about event state changes.
type EventListener interface {
	OnEventStart(event *ActiveEvent)
	OnEventEnd(event *ActiveEvent)
}

// EventScheduler manages scheduled events.
type EventScheduler struct {
	clock        clock.Clock
	events       []*ScheduledEvent
	activeEvents []*ActiveEvent
	listeners    []EventListener
	parsedCrons  map[string]*CronExpr // cached parsed cron expressions
	mu           sync.RWMutex
	done         chan struct{}
	wg           sync.WaitGroup
	stopped      bool
}

// NewEventScheduler creates a new event scheduler.
func NewEventScheduler(clk clock.Clock) *EventScheduler {
	if clk == nil {
		clk = clock.NewRealClock()
	}

	return &EventScheduler{
		clock:        clk,
		events:       make([]*ScheduledEvent, 0),
		activeEvents: make([]*ActiveEvent, 0),
		listeners:    make([]EventListener, 0),
		parsedCrons:  make(map[string]*CronExpr),
		done:         make(chan struct{}),
	}
}

// AddEvent adds a scheduled event.
func (es *EventScheduler) AddEvent(event *ScheduledEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}

	es.mu.Lock()
	defer es.mu.Unlock()

	// Check for duplicate name
	for _, e := range es.events {
		if e.Name == event.Name {
			// Replace existing event
			*e = *event
			delete(es.parsedCrons, event.Name) // Clear cached cron
			return nil
		}
	}

	// Parse and cache cron expression
	cron, err := ParseCron(event.Schedule)
	if err != nil {
		return err
	}
	es.parsedCrons[event.Name] = cron

	es.events = append(es.events, event.Copy())
	return nil
}

// RemoveEvent removes a scheduled event by name.
func (es *EventScheduler) RemoveEvent(name string) {
	es.mu.Lock()
	defer es.mu.Unlock()

	// Remove from events
	for i, e := range es.events {
		if e.Name == name {
			es.events = append(es.events[:i], es.events[i+1:]...)
			break
		}
	}

	// Remove from active events
	for i, ae := range es.activeEvents {
		if ae.Event.Name == name {
			// Notify listeners
			for _, l := range es.listeners {
				l.OnEventEnd(ae)
			}
			es.activeEvents = append(es.activeEvents[:i], es.activeEvents[i+1:]...)
			break
		}
	}

	delete(es.parsedCrons, name)
}

// AddListener adds an event listener.
func (es *EventScheduler) AddListener(listener EventListener) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.listeners = append(es.listeners, listener)
}

// RemoveListener removes an event listener.
func (es *EventScheduler) RemoveListener(listener EventListener) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for i, l := range es.listeners {
		if l == listener {
			es.listeners = append(es.listeners[:i], es.listeners[i+1:]...)
			return
		}
	}
}

// Start starts the scheduler loop that checks events every minute.
func (es *EventScheduler) Start(ctx context.Context) {
	es.wg.Add(1)
	go es.schedulerLoop(ctx)
}

// Stop stops the scheduler.
func (es *EventScheduler) Stop() {
	es.mu.Lock()
	if es.stopped {
		es.mu.Unlock()
		return
	}
	es.stopped = true
	close(es.done)
	es.mu.Unlock()

	es.wg.Wait()
}

// schedulerLoop checks events periodically.
func (es *EventScheduler) schedulerLoop(ctx context.Context) {
	defer es.wg.Done()

	// Use clock's ticker for proper simulation support
	ticker := es.clock.Ticker(time.Minute)
	defer ticker.Stop()

	// Initial check
	es.checkEvents()

	for {
		select {
		case <-ctx.Done():
			return
		case <-es.done:
			return
		case <-ticker.C:
			es.checkEvents()
		}
	}
}

// checkEvents checks for events that should start or end.
func (es *EventScheduler) checkEvents() {
	es.mu.Lock()
	defer es.mu.Unlock()

	now := es.clock.Now()

	// Check for events that should end
	var stillActive []*ActiveEvent
	for _, ae := range es.activeEvents {
		if now.After(ae.EndTime) || now.Equal(ae.EndTime) {
			// Event ended
			for _, l := range es.listeners {
				l.OnEventEnd(ae)
			}
		} else {
			stillActive = append(stillActive, ae)
		}
	}
	es.activeEvents = stillActive

	// Check for events that should start
	for _, event := range es.events {
		if !event.Enabled {
			continue
		}

		// Skip if already active
		if es.isEventActiveUnlocked(event.Name) {
			continue
		}

		// Check if this event should trigger now
		cron := es.parsedCrons[event.Name]
		if cron == nil {
			var err error
			cron, err = ParseCron(event.Schedule)
			if err != nil {
				continue
			}
			es.parsedCrons[event.Name] = cron
		}

		// Check if the current time matches
		if cron.Matches(now) {
			ae := &ActiveEvent{
				Event:     event.Copy(),
				StartTime: now,
				EndTime:   now.Add(event.Duration),
			}
			es.activeEvents = append(es.activeEvents, ae)

			// Notify listeners
			for _, l := range es.listeners {
				l.OnEventStart(ae)
			}
		}
	}

	// Sort active events by priority (highest first)
	sort.Slice(es.activeEvents, func(i, j int) bool {
		return es.activeEvents[i].Event.Priority > es.activeEvents[j].Event.Priority
	})
}

// isEventActiveUnlocked checks if an event is active (must hold lock).
func (es *EventScheduler) isEventActiveUnlocked(name string) bool {
	for _, ae := range es.activeEvents {
		if ae.Event.Name == name {
			return true
		}
	}
	return false
}

// GetActiveEvents returns a copy of currently active events.
func (es *EventScheduler) GetActiveEvents() []*ActiveEvent {
	es.mu.RLock()
	defer es.mu.RUnlock()

	result := make([]*ActiveEvent, len(es.activeEvents))
	for i, ae := range es.activeEvents {
		aeCopy := *ae
		aeCopy.Event = ae.Event.Copy()
		result[i] = &aeCopy
	}
	return result
}

// IsEventActive checks if a specific event is currently active.
func (es *EventScheduler) IsEventActive(name string) bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.isEventActiveUnlocked(name)
}

// GetEffectiveMultiplier returns the effective multiplier considering active events.
// If multiple events are active, the highest priority event's multiplier is used.
// If no events with multiplier override are active, returns the base multiplier.
func (es *EventScheduler) GetEffectiveMultiplier(baseMultiplier float64) float64 {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if len(es.activeEvents) == 0 {
		return baseMultiplier
	}

	// Events are sorted by priority (highest first)
	for _, ae := range es.activeEvents {
		if ae.Event.Multiplier > 0 {
			return ae.Event.Multiplier
		}
	}

	return baseMultiplier
}

// GetEvents returns a copy of all scheduled events.
func (es *EventScheduler) GetEvents() []*ScheduledEvent {
	es.mu.RLock()
	defer es.mu.RUnlock()

	result := make([]*ScheduledEvent, len(es.events))
	for i, e := range es.events {
		result[i] = e.Copy()
	}
	return result
}

// GetEvent returns a specific event by name.
func (es *EventScheduler) GetEvent(name string) (*ScheduledEvent, bool) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	for _, e := range es.events {
		if e.Name == name {
			return e.Copy(), true
		}
	}
	return nil, false
}

// EnableEvent enables or disables an event.
func (es *EventScheduler) EnableEvent(name string, enabled bool) bool {
	es.mu.Lock()
	defer es.mu.Unlock()

	for _, e := range es.events {
		if e.Name == name {
			e.Enabled = enabled
			return true
		}
	}
	return false
}

// GetNextOccurrence returns the next occurrence time for a named event.
func (es *EventScheduler) GetNextOccurrence(name string) (time.Time, bool) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	for _, e := range es.events {
		if e.Name == name {
			cron := es.parsedCrons[name]
			if cron == nil {
				var err error
				cron, err = ParseCron(e.Schedule)
				if err != nil {
					return time.Time{}, false
				}
			}
			return cron.Next(es.clock.Now()), true
		}
	}
	return time.Time{}, false
}

// TriggerEventNow manually triggers an event immediately.
func (es *EventScheduler) TriggerEventNow(name string) bool {
	es.mu.Lock()
	defer es.mu.Unlock()

	// Find the event
	var event *ScheduledEvent
	for _, e := range es.events {
		if e.Name == name {
			event = e
			break
		}
	}

	if event == nil {
		return false
	}

	// Check if already active
	if es.isEventActiveUnlocked(name) {
		return false
	}

	now := es.clock.Now()
	ae := &ActiveEvent{
		Event:     event.Copy(),
		StartTime: now,
		EndTime:   now.Add(event.Duration),
	}
	es.activeEvents = append(es.activeEvents, ae)

	// Sort by priority
	sort.Slice(es.activeEvents, func(i, j int) bool {
		return es.activeEvents[i].Event.Priority > es.activeEvents[j].Event.Priority
	})

	// Notify listeners
	for _, l := range es.listeners {
		l.OnEventStart(ae)
	}

	return true
}

// ForceCheck forces an immediate event check (useful for testing).
func (es *EventScheduler) ForceCheck() {
	es.checkEvents()
}
