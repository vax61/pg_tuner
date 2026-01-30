package events

import (
	"fmt"
	"strings"
)

// WorkloadDist defines the read/write distribution for effects.
type WorkloadDist struct {
	Read  int `yaml:"read" json:"read"`
	Write int `yaml:"write" json:"write"`
}

// EventEffects represents the combined effects of active events.
type EventEffects struct {
	MultiplierOverride *float64     // nil if no override
	WorkloadOverride   *WorkloadDist // nil if no override
	Description        string       // human-readable description of active effects
	ActiveEventNames   []string     // names of contributing events
}

// GetCurrentEffects returns the combined effects of all active events.
func (es *EventScheduler) GetCurrentEffects() *EventEffects {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if len(es.activeEvents) == 0 {
		return &EventEffects{
			Description: "No active events",
		}
	}

	return MergeEffects(nil, es.activeEvents)
}

// MergeEffects combines effects from multiple active events.
// Events are expected to be sorted by priority (highest first).
// Higher priority events override lower priority ones.
func MergeEffects(base *EventEffects, events []*ActiveEvent) *EventEffects {
	if len(events) == 0 {
		if base != nil {
			return base
		}
		return &EventEffects{
			Description: "No active events",
		}
	}

	result := &EventEffects{
		ActiveEventNames: make([]string, 0, len(events)),
	}

	// Copy base if provided
	if base != nil {
		if base.MultiplierOverride != nil {
			m := *base.MultiplierOverride
			result.MultiplierOverride = &m
		}
		if base.WorkloadOverride != nil {
			w := *base.WorkloadOverride
			result.WorkloadOverride = &w
		}
		result.ActiveEventNames = append(result.ActiveEventNames, base.ActiveEventNames...)
	}

	// Process events (highest priority first)
	var descriptions []string

	for _, ae := range events {
		result.ActiveEventNames = append(result.ActiveEventNames, ae.Event.Name)

		// Multiplier: highest priority event with multiplier wins
		if result.MultiplierOverride == nil && ae.Event.Multiplier > 0 {
			m := ae.Event.Multiplier
			result.MultiplierOverride = &m
			descriptions = append(descriptions, fmt.Sprintf("%s: multiplier=%.2f", ae.Event.Name, m))
		}

		// Read/Write ratio: highest priority event with ratio wins
		if result.WorkloadOverride == nil && ae.Event.ReadWriteRatio != nil {
			result.WorkloadOverride = &WorkloadDist{
				Read:  ae.Event.ReadWriteRatio.Read,
				Write: ae.Event.ReadWriteRatio.Write,
			}
			descriptions = append(descriptions, fmt.Sprintf("%s: R/W=%d/%d",
				ae.Event.Name,
				ae.Event.ReadWriteRatio.Read,
				ae.Event.ReadWriteRatio.Write))
		}

		// Workload override name (for profile switching)
		if ae.Event.WorkloadOverride != "" {
			descriptions = append(descriptions, fmt.Sprintf("%s: profile=%s", ae.Event.Name, ae.Event.WorkloadOverride))
		}
	}

	if len(descriptions) > 0 {
		result.Description = strings.Join(descriptions, "; ")
	} else {
		result.Description = fmt.Sprintf("%d active event(s)", len(events))
	}

	return result
}

// HasEffects returns true if there are any active effects.
func (e *EventEffects) HasEffects() bool {
	return e.MultiplierOverride != nil || e.WorkloadOverride != nil
}

// GetMultiplier returns the multiplier override or the provided default.
func (e *EventEffects) GetMultiplier(defaultValue float64) float64 {
	if e.MultiplierOverride != nil {
		return *e.MultiplierOverride
	}
	return defaultValue
}

// GetWorkloadDist returns the workload distribution override or the provided default.
func (e *EventEffects) GetWorkloadDist(defaultRead, defaultWrite int) (read, write int) {
	if e.WorkloadOverride != nil {
		return e.WorkloadOverride.Read, e.WorkloadOverride.Write
	}
	return defaultRead, defaultWrite
}

// ApplyTo applies the effects to a base configuration.
type EffectTarget struct {
	Multiplier  float64
	ReadRatio   int
	WriteRatio  int
	ProfileName string
}

// Apply applies event effects to a target configuration.
func (e *EventEffects) Apply(target *EffectTarget) {
	if target == nil {
		return
	}

	if e.MultiplierOverride != nil {
		target.Multiplier = *e.MultiplierOverride
	}

	if e.WorkloadOverride != nil {
		target.ReadRatio = e.WorkloadOverride.Read
		target.WriteRatio = e.WorkloadOverride.Write
	}
}

// String returns a string representation of the effects.
func (e *EventEffects) String() string {
	if !e.HasEffects() {
		return "EventEffects{none}"
	}

	var parts []string
	if e.MultiplierOverride != nil {
		parts = append(parts, fmt.Sprintf("multiplier=%.2f", *e.MultiplierOverride))
	}
	if e.WorkloadOverride != nil {
		parts = append(parts, fmt.Sprintf("R/W=%d/%d", e.WorkloadOverride.Read, e.WorkloadOverride.Write))
	}

	return fmt.Sprintf("EventEffects{%s}", strings.Join(parts, ", "))
}

// CombineMultipliers combines multiple multipliers.
// By default, uses the highest priority (first) non-zero multiplier.
// Alternative strategies can be implemented as needed.
type MultiplierStrategy int

const (
	// MultiplierHighestPriority uses the first non-zero multiplier (highest priority).
	MultiplierHighestPriority MultiplierStrategy = iota

	// MultiplierSum adds all multipliers together.
	MultiplierSum

	// MultiplierMultiply multiplies all multipliers together.
	MultiplierMultiply

	// MultiplierMax uses the maximum multiplier.
	MultiplierMax
)

// CombineMultipliers combines multipliers from multiple events using the specified strategy.
func CombineMultipliers(baseMultiplier float64, events []*ActiveEvent, strategy MultiplierStrategy) float64 {
	if len(events) == 0 {
		return baseMultiplier
	}

	switch strategy {
	case MultiplierHighestPriority:
		for _, ae := range events {
			if ae.Event.Multiplier > 0 {
				return ae.Event.Multiplier
			}
		}
		return baseMultiplier

	case MultiplierSum:
		sum := baseMultiplier
		for _, ae := range events {
			if ae.Event.Multiplier > 0 {
				sum += ae.Event.Multiplier - 1.0 // Add the delta from base
			}
		}
		return sum

	case MultiplierMultiply:
		result := baseMultiplier
		for _, ae := range events {
			if ae.Event.Multiplier > 0 {
				result *= ae.Event.Multiplier
			}
		}
		return result

	case MultiplierMax:
		maxMult := baseMultiplier
		for _, ae := range events {
			if ae.Event.Multiplier > maxMult {
				maxMult = ae.Event.Multiplier
			}
		}
		return maxMult

	default:
		return baseMultiplier
	}
}

// GetEffectiveWorkload returns the effective workload profile name.
func (es *EventScheduler) GetEffectiveWorkload(baseProfile string) string {
	es.mu.RLock()
	defer es.mu.RUnlock()

	// Return first active event's workload override (highest priority)
	for _, ae := range es.activeEvents {
		if ae.Event.WorkloadOverride != "" {
			return ae.Event.WorkloadOverride
		}
	}

	return baseProfile
}

// GetEffectiveReadWriteRatio returns the effective read/write ratio.
func (es *EventScheduler) GetEffectiveReadWriteRatio(baseRead, baseWrite int) (read, write int) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	// Return first active event's ratio override (highest priority)
	for _, ae := range es.activeEvents {
		if ae.Event.ReadWriteRatio != nil {
			return ae.Event.ReadWriteRatio.Read, ae.Event.ReadWriteRatio.Write
		}
	}

	return baseRead, baseWrite
}
