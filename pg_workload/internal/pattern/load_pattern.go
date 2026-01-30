package pattern

import (
	"fmt"
)

// LoadPattern defines how QPS varies over time.
type LoadPattern struct {
	// Type is the pattern type ("hourly", "custom")
	Type string `yaml:"type" json:"type"`

	// BaselineQPS is the base QPS (multiplier 1.0 = this value)
	BaselineQPS int `yaml:"baseline_qps" json:"baseline_qps"`

	// HourlyMultipliers maps hour (0-23) to QPS multiplier
	HourlyMultipliers map[int]float64 `yaml:"hourly_multipliers" json:"hourly_multipliers"`

	// MinMultiplier is the floor for multipliers (default 0.1)
	MinMultiplier float64 `yaml:"min_multiplier" json:"min_multiplier"`

	// MaxMultiplier is the ceiling for multipliers (default 10.0)
	MaxMultiplier float64 `yaml:"max_multiplier" json:"max_multiplier"`
}

// NewLoadPattern creates a new LoadPattern with default values.
func NewLoadPattern() *LoadPattern {
	return &LoadPattern{
		Type:              "hourly",
		BaselineQPS:       100,
		HourlyMultipliers: make(map[int]float64),
		MinMultiplier:     0.1,
		MaxMultiplier:     10.0,
	}
}

// GetMultiplier returns the multiplier for a given hour (0-23).
// Returns 1.0 if hour is not defined in the pattern.
func (p *LoadPattern) GetMultiplier(hour int) float64 {
	// Normalize hour to 0-23
	hour = normalizeHour(hour)

	mult, ok := p.HourlyMultipliers[hour]
	if !ok {
		return 1.0
	}

	return p.clampMultiplier(mult)
}

// GetTargetQPS returns the target QPS for a given hour.
func (p *LoadPattern) GetTargetQPS(hour int) int {
	mult := p.GetMultiplier(hour)
	qps := float64(p.BaselineQPS) * mult

	// Ensure at least 1 QPS
	if qps < 1 {
		return 1
	}
	return int(qps)
}

// Validate checks if the pattern is valid.
func (p *LoadPattern) Validate() error {
	// Validate type
	if p.Type != "hourly" && p.Type != "custom" {
		return fmt.Errorf("invalid pattern type: %s (must be 'hourly' or 'custom')", p.Type)
	}

	// Validate baseline QPS
	if p.BaselineQPS <= 0 {
		return fmt.Errorf("baseline_qps must be positive, got %d", p.BaselineQPS)
	}

	// Validate min/max multipliers
	if p.MinMultiplier < 0 {
		return fmt.Errorf("min_multiplier must be non-negative, got %f", p.MinMultiplier)
	}
	if p.MaxMultiplier <= 0 {
		return fmt.Errorf("max_multiplier must be positive, got %f", p.MaxMultiplier)
	}
	if p.MinMultiplier > p.MaxMultiplier {
		return fmt.Errorf("min_multiplier (%f) cannot exceed max_multiplier (%f)", p.MinMultiplier, p.MaxMultiplier)
	}

	// Validate hourly multipliers
	for hour, mult := range p.HourlyMultipliers {
		if hour < 0 || hour > 23 {
			return fmt.Errorf("invalid hour %d (must be 0-23)", hour)
		}
		if mult < 0 {
			return fmt.Errorf("multiplier for hour %d cannot be negative: %f", hour, mult)
		}
	}

	return nil
}

// SetDefaults ensures default values are set.
func (p *LoadPattern) SetDefaults() {
	if p.MinMultiplier == 0 {
		p.MinMultiplier = 0.1
	}
	if p.MaxMultiplier == 0 {
		p.MaxMultiplier = 10.0
	}
	if p.HourlyMultipliers == nil {
		p.HourlyMultipliers = make(map[int]float64)
	}
	if p.Type == "" {
		p.Type = "hourly"
	}
}

// clampMultiplier constrains a multiplier to min/max bounds.
func (p *LoadPattern) clampMultiplier(mult float64) float64 {
	if mult < p.MinMultiplier {
		return p.MinMultiplier
	}
	if mult > p.MaxMultiplier {
		return p.MaxMultiplier
	}
	return mult
}

// normalizeHour ensures hour is in range 0-23.
func normalizeHour(hour int) int {
	hour = hour % 24
	if hour < 0 {
		hour += 24
	}
	return hour
}

// Clone creates a deep copy of the pattern.
func (p *LoadPattern) Clone() *LoadPattern {
	clone := &LoadPattern{
		Type:              p.Type,
		BaselineQPS:       p.BaselineQPS,
		MinMultiplier:     p.MinMultiplier,
		MaxMultiplier:     p.MaxMultiplier,
		HourlyMultipliers: make(map[int]float64, len(p.HourlyMultipliers)),
	}
	for k, v := range p.HourlyMultipliers {
		clone.HourlyMultipliers[k] = v
	}
	return clone
}
