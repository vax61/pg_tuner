package pattern

import (
	"time"
)

// GetMultiplierSmooth returns the interpolated multiplier for a given time.
// It linearly interpolates between the current hour's multiplier and the next hour's.
// For example, at 09:30 it returns the midpoint between multiplier[9] and multiplier[10].
func (p *LoadPattern) GetMultiplierSmooth(t time.Time) float64 {
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()

	// Calculate fraction of hour elapsed (0.0 to 1.0)
	secondsIntoHour := float64(minute*60 + second)
	fraction := secondsIntoHour / 3600.0

	// Get multipliers for current and next hour
	currentMult := p.GetMultiplier(hour)
	nextMult := p.GetMultiplier((hour + 1) % 24)

	// Linear interpolation: current + fraction * (next - current)
	interpolated := currentMult + fraction*(nextMult-currentMult)

	return p.clampMultiplier(interpolated)
}

// GetTargetQPSSmooth returns the interpolated target QPS for a given time.
func (p *LoadPattern) GetTargetQPSSmooth(t time.Time) int {
	mult := p.GetMultiplierSmooth(t)
	qps := float64(p.BaselineQPS) * mult

	if qps < 1 {
		return 1
	}
	return int(qps)
}

// GetMultiplierAt returns the multiplier at a specific hour and minute.
// This is a convenience method for testing.
func (p *LoadPattern) GetMultiplierAt(hour, minute int) float64 {
	t := time.Date(2024, 1, 1, hour, minute, 0, 0, time.UTC)
	return p.GetMultiplierSmooth(t)
}

// InterpolatedMultipliers returns all multipliers for a 24-hour period
// at the specified resolution (e.g., every 15 minutes).
func (p *LoadPattern) InterpolatedMultipliers(resolution time.Duration) []TimeMultiplier {
	if resolution < time.Minute {
		resolution = time.Minute
	}

	steps := int((24 * time.Hour) / resolution)
	result := make([]TimeMultiplier, 0, steps)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < steps; i++ {
		t := baseTime.Add(time.Duration(i) * resolution)
		result = append(result, TimeMultiplier{
			Time:       t,
			Hour:       t.Hour(),
			Minute:     t.Minute(),
			Multiplier: p.GetMultiplierSmooth(t),
			TargetQPS:  p.GetTargetQPSSmooth(t),
		})
	}

	return result
}

// TimeMultiplier represents a multiplier at a specific time.
type TimeMultiplier struct {
	Time       time.Time
	Hour       int
	Minute     int
	Multiplier float64
	TargetQPS  int
}
