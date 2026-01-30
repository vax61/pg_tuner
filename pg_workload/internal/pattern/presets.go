package pattern

import "fmt"

// Predefined load patterns.
var (
	// ProductionDayPattern simulates typical office hours (9-18) workload.
	// Peak load during business hours, low load at night.
	ProductionDayPattern = &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  0.2,  // Midnight - very low
			1:  0.15, // 1 AM
			2:  0.1,  // 2 AM - minimum
			3:  0.1,  // 3 AM
			4:  0.1,  // 4 AM
			5:  0.15, // 5 AM - starting to wake up
			6:  0.3,  // 6 AM
			7:  0.5,  // 7 AM - early birds
			8:  0.8,  // 8 AM - ramping up
			9:  1.2,  // 9 AM - work starts
			10: 1.5,  // 10 AM - morning peak
			11: 1.4,  // 11 AM
			12: 1.0,  // 12 PM - lunch dip
			13: 1.1,  // 1 PM
			14: 1.5,  // 2 PM - afternoon peak
			15: 1.4,  // 3 PM
			16: 1.3,  // 4 PM
			17: 1.2,  // 5 PM - end of day
			18: 0.8,  // 6 PM - winding down
			19: 0.5,  // 7 PM
			20: 0.4,  // 8 PM
			21: 0.3,  // 9 PM
			22: 0.25, // 10 PM
			23: 0.2,  // 11 PM
		},
	}

	// EcommercePattern simulates e-commerce site traffic.
	// Peaks in evening hours when people shop from home.
	EcommercePattern = &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  0.3,  // Midnight - late shoppers
			1:  0.2,  // 1 AM
			2:  0.15, // 2 AM
			3:  0.1,  // 3 AM - minimum
			4:  0.1,  // 4 AM
			5:  0.15, // 5 AM
			6:  0.25, // 6 AM - early risers
			7:  0.4,  // 7 AM
			8:  0.5,  // 8 AM - commute browsing
			9:  0.6,  // 9 AM
			10: 0.7,  // 10 AM
			11: 0.8,  // 11 AM
			12: 1.0,  // 12 PM - lunch break shopping
			13: 0.9,  // 1 PM
			14: 0.8,  // 2 PM
			15: 0.9,  // 3 PM
			16: 1.0,  // 4 PM
			17: 1.2,  // 5 PM - after work
			18: 1.5,  // 6 PM - dinner time browsing
			19: 1.8,  // 7 PM - evening peak starts
			20: 2.0,  // 8 PM - peak shopping time
			21: 1.8,  // 9 PM - still high
			22: 1.2,  // 10 PM - winding down
			23: 0.6,  // 11 PM
		},
	}

	// UniformPattern has constant load throughout the day.
	UniformPattern = &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0,
			6: 1.0, 7: 1.0, 8: 1.0, 9: 1.0, 10: 1.0, 11: 1.0,
			12: 1.0, 13: 1.0, 14: 1.0, 15: 1.0, 16: 1.0, 17: 1.0,
			18: 1.0, 19: 1.0, 20: 1.0, 21: 1.0, 22: 1.0, 23: 1.0,
		},
	}

	// BatchProcessingPattern simulates overnight batch jobs.
	// Heavy load at night, minimal during business hours.
	BatchProcessingPattern = &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  2.0,  // Midnight - batch jobs running
			1:  2.5,  // 1 AM - peak batch processing
			2:  2.5,  // 2 AM
			3:  2.0,  // 3 AM
			4:  1.5,  // 4 AM - winding down
			5:  1.0,  // 5 AM
			6:  0.5,  // 6 AM - minimal
			7:  0.2,  // 7 AM
			8:  0.15, // 8 AM - business hours start
			9:  0.1,  // 9 AM - minimum
			10: 0.1,  // 10 AM
			11: 0.1,  // 11 AM
			12: 0.1,  // 12 PM
			13: 0.1,  // 1 PM
			14: 0.1,  // 2 PM
			15: 0.1,  // 3 PM
			16: 0.1,  // 4 PM
			17: 0.15, // 5 PM
			18: 0.2,  // 6 PM - preparing for batch
			19: 0.3,  // 7 PM
			20: 0.5,  // 8 PM
			21: 0.8,  // 9 PM - batch jobs starting
			22: 1.2,  // 10 PM
			23: 1.5,  // 11 PM - ramping up
		},
	}

	// WeekendPattern simulates weekend traffic - lower overall.
	WeekendPattern = &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  0.2,  // Midnight
			1:  0.15, // 1 AM
			2:  0.1,  // 2 AM
			3:  0.1,  // 3 AM
			4:  0.1,  // 4 AM
			5:  0.1,  // 5 AM
			6:  0.15, // 6 AM
			7:  0.2,  // 7 AM
			8:  0.3,  // 8 AM - late wake up
			9:  0.4,  // 9 AM
			10: 0.5,  // 10 AM
			11: 0.6,  // 11 AM - brunch time
			12: 0.7,  // 12 PM
			13: 0.6,  // 1 PM
			14: 0.5,  // 2 PM - afternoon lull
			15: 0.5,  // 3 PM
			16: 0.6,  // 4 PM
			17: 0.7,  // 5 PM
			18: 0.8,  // 6 PM - evening activity
			19: 0.9,  // 7 PM
			20: 0.8,  // 8 PM
			21: 0.6,  // 9 PM
			22: 0.4,  // 10 PM
			23: 0.3,  // 11 PM
		},
	}
)

// presetPatterns maps preset names to patterns.
var presetPatterns = map[string]*LoadPattern{
	"production":       ProductionDayPattern,
	"production_day":   ProductionDayPattern,
	"office":           ProductionDayPattern,
	"ecommerce":        EcommercePattern,
	"retail":           EcommercePattern,
	"uniform":          UniformPattern,
	"constant":         UniformPattern,
	"flat":             UniformPattern,
	"batch":            BatchProcessingPattern,
	"batch_processing": BatchProcessingPattern,
	"night":            BatchProcessingPattern,
	"weekend":          WeekendPattern,
}

// GetPresetPattern returns a clone of a predefined pattern by name.
// Available presets: production, ecommerce, uniform, batch, weekend
func GetPresetPattern(name string) (*LoadPattern, error) {
	pattern, ok := presetPatterns[name]
	if !ok {
		return nil, fmt.Errorf("unknown preset pattern: %s (available: production, ecommerce, uniform, batch, weekend)", name)
	}
	// Return a clone to prevent modification of the original
	return pattern.Clone(), nil
}

// ListPresets returns all available preset names.
func ListPresets() []string {
	return []string{
		"production",
		"ecommerce",
		"uniform",
		"batch",
		"weekend",
	}
}

// PresetDescription returns a description of a preset pattern.
func PresetDescription(name string) string {
	descriptions := map[string]string{
		"production": "Office hours workload (9-18 peak)",
		"ecommerce":  "E-commerce pattern (evening peak 19-21)",
		"uniform":    "Constant load throughout the day",
		"batch":      "Overnight batch processing (1-3 AM peak)",
		"weekend":    "Weekend traffic (lower overall, evening peak)",
	}
	if desc, ok := descriptions[name]; ok {
		return desc
	}
	return "Unknown preset"
}
