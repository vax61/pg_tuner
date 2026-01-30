package pattern

import (
	"math"
	"testing"
	"time"
)

func TestLoadPattern_GetMultiplier(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  0.2,
			9:  1.5,
			14: 2.0,
			23: 0.3,
		},
	}

	tests := []struct {
		hour     int
		expected float64
	}{
		{0, 0.2},
		{9, 1.5},
		{14, 2.0},
		{23, 0.3},
		{12, 1.0}, // Not defined, should return 1.0
		{6, 1.0},  // Not defined
	}

	for _, tc := range tests {
		got := pattern.GetMultiplier(tc.hour)
		if got != tc.expected {
			t.Errorf("GetMultiplier(%d) = %f, want %f", tc.hour, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetMultiplier_AllHours(t *testing.T) {
	// Test with full 24-hour pattern
	pattern := ProductionDayPattern.Clone()

	for hour := 0; hour < 24; hour++ {
		mult := pattern.GetMultiplier(hour)
		if mult < pattern.MinMultiplier || mult > pattern.MaxMultiplier {
			t.Errorf("GetMultiplier(%d) = %f, outside bounds [%f, %f]",
				hour, mult, pattern.MinMultiplier, pattern.MaxMultiplier)
		}
	}
}

func TestLoadPattern_GetMultiplier_NormalizeHour(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 0.5,
			5: 1.5,
		},
	}

	// Test hour normalization
	tests := []struct {
		hour     int
		expected float64
	}{
		{24, 0.5},  // 24 -> 0
		{25, 1.0},  // 25 -> 1 (not defined)
		{29, 1.5},  // 29 -> 5
		{-1, 1.0},  // -1 -> 23 (not defined)
		{-24, 0.5}, // -24 -> 0
	}

	for _, tc := range tests {
		got := pattern.GetMultiplier(tc.hour)
		if got != tc.expected {
			t.Errorf("GetMultiplier(%d) = %f, want %f", tc.hour, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetMultiplier_Clamping(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.5,
		MaxMultiplier: 2.0,
		HourlyMultipliers: map[int]float64{
			0: 0.1,  // Below min
			1: 5.0,  // Above max
			2: 1.0,  // Within bounds
		},
	}

	tests := []struct {
		hour     int
		expected float64
	}{
		{0, 0.5}, // Clamped to min
		{1, 2.0}, // Clamped to max
		{2, 1.0}, // Unchanged
	}

	for _, tc := range tests {
		got := pattern.GetMultiplier(tc.hour)
		if got != tc.expected {
			t.Errorf("GetMultiplier(%d) = %f, want %f (with clamping)", tc.hour, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetTargetQPS(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0:  0.5,
			9:  2.0,
			14: 1.5,
		},
	}

	tests := []struct {
		hour     int
		expected int
	}{
		{0, 50},   // 100 * 0.5
		{9, 200},  // 100 * 2.0
		{14, 150}, // 100 * 1.5
		{12, 100}, // 100 * 1.0 (default)
	}

	for _, tc := range tests {
		got := pattern.GetTargetQPS(tc.hour)
		if got != tc.expected {
			t.Errorf("GetTargetQPS(%d) = %d, want %d", tc.hour, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetTargetQPS_MinimumOne(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   10,
		MinMultiplier: 0.01,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			0: 0.01, // Would result in 0.1 QPS
		},
	}

	got := pattern.GetTargetQPS(0)
	if got < 1 {
		t.Errorf("GetTargetQPS should return at least 1, got %d", got)
	}
}

func TestLoadPattern_Validate(t *testing.T) {
	tests := []struct {
		name    string
		pattern *LoadPattern
		wantErr bool
	}{
		{
			name: "valid pattern",
			pattern: &LoadPattern{
				Type:              "hourly",
				BaselineQPS:       100,
				MinMultiplier:     0.1,
				MaxMultiplier:     10.0,
				HourlyMultipliers: map[int]float64{0: 0.5, 12: 1.5},
			},
			wantErr: false,
		},
		{
			name: "invalid type",
			pattern: &LoadPattern{
				Type:          "invalid",
				BaselineQPS:   100,
				MinMultiplier: 0.1,
				MaxMultiplier: 10.0,
			},
			wantErr: true,
		},
		{
			name: "zero baseline QPS",
			pattern: &LoadPattern{
				Type:          "hourly",
				BaselineQPS:   0,
				MinMultiplier: 0.1,
				MaxMultiplier: 10.0,
			},
			wantErr: true,
		},
		{
			name: "negative baseline QPS",
			pattern: &LoadPattern{
				Type:          "hourly",
				BaselineQPS:   -100,
				MinMultiplier: 0.1,
				MaxMultiplier: 10.0,
			},
			wantErr: true,
		},
		{
			name: "negative min multiplier",
			pattern: &LoadPattern{
				Type:          "hourly",
				BaselineQPS:   100,
				MinMultiplier: -0.1,
				MaxMultiplier: 10.0,
			},
			wantErr: true,
		},
		{
			name: "zero max multiplier",
			pattern: &LoadPattern{
				Type:          "hourly",
				BaselineQPS:   100,
				MinMultiplier: 0.1,
				MaxMultiplier: 0,
			},
			wantErr: true,
		},
		{
			name: "min greater than max",
			pattern: &LoadPattern{
				Type:          "hourly",
				BaselineQPS:   100,
				MinMultiplier: 5.0,
				MaxMultiplier: 2.0,
			},
			wantErr: true,
		},
		{
			name: "invalid hour in multipliers",
			pattern: &LoadPattern{
				Type:              "hourly",
				BaselineQPS:       100,
				MinMultiplier:     0.1,
				MaxMultiplier:     10.0,
				HourlyMultipliers: map[int]float64{25: 1.0},
			},
			wantErr: true,
		},
		{
			name: "negative hour in multipliers",
			pattern: &LoadPattern{
				Type:              "hourly",
				BaselineQPS:       100,
				MinMultiplier:     0.1,
				MaxMultiplier:     10.0,
				HourlyMultipliers: map[int]float64{-1: 1.0},
			},
			wantErr: true,
		},
		{
			name: "negative multiplier value",
			pattern: &LoadPattern{
				Type:              "hourly",
				BaselineQPS:       100,
				MinMultiplier:     0.1,
				MaxMultiplier:     10.0,
				HourlyMultipliers: map[int]float64{12: -0.5},
			},
			wantErr: true,
		},
		{
			name: "custom type valid",
			pattern: &LoadPattern{
				Type:          "custom",
				BaselineQPS:   50,
				MinMultiplier: 0.5,
				MaxMultiplier: 5.0,
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.pattern.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLoadPattern_GetMultiplierSmooth(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0,
			10: 2.0,
		},
	}

	tests := []struct {
		hour     int
		minute   int
		expected float64
	}{
		{9, 0, 1.0},   // Start of hour 9
		{9, 30, 1.5},  // Midpoint between 1.0 and 2.0
		{9, 15, 1.25}, // Quarter way
		{9, 45, 1.75}, // Three-quarters
		{10, 0, 2.0},  // Start of hour 10
	}

	for _, tc := range tests {
		tm := time.Date(2024, 1, 1, tc.hour, tc.minute, 0, 0, time.UTC)
		got := pattern.GetMultiplierSmooth(tm)
		if math.Abs(got-tc.expected) > 0.01 {
			t.Errorf("GetMultiplierSmooth(%d:%02d) = %f, want %f", tc.hour, tc.minute, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetMultiplierSmooth_WrapAround(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			23: 0.5,
			0:  1.0,
		},
	}

	// Test interpolation from 23:00 to 00:00
	tests := []struct {
		hour     int
		minute   int
		expected float64
	}{
		{23, 0, 0.5},   // Start
		{23, 30, 0.75}, // Midpoint
	}

	for _, tc := range tests {
		tm := time.Date(2024, 1, 1, tc.hour, tc.minute, 0, 0, time.UTC)
		got := pattern.GetMultiplierSmooth(tm)
		if math.Abs(got-tc.expected) > 0.01 {
			t.Errorf("GetMultiplierSmooth(%d:%02d) = %f, want %f", tc.hour, tc.minute, got, tc.expected)
		}
	}
}

func TestLoadPattern_GetMultiplierAt(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0,
			10: 2.0,
		},
	}

	got := pattern.GetMultiplierAt(9, 30)
	expected := 1.5
	if math.Abs(got-expected) > 0.01 {
		t.Errorf("GetMultiplierAt(9, 30) = %f, want %f", got, expected)
	}
}

func TestLoadPattern_GetTargetQPSSmooth(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9:  1.0, // 100 QPS
			10: 2.0, // 200 QPS
		},
	}

	tm := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
	got := pattern.GetTargetQPSSmooth(tm)
	expected := 150 // Midpoint

	if got != expected {
		t.Errorf("GetTargetQPSSmooth(9:30) = %d, want %d", got, expected)
	}
}

func TestParseLoadPattern_Valid(t *testing.T) {
	yaml := `
load_pattern:
  type: hourly
  baseline_qps: 200
  min_multiplier: 0.2
  max_multiplier: 5.0
  hourly_multipliers:
    0: 0.3
    9: 1.5
    14: 2.0
    23: 0.4
`
	pattern, err := ParseLoadPattern([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseLoadPattern() error = %v", err)
	}

	if pattern.Type != "hourly" {
		t.Errorf("Type = %s, want hourly", pattern.Type)
	}
	if pattern.BaselineQPS != 200 {
		t.Errorf("BaselineQPS = %d, want 200", pattern.BaselineQPS)
	}
	if pattern.MinMultiplier != 0.2 {
		t.Errorf("MinMultiplier = %f, want 0.2", pattern.MinMultiplier)
	}
	if pattern.MaxMultiplier != 5.0 {
		t.Errorf("MaxMultiplier = %f, want 5.0", pattern.MaxMultiplier)
	}
	if pattern.HourlyMultipliers[9] != 1.5 {
		t.Errorf("HourlyMultipliers[9] = %f, want 1.5", pattern.HourlyMultipliers[9])
	}
}

func TestParseLoadPattern_DirectFormat(t *testing.T) {
	yaml := `
type: hourly
baseline_qps: 150
hourly_multipliers:
  12: 1.5
`
	pattern, err := ParseLoadPattern([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseLoadPattern() error = %v", err)
	}

	if pattern.BaselineQPS != 150 {
		t.Errorf("BaselineQPS = %d, want 150", pattern.BaselineQPS)
	}
	// Should have default min/max
	if pattern.MinMultiplier != 0.1 {
		t.Errorf("MinMultiplier = %f, want 0.1 (default)", pattern.MinMultiplier)
	}
}

func TestParseLoadPattern_Invalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "invalid YAML syntax",
			yaml: `
load_pattern:
  type: hourly
  baseline_qps: invalid
`,
		},
		{
			name: "invalid type",
			yaml: `
load_pattern:
  type: unknown
  baseline_qps: 100
`,
		},
		{
			name: "zero baseline",
			yaml: `
load_pattern:
  type: hourly
  baseline_qps: 0
`,
		},
		{
			name: "invalid hour",
			yaml: `
load_pattern:
  type: hourly
  baseline_qps: 100
  hourly_multipliers:
    30: 1.5
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseLoadPattern([]byte(tc.yaml))
			if err == nil {
				t.Error("ParseLoadPattern() expected error, got nil")
			}
		})
	}
}

func TestGetPresetPattern(t *testing.T) {
	presets := []string{"production", "ecommerce", "uniform", "batch", "weekend"}

	for _, name := range presets {
		t.Run(name, func(t *testing.T) {
			pattern, err := GetPresetPattern(name)
			if err != nil {
				t.Fatalf("GetPresetPattern(%s) error = %v", name, err)
			}
			if pattern == nil {
				t.Fatalf("GetPresetPattern(%s) returned nil", name)
			}
			if err := pattern.Validate(); err != nil {
				t.Errorf("Preset %s failed validation: %v", name, err)
			}
		})
	}
}

func TestGetPresetPattern_Clone(t *testing.T) {
	// Ensure presets return clones, not originals
	p1, _ := GetPresetPattern("production")
	p2, _ := GetPresetPattern("production")

	p1.BaselineQPS = 999

	if p2.BaselineQPS == 999 {
		t.Error("GetPresetPattern should return clones, not references")
	}
}

func TestGetPresetPattern_Aliases(t *testing.T) {
	aliases := map[string]string{
		"production_day": "production",
		"office":         "production",
		"retail":         "ecommerce",
		"constant":       "uniform",
		"flat":           "uniform",
		"night":          "batch",
	}

	for alias, primary := range aliases {
		t.Run(alias, func(t *testing.T) {
			p1, err1 := GetPresetPattern(alias)
			p2, err2 := GetPresetPattern(primary)

			if err1 != nil || err2 != nil {
				t.Fatalf("GetPresetPattern errors: %v, %v", err1, err2)
			}

			// Should have same values
			if p1.BaselineQPS != p2.BaselineQPS {
				t.Errorf("Alias %s has different BaselineQPS than %s", alias, primary)
			}
		})
	}
}

func TestGetPresetPattern_Unknown(t *testing.T) {
	_, err := GetPresetPattern("nonexistent")
	if err == nil {
		t.Error("GetPresetPattern(nonexistent) expected error, got nil")
	}
}

func TestPresetPatterns_Validate(t *testing.T) {
	// Verify all built-in patterns are valid
	patterns := []*LoadPattern{
		ProductionDayPattern,
		EcommercePattern,
		UniformPattern,
		BatchProcessingPattern,
		WeekendPattern,
	}

	for i, p := range patterns {
		if err := p.Validate(); err != nil {
			t.Errorf("Built-in pattern %d failed validation: %v", i, err)
		}
	}
}

func TestPresetPatterns_Complete(t *testing.T) {
	// Verify all presets have all 24 hours defined
	patterns := []*LoadPattern{
		ProductionDayPattern,
		EcommercePattern,
		UniformPattern,
		BatchProcessingPattern,
		WeekendPattern,
	}

	for _, p := range patterns {
		if len(p.HourlyMultipliers) != 24 {
			t.Errorf("Pattern %s has %d hours, want 24", p.Type, len(p.HourlyMultipliers))
		}
	}
}

func TestLoadPattern_Clone(t *testing.T) {
	original := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.2,
		MaxMultiplier: 5.0,
		HourlyMultipliers: map[int]float64{
			9:  1.5,
			14: 2.0,
		},
	}

	clone := original.Clone()

	// Modify clone
	clone.BaselineQPS = 999
	clone.HourlyMultipliers[9] = 9.9

	// Original should be unchanged
	if original.BaselineQPS != 100 {
		t.Errorf("Clone modified original BaselineQPS")
	}
	if original.HourlyMultipliers[9] != 1.5 {
		t.Errorf("Clone modified original HourlyMultipliers")
	}
}

func TestLoadPattern_SetDefaults(t *testing.T) {
	p := &LoadPattern{}
	p.SetDefaults()

	if p.Type != "hourly" {
		t.Errorf("Type = %s, want hourly", p.Type)
	}
	if p.MinMultiplier != 0.1 {
		t.Errorf("MinMultiplier = %f, want 0.1", p.MinMultiplier)
	}
	if p.MaxMultiplier != 10.0 {
		t.Errorf("MaxMultiplier = %f, want 10.0", p.MaxMultiplier)
	}
	if p.HourlyMultipliers == nil {
		t.Error("HourlyMultipliers should not be nil")
	}
}

func TestLoadPattern_ToYAML(t *testing.T) {
	pattern := &LoadPattern{
		Type:          "hourly",
		BaselineQPS:   100,
		MinMultiplier: 0.1,
		MaxMultiplier: 10.0,
		HourlyMultipliers: map[int]float64{
			9: 1.5,
		},
	}

	data, err := pattern.ToYAML()
	if err != nil {
		t.Fatalf("ToYAML() error = %v", err)
	}

	// Parse it back
	parsed, err := ParseLoadPattern(data)
	if err != nil {
		t.Fatalf("Failed to parse serialized YAML: %v", err)
	}

	if parsed.BaselineQPS != pattern.BaselineQPS {
		t.Errorf("Round-trip failed: BaselineQPS = %d, want %d", parsed.BaselineQPS, pattern.BaselineQPS)
	}
}

func TestInterpolatedMultipliers(t *testing.T) {
	pattern := UniformPattern.Clone()

	// Get multipliers every 15 minutes
	multipliers := pattern.InterpolatedMultipliers(15 * time.Minute)

	// Should have 96 entries (24 hours * 4 per hour)
	expected := 96
	if len(multipliers) != expected {
		t.Errorf("InterpolatedMultipliers(15m) returned %d entries, want %d", len(multipliers), expected)
	}

	// All should be 1.0 for uniform pattern
	for _, m := range multipliers {
		if m.Multiplier != 1.0 {
			t.Errorf("Uniform pattern at %d:%02d has multiplier %f, want 1.0",
				m.Hour, m.Minute, m.Multiplier)
		}
	}
}

func TestListPresets(t *testing.T) {
	presets := ListPresets()
	if len(presets) == 0 {
		t.Error("ListPresets() returned empty list")
	}

	// Should contain known presets
	expected := map[string]bool{
		"production": true,
		"ecommerce":  true,
		"uniform":    true,
		"batch":      true,
		"weekend":    true,
	}

	for _, p := range presets {
		if !expected[p] {
			t.Errorf("Unexpected preset: %s", p)
		}
		delete(expected, p)
	}

	for p := range expected {
		t.Errorf("Missing preset: %s", p)
	}
}

func TestPresetDescription(t *testing.T) {
	for _, name := range ListPresets() {
		desc := PresetDescription(name)
		if desc == "" || desc == "Unknown preset" {
			t.Errorf("PresetDescription(%s) returned invalid description", name)
		}
	}
}
