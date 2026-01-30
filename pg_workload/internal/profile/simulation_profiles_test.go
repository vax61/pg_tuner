package profile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
)

func TestLoadSimulationProfile_AllEmbedded(t *testing.T) {
	profiles := []string{
		"production_day",
		"ecommerce",
		"batch_heavy",
		"24x7_uniform",
		"financial",
	}

	for _, name := range profiles {
		t.Run(name, func(t *testing.T) {
			profile, err := LoadSimulationProfile(name)
			if err != nil {
				t.Fatalf("Failed to load profile %s: %v", name, err)
			}

			if profile.Name == "" {
				t.Errorf("Profile %s has empty name", name)
			}

			if profile.Mode != "simulation" {
				t.Errorf("Profile %s has mode %s, want simulation", name, profile.Mode)
			}

			if profile.LoadPattern == nil {
				t.Errorf("Profile %s has nil LoadPattern", name)
			}

			if err := profile.Validate(); err != nil {
				t.Errorf("Profile %s validation failed: %v", name, err)
			}
		})
	}
}

func TestLoadSimulationProfile_Aliases(t *testing.T) {
	aliases := map[string]string{
		"production": "production_day",
		"office":     "production_day",
		"retail":     "ecommerce",
		"b2c":        "ecommerce",
		"etl":        "batch_heavy",
		"batch":      "batch_heavy",
		"uniform":    "24x7_uniform",
		"constant":   "24x7_uniform",
		"24x7":       "24x7_uniform",
		"trading":    "financial",
		"banking":    "financial",
	}

	for alias, expected := range aliases {
		t.Run(alias, func(t *testing.T) {
			profile, err := LoadSimulationProfile(alias)
			if err != nil {
				t.Fatalf("Failed to load alias %s: %v", alias, err)
			}

			expectedProfile, _ := LoadSimulationProfile(expected)
			if profile.Name != expectedProfile.Name {
				t.Errorf("Alias %s loaded %s, expected %s", alias, profile.Name, expectedProfile.Name)
			}
		})
	}
}

func TestLoadSimulationProfile_NotFound(t *testing.T) {
	_, err := LoadSimulationProfile("nonexistent_profile")
	if err == nil {
		t.Error("Expected error for nonexistent profile, got nil")
	}
}

func TestLoadSimulationProfile_FromFile(t *testing.T) {
	// Create a temporary profile file
	tmpDir := t.TempDir()
	profilePath := filepath.Join(tmpDir, "test_profile.yaml")

	content := `
profile:
  name: "Test Profile"
  mode: simulation
  description: "A test profile"

load_pattern:
  type: hourly
  baseline_qps: 50
  hourly_multipliers:
    0: 1.0
    12: 2.0

workload_distribution:
  read: 80
  write: 20

connection_pattern:
  min_connections: 2
  max_connections: 20
  scale_with_load: true
`

	if err := os.WriteFile(profilePath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test profile: %v", err)
	}

	profile, err := LoadSimulationProfileFromFile(profilePath)
	if err != nil {
		t.Fatalf("Failed to load profile from file: %v", err)
	}

	if profile.Name != "Test Profile" {
		t.Errorf("Name = %s, want 'Test Profile'", profile.Name)
	}

	if profile.LoadPattern.BaselineQPS != 50 {
		t.Errorf("BaselineQPS = %d, want 50", profile.LoadPattern.BaselineQPS)
	}

	if profile.WorkloadDistribution.Read != 80 {
		t.Errorf("Read = %d, want 80", profile.WorkloadDistribution.Read)
	}
}

func TestLoadSimulationProfile_FromSearchPath(t *testing.T) {
	// Add temp dir to search path
	tmpDir := t.TempDir()
	AddProfileSearchPath(tmpDir)

	// Create profile file
	profilePath := filepath.Join(tmpDir, "custom_test.yaml")
	content := `
profile:
  name: "Custom Test"
  mode: simulation
  description: "Custom test profile"

load_pattern:
  type: hourly
  baseline_qps: 75

workload_distribution:
  read: 60
  write: 40

connection_pattern:
  min_connections: 5
  max_connections: 25
`

	if err := os.WriteFile(profilePath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test profile: %v", err)
	}

	profile, err := LoadSimulationProfile("custom_test")
	if err != nil {
		t.Fatalf("Failed to load profile: %v", err)
	}

	if profile.Name != "Custom Test" {
		t.Errorf("Name = %s, want 'Custom Test'", profile.Name)
	}
}

func TestParseSimulationProfile_Valid(t *testing.T) {
	yaml := `
profile:
  name: "Parsed Profile"
  mode: simulation
  description: "Test parsing"

load_pattern:
  type: hourly
  baseline_qps: 100
  hourly_multipliers:
    9: 1.5
    14: 2.0

workload_distribution:
  read: 70
  write: 30

connection_pattern:
  min_connections: 5
  max_connections: 50
  scale_with_load: true
`

	profile, err := ParseSimulationProfile([]byte(yaml))
	if err != nil {
		t.Fatalf("Failed to parse profile: %v", err)
	}

	if profile.Name != "Parsed Profile" {
		t.Errorf("Name = %s, want 'Parsed Profile'", profile.Name)
	}

	if profile.LoadPattern.HourlyMultipliers[9] != 1.5 {
		t.Errorf("HourlyMultipliers[9] = %f, want 1.5", profile.LoadPattern.HourlyMultipliers[9])
	}
}

func TestParseSimulationProfile_Invalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "invalid mode",
			yaml: `
profile:
  name: "Bad Profile"
  mode: burst
`,
		},
		{
			name: "invalid workload distribution",
			yaml: `
profile:
  name: "Bad Profile"
  mode: simulation
workload_distribution:
  read: 60
  write: 60
`,
		},
		{
			name: "invalid connection pattern",
			yaml: `
profile:
  name: "Bad Profile"
  mode: simulation
connection_pattern:
  min_connections: 50
  max_connections: 10
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSimulationProfile([]byte(tc.yaml))
			if err == nil {
				t.Error("Expected validation error, got nil")
			}
		})
	}
}

func TestListSimulationProfiles(t *testing.T) {
	profiles := ListSimulationProfiles()

	// Should include all embedded profiles
	expected := map[string]bool{
		"production_day": false,
		"ecommerce":      false,
		"batch_heavy":    false,
		"24x7_uniform":   false,
		"financial":      false,
	}

	for _, name := range profiles {
		if _, ok := expected[name]; ok {
			expected[name] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("Expected profile %s not in list", name)
		}
	}
}

func TestValidateSimulationProfile(t *testing.T) {
	// Valid profile
	valid := &SimulationProfile{
		Name: "Valid",
		Mode: "simulation",
		LoadPattern: &pattern.LoadPattern{
			Type:        "hourly",
			BaselineQPS: 100,
		},
		WorkloadDistribution: WorkloadDist{Read: 70, Write: 30},
		ConnectionPattern:    ConnectionPattern{MinConnections: 5, MaxConnections: 50},
	}
	valid.LoadPattern.SetDefaults()

	if err := ValidateSimulationProfile(valid); err != nil {
		t.Errorf("Valid profile failed validation: %v", err)
	}

	// Nil profile
	if err := ValidateSimulationProfile(nil); err == nil {
		t.Error("Expected error for nil profile")
	}

	// Invalid profile
	invalid := &SimulationProfile{
		Name: "",
		Mode: "simulation",
	}
	if err := ValidateSimulationProfile(invalid); err == nil {
		t.Error("Expected error for invalid profile")
	}
}

func TestSimulationProfile_Validate(t *testing.T) {
	tests := []struct {
		name    string
		profile *SimulationProfile
		wantErr bool
	}{
		{
			name: "valid profile",
			profile: &SimulationProfile{
				Name: "Test",
				Mode: "simulation",
				LoadPattern: &pattern.LoadPattern{
					Type:              "hourly",
					BaselineQPS:       100,
					MinMultiplier:     0.1,
					MaxMultiplier:     10.0,
					HourlyMultipliers: map[int]float64{0: 1.0},
				},
				WorkloadDistribution: WorkloadDist{Read: 70, Write: 30},
				ConnectionPattern:    ConnectionPattern{MinConnections: 5, MaxConnections: 50},
			},
			wantErr: false,
		},
		{
			name: "empty name",
			profile: &SimulationProfile{
				Name:        "",
				Mode:        "simulation",
				LoadPattern: &pattern.LoadPattern{Type: "hourly", BaselineQPS: 100},
			},
			wantErr: true,
		},
		{
			name: "wrong mode",
			profile: &SimulationProfile{
				Name:        "Test",
				Mode:        "burst",
				LoadPattern: &pattern.LoadPattern{Type: "hourly", BaselineQPS: 100},
			},
			wantErr: true,
		},
		{
			name: "nil load pattern",
			profile: &SimulationProfile{
				Name:        "Test",
				Mode:        "simulation",
				LoadPattern: nil,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.profile.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestWorkloadDist_Validate(t *testing.T) {
	tests := []struct {
		name    string
		dist    WorkloadDist
		wantErr bool
	}{
		{name: "valid 70/30", dist: WorkloadDist{Read: 70, Write: 30}, wantErr: false},
		{name: "valid 50/50", dist: WorkloadDist{Read: 50, Write: 50}, wantErr: false},
		{name: "valid 100/0", dist: WorkloadDist{Read: 100, Write: 0}, wantErr: false},
		{name: "valid 0/100", dist: WorkloadDist{Read: 0, Write: 100}, wantErr: false},
		{name: "sum not 100", dist: WorkloadDist{Read: 60, Write: 60}, wantErr: true},
		{name: "negative read", dist: WorkloadDist{Read: -10, Write: 110}, wantErr: true},
		{name: "read over 100", dist: WorkloadDist{Read: 110, Write: -10}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.dist.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestConnectionPattern_Validate(t *testing.T) {
	tests := []struct {
		name    string
		pattern ConnectionPattern
		wantErr bool
	}{
		{name: "valid", pattern: ConnectionPattern{MinConnections: 5, MaxConnections: 50}, wantErr: false},
		{name: "min equals max", pattern: ConnectionPattern{MinConnections: 10, MaxConnections: 10}, wantErr: false},
		{name: "min zero", pattern: ConnectionPattern{MinConnections: 0, MaxConnections: 50}, wantErr: true},
		{name: "max less than min", pattern: ConnectionPattern{MinConnections: 50, MaxConnections: 10}, wantErr: true},
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

func TestSimulationProfile_GetTargetConnections(t *testing.T) {
	profile := &SimulationProfile{
		ConnectionPattern: ConnectionPattern{
			MinConnections: 10,
			MaxConnections: 100,
			ScaleWithLoad:  true,
		},
	}

	tests := []struct {
		mult     float64
		expected int
	}{
		{0.0, 10},  // Minimum
		{0.5, 55},  // Midpoint
		{1.0, 100}, // Maximum
		{2.0, 100}, // Above max, clamped
	}

	for _, tc := range tests {
		got := profile.GetTargetConnections(tc.mult)
		if got != tc.expected {
			t.Errorf("GetTargetConnections(%f) = %d, want %d", tc.mult, got, tc.expected)
		}
	}
}

func TestSimulationProfile_GetTargetConnections_NoScale(t *testing.T) {
	profile := &SimulationProfile{
		ConnectionPattern: ConnectionPattern{
			MinConnections: 10,
			MaxConnections: 100,
			ScaleWithLoad:  false,
		},
	}

	// Should always return max when not scaling
	for _, mult := range []float64{0.0, 0.5, 1.0, 2.0} {
		got := profile.GetTargetConnections(mult)
		if got != 100 {
			t.Errorf("GetTargetConnections(%f) with ScaleWithLoad=false = %d, want 100", mult, got)
		}
	}
}

func TestSimulationProfile_Clone(t *testing.T) {
	original, _ := LoadSimulationProfile("production_day")

	clone := original.Clone()

	// Modify clone
	clone.Name = "Modified"
	clone.LoadPattern.BaselineQPS = 999

	// Original should be unchanged
	if original.Name == "Modified" {
		t.Error("Clone modified original name")
	}
	if original.LoadPattern.BaselineQPS == 999 {
		t.Error("Clone modified original load pattern")
	}
}

func TestProfileDescription(t *testing.T) {
	desc := ProfileDescription("production_day")
	if desc == "" || desc == "Unknown profile" {
		t.Error("ProfileDescription returned empty or unknown for valid profile")
	}

	desc = ProfileDescription("nonexistent")
	if desc != "Unknown profile" {
		t.Errorf("ProfileDescription for nonexistent profile = %s, want 'Unknown profile'", desc)
	}
}

func TestEmbeddedProfiles_AllValid(t *testing.T) {
	for name, profile := range embeddedProfiles {
		t.Run(name, func(t *testing.T) {
			if err := profile.Validate(); err != nil {
				t.Errorf("Embedded profile %s validation failed: %v", name, err)
			}

			// Check all 24 hours are defined
			if len(profile.LoadPattern.HourlyMultipliers) != 24 {
				t.Errorf("Profile %s has %d hours, want 24",
					name, len(profile.LoadPattern.HourlyMultipliers))
			}
		})
	}
}

func TestYAMLProfiles_Parseable(t *testing.T) {
	// Test that the YAML files in profiles/simulation/ are parseable
	profilesDir := "../../profiles/simulation"

	entries, err := os.ReadDir(profilesDir)
	if err != nil {
		t.Skipf("Skipping YAML file tests: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if name[0] == '.' {
			continue
		}

		t.Run(name, func(t *testing.T) {
			path := filepath.Join(profilesDir, name)
			profile, err := LoadSimulationProfileFromFile(path)
			if err != nil {
				t.Fatalf("Failed to load %s: %v", name, err)
			}

			if err := profile.Validate(); err != nil {
				t.Errorf("Profile %s validation failed: %v", name, err)
			}
		})
	}
}
