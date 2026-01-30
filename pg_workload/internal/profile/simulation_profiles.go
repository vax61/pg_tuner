package profile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
	"gopkg.in/yaml.v3"
)

// profileWrapper is used for parsing YAML with top-level keys.
type profileWrapper struct {
	Profile struct {
		Name        string `yaml:"name"`
		Mode        string `yaml:"mode"`
		Description string `yaml:"description"`
	} `yaml:"profile"`
	LoadPattern          *pattern.LoadPattern `yaml:"load_pattern"`
	WorkloadDistribution WorkloadDist         `yaml:"workload_distribution"`
	ConnectionPattern    ConnectionPattern    `yaml:"connection_pattern"`
}

// Default profiles directory paths to search.
var profileSearchPaths = []string{
	"profiles/simulation",
	"./profiles/simulation",
	"../profiles/simulation",
}

// LoadSimulationProfile loads a simulation profile by name.
// Searches in profile directories first, then falls back to embedded defaults.
func LoadSimulationProfile(name string) (*SimulationProfile, error) {
	// Normalize name
	name = strings.TrimSuffix(name, ".yaml")
	name = strings.TrimSuffix(name, ".yml")

	// Try to load from file first
	profile, err := loadProfileFromFile(name)
	if err == nil {
		return profile, nil
	}

	// Fall back to embedded profiles
	profile, err = getEmbeddedProfile(name)
	if err == nil {
		return profile, nil
	}

	return nil, fmt.Errorf("profile not found: %s", name)
}

// LoadSimulationProfileFromFile loads a simulation profile from a specific file path.
func LoadSimulationProfileFromFile(path string) (*SimulationProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	return ParseSimulationProfile(data)
}

// ParseSimulationProfile parses a simulation profile from YAML data.
func ParseSimulationProfile(data []byte) (*SimulationProfile, error) {
	var wrapper profileWrapper
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	profile := &SimulationProfile{
		Name:                 wrapper.Profile.Name,
		Mode:                 wrapper.Profile.Mode,
		Description:          wrapper.Profile.Description,
		LoadPattern:          wrapper.LoadPattern,
		WorkloadDistribution: wrapper.WorkloadDistribution,
		ConnectionPattern:    wrapper.ConnectionPattern,
	}

	profile.SetDefaults()

	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return profile, nil
}

// loadProfileFromFile attempts to load a profile from the filesystem.
func loadProfileFromFile(name string) (*SimulationProfile, error) {
	// Try each search path
	for _, basePath := range profileSearchPaths {
		// Try .yaml extension
		path := filepath.Join(basePath, name+".yaml")
		if _, err := os.Stat(path); err == nil {
			return LoadSimulationProfileFromFile(path)
		}

		// Try .yml extension
		path = filepath.Join(basePath, name+".yml")
		if _, err := os.Stat(path); err == nil {
			return LoadSimulationProfileFromFile(path)
		}
	}

	return nil, fmt.Errorf("profile file not found: %s", name)
}

// ListSimulationProfiles returns all available simulation profile names.
func ListSimulationProfiles() []string {
	profiles := make(map[string]bool)

	// Add embedded profile names
	for name := range embeddedProfiles {
		profiles[name] = true
	}

	// Scan filesystem for additional profiles
	for _, basePath := range profileSearchPaths {
		entries, err := os.ReadDir(basePath)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			name := entry.Name()
			if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
				name = strings.TrimSuffix(name, ".yaml")
				name = strings.TrimSuffix(name, ".yml")
				profiles[name] = true
			}
		}
	}

	// Convert to sorted slice
	result := make([]string, 0, len(profiles))
	for name := range profiles {
		result = append(result, name)
	}

	return result
}

// ValidateSimulationProfile validates a simulation profile.
func ValidateSimulationProfile(p *SimulationProfile) error {
	if p == nil {
		return fmt.Errorf("profile is nil")
	}
	return p.Validate()
}

// ProfileDescription returns a description of a profile by name.
func ProfileDescription(name string) string {
	profile, err := LoadSimulationProfile(name)
	if err != nil {
		return "Unknown profile"
	}
	return profile.Description
}

// getEmbeddedProfile returns an embedded profile by name.
func getEmbeddedProfile(name string) (*SimulationProfile, error) {
	profile, ok := embeddedProfiles[name]
	if !ok {
		// Try common aliases
		aliases := map[string]string{
			"production":     "production_day",
			"office":         "production_day",
			"retail":         "ecommerce",
			"b2c":            "ecommerce",
			"etl":            "batch_heavy",
			"batch":          "batch_heavy",
			"warehouse":      "batch_heavy",
			"uniform":        "24x7_uniform",
			"constant":       "24x7_uniform",
			"24x7":           "24x7_uniform",
			"trading":        "financial",
			"banking":        "financial",
			"market":         "financial",
		}

		if aliasName, hasAlias := aliases[name]; hasAlias {
			profile, ok = embeddedProfiles[aliasName]
		}
	}

	if !ok {
		return nil, fmt.Errorf("unknown embedded profile: %s", name)
	}

	return profile.Clone(), nil
}

// embeddedProfiles contains the default profiles.
var embeddedProfiles = map[string]*SimulationProfile{
	"production_day": {
		Name:        "Production Day",
		Mode:        "simulation",
		Description: "Typical office workload 9-18, minimal overnight",
		LoadPattern: &pattern.LoadPattern{
			Type:          "hourly",
			BaselineQPS:   100,
			MinMultiplier: 0.1,
			MaxMultiplier: 10.0,
			HourlyMultipliers: map[int]float64{
				0: 0.15, 1: 0.10, 2: 0.10, 3: 0.10, 4: 0.15, 5: 0.20,
				6: 0.40, 7: 0.70, 8: 1.20, 9: 1.50, 10: 1.40, 11: 1.30,
				12: 0.90, 13: 1.10, 14: 1.40, 15: 1.35, 16: 1.20, 17: 0.90,
				18: 0.50, 19: 0.30, 20: 0.25, 21: 0.20, 22: 0.18, 23: 0.15,
			},
		},
		WorkloadDistribution: WorkloadDist{Read: 75, Write: 25},
		ConnectionPattern:    ConnectionPattern{MinConnections: 5, MaxConnections: 50, ScaleWithLoad: true},
	},
	"ecommerce": {
		Name:        "E-commerce B2C",
		Mode:        "simulation",
		Description: "E-commerce pattern with evening peaks (19-22), higher weekend activity",
		LoadPattern: &pattern.LoadPattern{
			Type:          "hourly",
			BaselineQPS:   150,
			MinMultiplier: 0.1,
			MaxMultiplier: 10.0,
			HourlyMultipliers: map[int]float64{
				0: 0.25, 1: 0.15, 2: 0.10, 3: 0.10, 4: 0.10, 5: 0.15,
				6: 0.20, 7: 0.35, 8: 0.50, 9: 0.60, 10: 0.70, 11: 0.80,
				12: 1.00, 13: 0.90, 14: 0.85, 15: 0.90, 16: 1.00, 17: 1.20,
				18: 1.50, 19: 1.80, 20: 2.00, 21: 1.90, 22: 1.50, 23: 0.80,
			},
		},
		WorkloadDistribution: WorkloadDist{Read: 60, Write: 40},
		ConnectionPattern:    ConnectionPattern{MinConnections: 10, MaxConnections: 100, ScaleWithLoad: true},
	},
	"batch_heavy": {
		Name:        "Batch Heavy / ETL",
		Mode:        "simulation",
		Description: "ETL/Data Warehouse pattern with heavy overnight batch processing",
		LoadPattern: &pattern.LoadPattern{
			Type:          "hourly",
			BaselineQPS:   80,
			MinMultiplier: 0.1,
			MaxMultiplier: 10.0,
			HourlyMultipliers: map[int]float64{
				0: 1.80, 1: 2.00, 2: 2.00, 3: 1.90, 4: 1.70, 5: 1.50,
				6: 0.80, 7: 0.50, 8: 0.40, 9: 0.35, 10: 0.30, 11: 0.30,
				12: 0.30, 13: 0.30, 14: 0.35, 15: 0.40, 16: 0.45, 17: 0.50,
				18: 0.60, 19: 0.70, 20: 0.90, 21: 1.10, 22: 1.40, 23: 1.60,
			},
		},
		WorkloadDistribution: WorkloadDist{Read: 30, Write: 70},
		ConnectionPattern:    ConnectionPattern{MinConnections: 5, MaxConnections: 30, ScaleWithLoad: true},
	},
	"24x7_uniform": {
		Name:        "24x7 Uniform",
		Mode:        "simulation",
		Description: "Always-on services with constant load throughout the day",
		LoadPattern: &pattern.LoadPattern{
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
		},
		WorkloadDistribution: WorkloadDist{Read: 70, Write: 30},
		ConnectionPattern:    ConnectionPattern{MinConnections: 10, MaxConnections: 50, ScaleWithLoad: false},
	},
	"financial": {
		Name:        "Financial / Trading",
		Mode:        "simulation",
		Description: "Trading/Banking pattern with market open/close peaks",
		LoadPattern: &pattern.LoadPattern{
			Type:          "hourly",
			BaselineQPS:   200,
			MinMultiplier: 0.1,
			MaxMultiplier: 10.0,
			HourlyMultipliers: map[int]float64{
				0: 0.20, 1: 0.15, 2: 0.15, 3: 0.15, 4: 0.20, 5: 0.30,
				6: 0.50, 7: 0.80, 8: 1.80, 9: 2.00, 10: 1.50, 11: 1.20,
				12: 0.60, 13: 0.70, 14: 1.00, 15: 1.60, 16: 1.80, 17: 1.20,
				18: 0.70, 19: 0.50, 20: 0.40, 21: 0.30, 22: 0.25, 23: 0.20,
			},
		},
		WorkloadDistribution: WorkloadDist{Read: 65, Write: 35},
		ConnectionPattern:    ConnectionPattern{MinConnections: 20, MaxConnections: 100, ScaleWithLoad: true},
	},
}

// AddProfileSearchPath adds a directory to search for profiles.
func AddProfileSearchPath(path string) {
	profileSearchPaths = append([]string{path}, profileSearchPaths...)
}
