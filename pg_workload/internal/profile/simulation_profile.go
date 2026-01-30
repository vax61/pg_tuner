package profile

import (
	"fmt"

	"github.com/myorg/pg_tuner/pg_workload/internal/pattern"
)

// SimulationProfile represents a complete simulation configuration.
type SimulationProfile struct {
	// Profile metadata
	Name        string `yaml:"name" json:"name"`
	Mode        string `yaml:"mode" json:"mode"`
	Description string `yaml:"description" json:"description"`

	// Load pattern for time-varying load
	LoadPattern *pattern.LoadPattern `yaml:"load_pattern" json:"load_pattern"`

	// Workload distribution
	WorkloadDistribution WorkloadDist `yaml:"workload_distribution" json:"workload_distribution"`

	// Connection pattern
	ConnectionPattern ConnectionPattern `yaml:"connection_pattern" json:"connection_pattern"`
}

// WorkloadDist defines the read/write distribution.
type WorkloadDist struct {
	Read  int `yaml:"read" json:"read"`   // Percentage of reads (0-100)
	Write int `yaml:"write" json:"write"` // Percentage of writes (0-100)
}

// ConnectionPattern defines how connections scale.
type ConnectionPattern struct {
	MinConnections int  `yaml:"min_connections" json:"min_connections"`
	MaxConnections int  `yaml:"max_connections" json:"max_connections"`
	ScaleWithLoad  bool `yaml:"scale_with_load" json:"scale_with_load"`
}

// NewSimulationProfile creates a new SimulationProfile with defaults.
func NewSimulationProfile() *SimulationProfile {
	return &SimulationProfile{
		Mode: "simulation",
		LoadPattern: &pattern.LoadPattern{
			Type:              "hourly",
			BaselineQPS:       100,
			MinMultiplier:     0.1,
			MaxMultiplier:     10.0,
			HourlyMultipliers: make(map[int]float64),
		},
		WorkloadDistribution: WorkloadDist{
			Read:  70,
			Write: 30,
		},
		ConnectionPattern: ConnectionPattern{
			MinConnections: 5,
			MaxConnections: 50,
			ScaleWithLoad:  true,
		},
	}
}

// Validate checks if the profile is valid.
func (p *SimulationProfile) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("profile name is required")
	}

	if p.Mode != "simulation" {
		return fmt.Errorf("invalid mode: %s (must be 'simulation')", p.Mode)
	}

	// Validate load pattern
	if p.LoadPattern == nil {
		return fmt.Errorf("load_pattern is required")
	}
	if err := p.LoadPattern.Validate(); err != nil {
		return fmt.Errorf("invalid load_pattern: %w", err)
	}

	// Validate workload distribution
	if err := p.WorkloadDistribution.Validate(); err != nil {
		return fmt.Errorf("invalid workload_distribution: %w", err)
	}

	// Validate connection pattern
	if err := p.ConnectionPattern.Validate(); err != nil {
		return fmt.Errorf("invalid connection_pattern: %w", err)
	}

	return nil
}

// Validate checks if the workload distribution is valid.
func (w *WorkloadDist) Validate() error {
	if w.Read < 0 || w.Read > 100 {
		return fmt.Errorf("read percentage must be 0-100, got %d", w.Read)
	}
	if w.Write < 0 || w.Write > 100 {
		return fmt.Errorf("write percentage must be 0-100, got %d", w.Write)
	}
	if w.Read+w.Write != 100 {
		return fmt.Errorf("read + write must equal 100, got %d", w.Read+w.Write)
	}
	return nil
}

// Validate checks if the connection pattern is valid.
func (c *ConnectionPattern) Validate() error {
	if c.MinConnections < 1 {
		return fmt.Errorf("min_connections must be at least 1, got %d", c.MinConnections)
	}
	if c.MaxConnections < c.MinConnections {
		return fmt.Errorf("max_connections (%d) must be >= min_connections (%d)",
			c.MaxConnections, c.MinConnections)
	}
	return nil
}

// SetDefaults ensures default values are set.
func (p *SimulationProfile) SetDefaults() {
	if p.Mode == "" {
		p.Mode = "simulation"
	}

	if p.LoadPattern == nil {
		p.LoadPattern = &pattern.LoadPattern{
			Type:              "hourly",
			BaselineQPS:       100,
			MinMultiplier:     0.1,
			MaxMultiplier:     10.0,
			HourlyMultipliers: make(map[int]float64),
		}
	}
	p.LoadPattern.SetDefaults()

	if p.WorkloadDistribution.Read == 0 && p.WorkloadDistribution.Write == 0 {
		p.WorkloadDistribution.Read = 70
		p.WorkloadDistribution.Write = 30
	}

	if p.ConnectionPattern.MinConnections == 0 {
		p.ConnectionPattern.MinConnections = 5
	}
	if p.ConnectionPattern.MaxConnections == 0 {
		p.ConnectionPattern.MaxConnections = 50
	}
}

// GetTargetConnections returns the target connections for a given multiplier.
func (p *SimulationProfile) GetTargetConnections(multiplier float64) int {
	if !p.ConnectionPattern.ScaleWithLoad {
		return p.ConnectionPattern.MaxConnections
	}

	// Scale connections proportionally
	connRange := p.ConnectionPattern.MaxConnections - p.ConnectionPattern.MinConnections
	scaled := p.ConnectionPattern.MinConnections + int(float64(connRange)*multiplier)

	if scaled < p.ConnectionPattern.MinConnections {
		return p.ConnectionPattern.MinConnections
	}
	if scaled > p.ConnectionPattern.MaxConnections {
		return p.ConnectionPattern.MaxConnections
	}

	return scaled
}

// Clone creates a deep copy of the profile.
func (p *SimulationProfile) Clone() *SimulationProfile {
	clone := &SimulationProfile{
		Name:                 p.Name,
		Mode:                 p.Mode,
		Description:          p.Description,
		WorkloadDistribution: p.WorkloadDistribution,
		ConnectionPattern:    p.ConnectionPattern,
	}

	if p.LoadPattern != nil {
		clone.LoadPattern = p.LoadPattern.Clone()
	}

	return clone
}
