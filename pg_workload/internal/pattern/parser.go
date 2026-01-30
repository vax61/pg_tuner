package pattern

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// patternWrapper is used for parsing YAML with a top-level key.
type patternWrapper struct {
	LoadPattern *LoadPattern `yaml:"load_pattern"`
}

// ParseLoadPattern parses a LoadPattern from YAML data.
// Supports both wrapped format (with load_pattern: key) and direct format.
func ParseLoadPattern(data []byte) (*LoadPattern, error) {
	// First try parsing with wrapper
	var wrapper patternWrapper
	if err := yaml.Unmarshal(data, &wrapper); err == nil && wrapper.LoadPattern != nil {
		wrapper.LoadPattern.SetDefaults()
		if err := wrapper.LoadPattern.Validate(); err != nil {
			return nil, fmt.Errorf("validation failed: %w", err)
		}
		return wrapper.LoadPattern, nil
	}

	// Try parsing directly
	var pattern LoadPattern
	if err := yaml.Unmarshal(data, &pattern); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	pattern.SetDefaults()
	if err := pattern.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return &pattern, nil
}

// LoadPatternFromFile loads a LoadPattern from a YAML file.
func LoadPatternFromFile(path string) (*LoadPattern, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	pattern, err := ParseLoadPattern(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}

	return pattern, nil
}

// ToYAML serializes the pattern to YAML format.
func (p *LoadPattern) ToYAML() ([]byte, error) {
	wrapper := patternWrapper{LoadPattern: p}
	return yaml.Marshal(wrapper)
}

// ToYAMLDirect serializes the pattern to YAML without wrapper.
func (p *LoadPattern) ToYAMLDirect() ([]byte, error) {
	return yaml.Marshal(p)
}

// SaveToFile saves the pattern to a YAML file.
func (p *LoadPattern) SaveToFile(path string) error {
	data, err := p.ToYAML()
	if err != nil {
		return fmt.Errorf("failed to serialize pattern: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", path, err)
	}

	return nil
}
