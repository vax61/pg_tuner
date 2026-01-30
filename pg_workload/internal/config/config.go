package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete application configuration.
type Config struct {
	Database DatabaseConfig `yaml:"database"`
	Workload WorkloadConfig `yaml:"workload"`
	Output   OutputConfig   `yaml:"output"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
}

// WorkloadConfig holds workload execution settings.
type WorkloadConfig struct {
	Mode        string        `yaml:"mode"`
	Profile     string        `yaml:"profile"`
	Duration    time.Duration `yaml:"duration"`
	Warmup      time.Duration `yaml:"warmup"`
	Cooldown    time.Duration `yaml:"cooldown"`
	Connections int           `yaml:"connections"`
	Workers     int           `yaml:"workers"`
	Seed        int64         `yaml:"seed"`
}

// OutputConfig holds output settings.
type OutputConfig struct {
	File   string `yaml:"file"`
	Format string `yaml:"format"`
}

// LoadConfig reads configuration from a YAML file and applies environment overrides.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := LoadConfigWithDefaults()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	applyEnvOverrides(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

// LoadConfigWithDefaults returns a Config with default values.
func LoadConfigWithDefaults() *Config {
	cfg := &Config{
		Database: DatabaseConfig{
			Host:    "localhost",
			Port:    5432,
			User:    "postgres",
			DBName:  "postgres",
			SSLMode: "prefer",
		},
		Workload: WorkloadConfig{
			Mode:        "burst",
			Duration:    15 * time.Minute,
			Warmup:      2 * time.Minute,
			Cooldown:    1 * time.Minute,
			Connections: 10,
			Workers:     4,
			Seed:        42,
		},
		Output: OutputConfig{
			Format: "json",
		},
	}

	applyEnvOverrides(cfg)
	return cfg
}

// applyEnvOverrides applies environment variable overrides to the config.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("PGHOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv("PGPORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Database.Port = port
		}
	}
	if v := os.Getenv("PGUSER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("PGPASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("PGDATABASE"); v != "" {
		cfg.Database.DBName = v
	}
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.Database.Host == "" {
		return fmt.Errorf("database.host is required")
	}
	if c.Database.Port <= 0 || c.Database.Port > 65535 {
		return fmt.Errorf("database.port must be between 1 and 65535")
	}
	if c.Database.User == "" {
		return fmt.Errorf("database.user is required")
	}
	if c.Database.DBName == "" {
		return fmt.Errorf("database.dbname is required")
	}
	if c.Workload.Mode != "burst" {
		return fmt.Errorf("workload.mode must be 'burst'")
	}
	if c.Workload.Duration < 5*time.Minute {
		return fmt.Errorf("workload.duration must be >= 5m")
	}
	if c.Workload.Connections < 1 {
		return fmt.Errorf("workload.connections must be >= 1")
	}
	if c.Workload.Workers < 1 {
		return fmt.Errorf("workload.workers must be >= 1")
	}
	return nil
}

// ConnectionString returns a PostgreSQL connection string.
func (d *DatabaseConfig) ConnectionString() string {
	connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s",
		d.Host, d.Port, d.User, d.DBName)
	if d.Password != "" {
		connStr += fmt.Sprintf(" password=%s", d.Password)
	}
	if d.SSLMode != "" {
		connStr += fmt.Sprintf(" sslmode=%s", d.SSLMode)
	}
	return connStr
}
