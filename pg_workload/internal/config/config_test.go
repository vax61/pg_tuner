package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigWithDefaults(t *testing.T) {
	// Clear env vars for this test
	os.Unsetenv("PGHOST")
	os.Unsetenv("PGPORT")
	os.Unsetenv("PGUSER")
	os.Unsetenv("PGPASSWORD")
	os.Unsetenv("PGDATABASE")

	cfg := LoadConfigWithDefaults()

	// Database defaults
	if cfg.Database.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("expected port 5432, got %d", cfg.Database.Port)
	}
	if cfg.Database.User != "postgres" {
		t.Errorf("expected user 'postgres', got %q", cfg.Database.User)
	}
	if cfg.Database.DBName != "postgres" {
		t.Errorf("expected dbname 'postgres', got %q", cfg.Database.DBName)
	}
	if cfg.Database.SSLMode != "prefer" {
		t.Errorf("expected sslmode 'prefer', got %q", cfg.Database.SSLMode)
	}

	// Workload defaults
	if cfg.Workload.Mode != "burst" {
		t.Errorf("expected mode 'burst', got %q", cfg.Workload.Mode)
	}
	if cfg.Workload.Duration != 15*time.Minute {
		t.Errorf("expected duration 15m, got %v", cfg.Workload.Duration)
	}
	if cfg.Workload.Connections != 10 {
		t.Errorf("expected connections 10, got %d", cfg.Workload.Connections)
	}
	if cfg.Workload.Workers != 4 {
		t.Errorf("expected workers 4, got %d", cfg.Workload.Workers)
	}

	// Output defaults
	if cfg.Output.Format != "json" {
		t.Errorf("expected format 'json', got %q", cfg.Output.Format)
	}
}

func TestLoadConfigValidYAML(t *testing.T) {
	yaml := `
database:
  host: testhost
  port: 5433
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: disable

workload:
  mode: burst
  duration: 10m
  warmup: 1m
  cooldown: 30s
  connections: 20
  workers: 8
  seed: 123

output:
  file: results.json
  format: json
`
	tmpFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(tmpFile, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Database.Host != "testhost" {
		t.Errorf("expected host 'testhost', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != 5433 {
		t.Errorf("expected port 5433, got %d", cfg.Database.Port)
	}
	if cfg.Database.Password != "testpass" {
		t.Errorf("expected password 'testpass', got %q", cfg.Database.Password)
	}
	if cfg.Workload.Duration != 10*time.Minute {
		t.Errorf("expected duration 10m, got %v", cfg.Workload.Duration)
	}
	if cfg.Workload.Connections != 20 {
		t.Errorf("expected connections 20, got %d", cfg.Workload.Connections)
	}
	if cfg.Output.File != "results.json" {
		t.Errorf("expected output file 'results.json', got %q", cfg.Output.File)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	os.Setenv("PGHOST", "envhost")
	os.Setenv("PGPORT", "5434")
	os.Setenv("PGUSER", "envuser")
	os.Setenv("PGPASSWORD", "envpass")
	os.Setenv("PGDATABASE", "envdb")
	defer func() {
		os.Unsetenv("PGHOST")
		os.Unsetenv("PGPORT")
		os.Unsetenv("PGUSER")
		os.Unsetenv("PGPASSWORD")
		os.Unsetenv("PGDATABASE")
	}()

	cfg := LoadConfigWithDefaults()

	if cfg.Database.Host != "envhost" {
		t.Errorf("expected host 'envhost', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != 5434 {
		t.Errorf("expected port 5434, got %d", cfg.Database.Port)
	}
	if cfg.Database.User != "envuser" {
		t.Errorf("expected user 'envuser', got %q", cfg.Database.User)
	}
	if cfg.Database.Password != "envpass" {
		t.Errorf("expected password 'envpass', got %q", cfg.Database.Password)
	}
	if cfg.Database.DBName != "envdb" {
		t.Errorf("expected dbname 'envdb', got %q", cfg.Database.DBName)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(tmpFile, []byte("{{invalid yaml"), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestValidateErrors(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name:    "empty host",
			modify:  func(c *Config) { c.Database.Host = "" },
			wantErr: "database.host is required",
		},
		{
			name:    "invalid port",
			modify:  func(c *Config) { c.Database.Port = 0 },
			wantErr: "database.port must be between 1 and 65535",
		},
		{
			name:    "empty user",
			modify:  func(c *Config) { c.Database.User = "" },
			wantErr: "database.user is required",
		},
		{
			name:    "empty dbname",
			modify:  func(c *Config) { c.Database.DBName = "" },
			wantErr: "database.dbname is required",
		},
		{
			name:    "invalid mode",
			modify:  func(c *Config) { c.Workload.Mode = "invalid" },
			wantErr: "workload.mode must be 'burst'",
		},
		{
			name:    "duration too short",
			modify:  func(c *Config) { c.Workload.Duration = 1 * time.Minute },
			wantErr: "workload.duration must be >= 5m",
		},
		{
			name:    "zero connections",
			modify:  func(c *Config) { c.Workload.Connections = 0 },
			wantErr: "workload.connections must be >= 1",
		},
		{
			name:    "zero workers",
			modify:  func(c *Config) { c.Workload.Workers = 0 },
			wantErr: "workload.workers must be >= 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear env vars
			os.Unsetenv("PGHOST")
			os.Unsetenv("PGPORT")
			os.Unsetenv("PGUSER")
			os.Unsetenv("PGPASSWORD")
			os.Unsetenv("PGDATABASE")

			cfg := LoadConfigWithDefaults()
			tt.modify(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Errorf("expected error containing %q", tt.wantErr)
				return
			}
			if err.Error() != tt.wantErr {
				t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestConnectionString(t *testing.T) {
	db := DatabaseConfig{
		Host:     "myhost",
		Port:     5432,
		User:     "myuser",
		Password: "mypass",
		DBName:   "mydb",
		SSLMode:  "require",
	}

	connStr := db.ConnectionString()
	expected := "host=myhost port=5432 user=myuser dbname=mydb password=mypass sslmode=require"
	if connStr != expected {
		t.Errorf("expected %q, got %q", expected, connStr)
	}
}

func TestConnectionStringNoPassword(t *testing.T) {
	db := DatabaseConfig{
		Host:    "myhost",
		Port:    5432,
		User:    "myuser",
		DBName:  "mydb",
		SSLMode: "disable",
	}

	connStr := db.ConnectionString()
	expected := "host=myhost port=5432 user=myuser dbname=mydb sslmode=disable"
	if connStr != expected {
		t.Errorf("expected %q, got %q", expected, connStr)
	}
}
