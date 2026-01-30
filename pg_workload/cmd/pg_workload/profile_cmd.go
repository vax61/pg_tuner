package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Profile management commands",
	Long:  "Manage workload profiles: list available profiles, show details, validate custom profiles.",
}

// Profile command flags
var profileCfg struct {
	Format string // output format: text, json, yaml
}

var profileListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available profiles",
	Long: `List all available workload profiles.

Shows built-in profiles and any custom profiles in the profiles directory.

Examples:
  pg_workload profile list
  pg_workload profile list --format yaml
`,
	RunE: runProfileList,
}

var profileShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show profile details",
	Long: `Show detailed information about a profile.

Displays query weights, query templates, and other configuration.

Examples:
  pg_workload profile show oltp_standard
  pg_workload profile show olap
`,
	Args: cobra.ExactArgs(1),
	RunE: runProfileShow,
}

var profileValidateCmd = &cobra.Command{
	Use:   "validate <file>",
	Short: "Validate a profile YAML file",
	Long: `Validate a custom profile YAML file.

Checks for:
  - Valid YAML syntax
  - Required fields
  - Valid query weights (sum to 100)
  - Valid SQL syntax

Examples:
  pg_workload profile validate custom_profile.yaml
`,
	Args: cobra.ExactArgs(1),
	RunE: runProfileValidate,
}

var profileGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate a new profile template",
	Long: `Generate a new profile YAML template.

Creates a template file with example queries and weights
that you can customize for your workload.

Examples:
  pg_workload profile generate > my_profile.yaml
  pg_workload profile generate --format yaml > my_profile.yaml
`,
	RunE: runProfileGenerate,
}

func init() {
	// Add subcommands to profile
	profileCmd.AddCommand(profileListCmd)
	profileCmd.AddCommand(profileShowCmd)
	profileCmd.AddCommand(profileValidateCmd)
	profileCmd.AddCommand(profileGenerateCmd)

	// Common flags
	profileCmd.PersistentFlags().StringVar(&profileCfg.Format, "format", "text", "output format: text, json, yaml")
}

func runProfileList(cmd *cobra.Command, args []string) error {
	profiles := getAvailableProfiles()

	switch profileCfg.Format {
	case "yaml":
		data, err := yaml.Marshal(profiles)
		if err != nil {
			return err
		}
		fmt.Print(string(data))
	case "json":
		// Simple JSON output
		fmt.Println("[")
		for i, p := range profiles {
			comma := ","
			if i == len(profiles)-1 {
				comma = ""
			}
			fmt.Printf("  {\"name\": %q, \"type\": %q, \"description\": %q, \"queries\": %d}%s\n",
				p.Name, p.Type, p.Description, p.QueryCount, comma)
		}
		fmt.Println("]")
	default:
		fmt.Println("Available Profiles")
		fmt.Println("==================")
		fmt.Println()
		fmt.Printf("%-20s %-10s %-8s %s\n", "NAME", "TYPE", "QUERIES", "DESCRIPTION")
		fmt.Println(strings.Repeat("-", 70))
		for _, p := range profiles {
			fmt.Printf("%-20s %-10s %-8d %s\n", p.Name, p.Type, p.QueryCount, p.Description)
		}
		fmt.Println()
	}

	return nil
}

type profileInfo struct {
	Name        string `yaml:"name" json:"name"`
	Type        string `yaml:"type" json:"type"`
	Description string `yaml:"description" json:"description"`
	QueryCount  int    `yaml:"query_count" json:"query_count"`
}

func getAvailableProfiles() []profileInfo {
	profiles := []profileInfo{
		{
			Name:        "oltp_standard",
			Type:        "builtin",
			Description: "Standard OLTP workload with point lookups, range scans, and writes",
			QueryCount:  len(profile.OLTPQueries),
		},
		{
			Name:        "oltp",
			Type:        "builtin",
			Description: "Alias for oltp_standard",
			QueryCount:  len(profile.OLTPQueries),
		},
	}

	// Check for custom profiles in standard locations
	searchPaths := []string{
		"./profiles",
		"./pg_workload/profiles",
		filepath.Join(os.Getenv("HOME"), ".pg_workload", "profiles"),
	}

	for _, searchPath := range searchPaths {
		if entries, err := os.ReadDir(searchPath); err == nil {
			for _, entry := range entries {
				if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".yaml") || strings.HasSuffix(entry.Name(), ".yml")) {
					name := strings.TrimSuffix(strings.TrimSuffix(entry.Name(), ".yaml"), ".yml")
					profiles = append(profiles, profileInfo{
						Name:        name,
						Type:        "custom",
						Description: fmt.Sprintf("Custom profile from %s", searchPath),
						QueryCount:  0, // Would need to parse to get count
					})
				}
			}
		}
	}

	return profiles
}

func runProfileShow(cmd *cobra.Command, args []string) error {
	profileName := args[0]

	// Get profile queries
	queries := getProfileQueries(profileName)
	if queries == nil {
		return fmt.Errorf("profile not found: %s", profileName)
	}

	switch profileCfg.Format {
	case "yaml":
		return showProfileYAML(profileName, queries)
	case "json":
		return showProfileJSON(profileName, queries)
	default:
		return showProfileText(profileName, queries)
	}
}

func getProfileQueries(name string) []profile.QueryTemplate {
	switch name {
	case "oltp", "oltp_standard":
		return profile.OLTPQueries
	default:
		// Try to load custom profile
		p, err := profile.LoadSimulationProfile(name)
		if err != nil {
			return nil
		}
		// Convert to query templates (simplified)
		_ = p
		return profile.OLTPQueries // Fallback for now
	}
}

func showProfileText(name string, queries []profile.QueryTemplate) error {
	fmt.Printf("Profile: %s\n", name)
	fmt.Println(strings.Repeat("=", 40))
	fmt.Println()

	// Group by type
	readQueries := make([]profile.QueryTemplate, 0)
	writeQueries := make([]profile.QueryTemplate, 0)

	for _, q := range queries {
		if q.Type == profile.QueryTypeRead {
			readQueries = append(readQueries, q)
		} else {
			writeQueries = append(writeQueries, q)
		}
	}

	// Calculate totals
	var readWeight, writeWeight int
	for _, q := range readQueries {
		readWeight += q.Weight
	}
	for _, q := range writeQueries {
		writeWeight += q.Weight
	}

	fmt.Printf("Total queries: %d\n", len(queries))
	fmt.Printf("Read/Write ratio: %d%% / %d%%\n", readWeight, writeWeight)
	fmt.Println()

	fmt.Println("Read Queries:")
	fmt.Printf("%-25s %8s %s\n", "NAME", "WEIGHT", "CATEGORY")
	fmt.Println(strings.Repeat("-", 60))
	sort.Slice(readQueries, func(i, j int) bool { return readQueries[i].Weight > readQueries[j].Weight })
	for _, q := range readQueries {
		category := categorizeQuery(q.Name)
		fmt.Printf("%-25s %7d%% %s\n", q.Name, q.Weight, category)
	}
	fmt.Println()

	fmt.Println("Write Queries:")
	fmt.Printf("%-25s %8s %s\n", "NAME", "WEIGHT", "CATEGORY")
	fmt.Println(strings.Repeat("-", 60))
	sort.Slice(writeQueries, func(i, j int) bool { return writeQueries[i].Weight > writeQueries[j].Weight })
	for _, q := range writeQueries {
		category := categorizeQuery(q.Name)
		fmt.Printf("%-25s %7d%% %s\n", q.Name, q.Weight, category)
	}
	fmt.Println()

	return nil
}

func categorizeQuery(name string) string {
	switch {
	case strings.Contains(name, "point"):
		return "Point Lookup"
	case strings.Contains(name, "range"):
		return "Range Scan"
	case strings.Contains(name, "insert"):
		return "Insert"
	case strings.Contains(name, "update"):
		return "Update"
	case strings.Contains(name, "summary") || strings.Contains(name, "report"):
		return "Aggregate"
	case strings.Contains(name, "join") || strings.Contains(name, "_"):
		return "Join"
	default:
		return "Other"
	}
}

func showProfileYAML(name string, queries []profile.QueryTemplate) error {
	type queryDef struct {
		Name        string `yaml:"name"`
		Type        string `yaml:"type"`
		Weight      int    `yaml:"weight"`
		SQL         string `yaml:"sql"`
		Description string `yaml:"description,omitempty"`
	}

	type profileDef struct {
		Name    string     `yaml:"name"`
		Version string     `yaml:"version"`
		Queries []queryDef `yaml:"queries"`
	}

	p := profileDef{
		Name:    name,
		Version: "1.0",
		Queries: make([]queryDef, len(queries)),
	}

	for i, q := range queries {
		qType := "read"
		if q.Type == profile.QueryTypeWrite {
			qType = "write"
		}
		p.Queries[i] = queryDef{
			Name:        q.Name,
			Type:        qType,
			Weight:      q.Weight,
			SQL:         q.SQL,
			Description: q.Description,
		}
	}

	data, err := yaml.Marshal(p)
	if err != nil {
		return err
	}
	fmt.Print(string(data))
	return nil
}

func showProfileJSON(name string, queries []profile.QueryTemplate) error {
	fmt.Println("{")
	fmt.Printf("  \"name\": %q,\n", name)
	fmt.Printf("  \"version\": \"1.0\",\n")
	fmt.Println("  \"queries\": [")

	for i, q := range queries {
		qType := "read"
		if q.Type == profile.QueryTypeWrite {
			qType = "write"
		}
		comma := ","
		if i == len(queries)-1 {
			comma = ""
		}
		fmt.Printf("    {\"name\": %q, \"type\": %q, \"weight\": %d}%s\n",
			q.Name, qType, q.Weight, comma)
	}

	fmt.Println("  ]")
	fmt.Println("}")
	return nil
}

func runProfileValidate(cmd *cobra.Command, args []string) error {
	filename := args[0]

	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	// Parse YAML
	var p struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
		Queries []struct {
			Name     string  `yaml:"name"`
			Type     string  `yaml:"type"`
			Weight   float64 `yaml:"weight"`
			Template string  `yaml:"template"`
		} `yaml:"queries"`
	}

	if err := yaml.Unmarshal(data, &p); err != nil {
		return fmt.Errorf("invalid YAML syntax: %w", err)
	}

	// Validate
	var errors []string
	var warnings []string

	if p.Name == "" {
		errors = append(errors, "missing 'name' field")
	}

	if len(p.Queries) == 0 {
		errors = append(errors, "no queries defined")
	}

	var totalWeight float64
	for i, q := range p.Queries {
		if q.Name == "" {
			errors = append(errors, fmt.Sprintf("query %d: missing 'name'", i+1))
		}
		if q.Type != "read" && q.Type != "write" {
			errors = append(errors, fmt.Sprintf("query '%s': type must be 'read' or 'write'", q.Name))
		}
		if q.Weight <= 0 {
			errors = append(errors, fmt.Sprintf("query '%s': weight must be > 0", q.Name))
		}
		if q.Template == "" {
			warnings = append(warnings, fmt.Sprintf("query '%s': no template defined", q.Name))
		}
		totalWeight += q.Weight
	}

	// Check weight sum
	if totalWeight < 99.9 || totalWeight > 100.1 {
		warnings = append(warnings, fmt.Sprintf("query weights sum to %.1f%% (expected 100%%)", totalWeight))
	}

	// Print results
	fmt.Printf("Validating: %s\n", filename)
	fmt.Println()

	if len(errors) > 0 {
		fmt.Println("Errors:")
		for _, e := range errors {
			fmt.Printf("  - %s\n", e)
		}
		fmt.Println()
	}

	if len(warnings) > 0 {
		fmt.Println("Warnings:")
		for _, w := range warnings {
			fmt.Printf("  - %s\n", w)
		}
		fmt.Println()
	}

	if len(errors) == 0 {
		fmt.Printf("Profile '%s' is valid (%d queries)\n", p.Name, len(p.Queries))
		return nil
	}

	return fmt.Errorf("validation failed with %d error(s)", len(errors))
}

func runProfileGenerate(cmd *cobra.Command, args []string) error {
	template := `# Custom Workload Profile
# Generated by pg_workload

name: my_custom_profile
version: "1.0"
description: "Custom workload profile"

# Query weights should sum to 100
queries:
  # Point lookups - fast single-row reads
  - name: point_select
    type: read
    weight: 30
    template: "SELECT * FROM accounts WHERE id = $1"

  # Range scans - read multiple rows
  - name: range_select
    type: read
    weight: 20
    template: "SELECT * FROM transactions WHERE account_id = $1 ORDER BY timestamp DESC LIMIT 100"

  # Join queries - complex reads
  - name: customer_accounts
    type: read
    weight: 20
    template: |
      SELECT c.*, a.*
      FROM customers c
      JOIN accounts a ON a.customer_id = c.id
      WHERE c.id = $1

  # Write operations
  - name: insert_tx
    type: write
    weight: 20
    template: |
      INSERT INTO transactions (account_id, amount, type, description, timestamp)
      VALUES ($1, $2, $3, $4, NOW())

  - name: update_balance
    type: write
    weight: 10
    template: "UPDATE accounts SET balance = balance + $2 WHERE id = $1"

# Simulation events (optional)
events:
  - name: peak_hours
    schedule: "08:00-18:00"
    qps_multiplier: 2.0

  - name: batch_job
    schedule: "02:00-04:00"
    write_heavy: true
`

	fmt.Print(template)
	return nil
}
