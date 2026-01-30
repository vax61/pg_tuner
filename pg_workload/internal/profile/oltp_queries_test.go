package profile

import (
	"testing"
)

func TestOLTPQueriesCount(t *testing.T) {
	// Updated for new schema with JOIN queries:
	// 12 queries total (point_select, range_select, insert_tx, update_balance,
	// customer_accounts, account_transactions, branch_accounts,
	// customer_tx_summary, branch_tx_summary, customer_audit_trail,
	// full_customer_report, complex_join)
	if len(OLTPQueries) != 12 {
		t.Errorf("Expected 12 OLTP queries, got %d", len(OLTPQueries))
	}
}

func TestOLTPQueriesWeight(t *testing.T) {
	totalWeight := GetTotalWeight()
	if totalWeight != 100 {
		t.Errorf("Expected total weight 100, got %d", totalWeight)
	}
}

func TestGetQueryByName(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		// Original queries
		{"point_select", true},
		{"range_select", true},
		{"insert_tx", true},
		{"update_balance", true},
		{"complex_join", true},
		// 2-way JOIN queries
		{"customer_accounts", true},
		{"account_transactions", true},
		{"branch_accounts", true},
		// 3-way+ JOIN queries
		{"customer_tx_summary", true},
		{"branch_tx_summary", true},
		{"customer_audit_trail", true},
		{"full_customer_report", true},
		// Non-existent
		{"nonexistent", false},
	}

	for _, tt := range tests {
		q := GetQueryByName(tt.name)
		if tt.expected && q == nil {
			t.Errorf("Expected to find query %s", tt.name)
		}
		if !tt.expected && q != nil {
			t.Errorf("Expected not to find query %s", tt.name)
		}
	}
}

func TestQueryTypes(t *testing.T) {
	readQueries := GetReadQueries()
	writeQueries := GetWriteQueries()

	// 10 read queries, 2 write queries
	if len(readQueries) != 10 {
		t.Errorf("Expected 10 read queries, got %d", len(readQueries))
	}
	if len(writeQueries) != 2 {
		t.Errorf("Expected 2 write queries, got %d", len(writeQueries))
	}

	// Verify read queries
	expectedReads := map[string]bool{
		"point_select":        true,
		"range_select":        true,
		"complex_join":        true,
		"customer_accounts":   true,
		"account_transactions": true,
		"branch_accounts":     true,
		"customer_tx_summary": true,
		"branch_tx_summary":   true,
		"customer_audit_trail": true,
		"full_customer_report": true,
	}
	for _, q := range readQueries {
		if !expectedReads[q.Name] {
			t.Errorf("Unexpected read query: %s", q.Name)
		}
	}

	// Verify write queries
	expectedWrites := map[string]bool{"insert_tx": true, "update_balance": true}
	for _, q := range writeQueries {
		if !expectedWrites[q.Name] {
			t.Errorf("Unexpected write query: %s", q.Name)
		}
	}
}

func TestSelectQueryByWeight(t *testing.T) {
	// Test boundaries with new weights:
	// point_select: 30 (0-29)
	// range_select: 12 (30-41)
	// insert_tx: 18 (42-59)
	// update_balance: 12 (60-71)
	// customer_accounts: 8 (72-79)
	// account_transactions: 8 (80-87)
	// branch_accounts: 3 (88-90)
	// customer_tx_summary: 4 (91-94)
	// branch_tx_summary: 2 (95-96)
	// customer_audit_trail: 1 (97)
	// full_customer_report: 1 (98)
	// complex_join: 1 (99)
	tests := []struct {
		value    int
		expected string
	}{
		{0, "point_select"},
		{29, "point_select"},
		{30, "range_select"},
		{41, "range_select"},
		{42, "insert_tx"},
		{59, "insert_tx"},
		{60, "update_balance"},
		{71, "update_balance"},
		{72, "customer_accounts"},
		{79, "customer_accounts"},
		{80, "account_transactions"},
		{87, "account_transactions"},
		{88, "branch_accounts"},
		{90, "branch_accounts"},
		{91, "customer_tx_summary"},
		{94, "customer_tx_summary"},
		{95, "branch_tx_summary"},
		{96, "branch_tx_summary"},
		{97, "customer_audit_trail"},
		{98, "full_customer_report"},
		{99, "complex_join"},
	}

	for _, tt := range tests {
		q := SelectQueryByWeight(tt.value)
		if q.Name != tt.expected {
			t.Errorf("SelectQueryByWeight(%d) = %s, expected %s", tt.value, q.Name, tt.expected)
		}
	}
}

func TestQueryWeightDistribution(t *testing.T) {
	// Run many selections and verify distribution
	counts := make(map[string]int)
	totalWeight := GetTotalWeight()
	iterations := 10000

	for i := 0; i < iterations; i++ {
		value := i % totalWeight
		q := SelectQueryByWeight(value)
		counts[q.Name]++
	}

	// Each query should be selected proportionally to its weight
	for _, q := range OLTPQueries {
		expected := (q.Weight * iterations) / totalWeight
		actual := counts[q.Name]
		// Allow 1% tolerance
		tolerance := iterations / 100
		if actual < expected-tolerance || actual > expected+tolerance {
			t.Errorf("Query %s: expected ~%d selections, got %d", q.Name, expected, actual)
		}
	}
}

func TestQueryTemplateFields(t *testing.T) {
	for _, q := range OLTPQueries {
		if q.Name == "" {
			t.Error("Query has empty name")
		}
		if q.SQL == "" {
			t.Errorf("Query %s has empty SQL", q.Name)
		}
		if q.Type != QueryTypeRead && q.Type != QueryTypeWrite {
			t.Errorf("Query %s has invalid type: %s", q.Name, q.Type)
		}
		if q.Weight <= 0 {
			t.Errorf("Query %s has invalid weight: %d", q.Name, q.Weight)
		}
	}
}

func TestReadWriteRatio(t *testing.T) {
	readWeight := 0
	writeWeight := 0

	for _, q := range OLTPQueries {
		if q.Type == QueryTypeRead {
			readWeight += q.Weight
		} else {
			writeWeight += q.Weight
		}
	}

	// Updated: 70% read, 30% write
	if readWeight != 70 {
		t.Errorf("Expected read weight 70, got %d", readWeight)
	}
	if writeWeight != 30 {
		t.Errorf("Expected write weight 30, got %d", writeWeight)
	}
}

func TestGetJoinQueries(t *testing.T) {
	joinQueries := GetJoinQueries()

	// Should have 8 JOIN queries
	if len(joinQueries) != 8 {
		t.Errorf("Expected 8 join queries, got %d", len(joinQueries))
	}

	expectedJoins := map[string]bool{
		"customer_accounts":    true,
		"account_transactions": true,
		"branch_accounts":      true,
		"customer_tx_summary":  true,
		"branch_tx_summary":    true,
		"customer_audit_trail": true,
		"full_customer_report": true,
		"complex_join":         true,
	}

	for _, q := range joinQueries {
		if !expectedJoins[q.Name] {
			t.Errorf("Unexpected join query: %s", q.Name)
		}
	}
}
