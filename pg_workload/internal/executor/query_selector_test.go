package executor

import (
	"testing"

	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

func TestNewQuerySelector(t *testing.T) {
	selector := NewQuerySelector(profile.OLTPQueries, 42)

	// Updated for new schema with JOIN queries:
	// 12 queries total (point_select, range_select, insert_tx, update_balance,
	// customer_accounts, account_transactions, branch_accounts,
	// customer_tx_summary, branch_tx_summary, customer_audit_trail,
	// full_customer_report, complex_join)
	if selector.QueryCount() != 12 {
		t.Errorf("expected 12 queries, got %d", selector.QueryCount())
	}
	if selector.TotalWeight() != 100 {
		t.Errorf("expected total weight 100, got %d", selector.TotalWeight())
	}
}

func TestQuerySelectorEmpty(t *testing.T) {
	selector := NewQuerySelector(nil, 42)

	if selector.QueryCount() != 0 {
		t.Errorf("expected 0 queries, got %d", selector.QueryCount())
	}

	q := selector.Next()
	if q != nil {
		t.Error("expected nil for empty selector")
	}
}

func TestQuerySelectorDeterministic(t *testing.T) {
	// Two selectors with same seed should produce same sequence
	s1 := NewQuerySelector(profile.OLTPQueries, 12345)
	s2 := NewQuerySelector(profile.OLTPQueries, 12345)

	for i := 0; i < 100; i++ {
		q1 := s1.Next()
		q2 := s2.Next()
		if q1.Name != q2.Name {
			t.Errorf("iteration %d: got %s and %s", i, q1.Name, q2.Name)
		}
	}
}

func TestQuerySelectorDistribution(t *testing.T) {
	selector := NewQuerySelector(profile.OLTPQueries, 42)

	counts := make(map[string]int)
	iterations := 100000

	for i := 0; i < iterations; i++ {
		q := selector.Next()
		counts[q.Name]++
	}

	// Check distribution matches weights (within 2% tolerance)
	tolerance := 0.02
	for _, q := range profile.OLTPQueries {
		expected := float64(q.Weight) / 100.0
		actual := float64(counts[q.Name]) / float64(iterations)
		diff := actual - expected
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("query %s: expected %.2f%%, got %.2f%% (diff %.2f%%)",
				q.Name, expected*100, actual*100, diff*100)
		}
	}
}

func TestQuerySelectorReset(t *testing.T) {
	selector := NewQuerySelector(profile.OLTPQueries, 42)

	// Get some queries
	first := make([]string, 10)
	for i := 0; i < 10; i++ {
		first[i] = selector.Next().Name
	}

	// Reset with same seed
	selector.Reset(42)

	// Should get same sequence
	for i := 0; i < 10; i++ {
		q := selector.Next()
		if q.Name != first[i] {
			t.Errorf("after reset, iteration %d: expected %s, got %s", i, first[i], q.Name)
		}
	}
}

func TestQuerySelectorAllQueriesReachable(t *testing.T) {
	selector := NewQuerySelector(profile.OLTPQueries, 42)

	seen := make(map[string]bool)
	maxIterations := 10000

	for i := 0; i < maxIterations && len(seen) < len(profile.OLTPQueries); i++ {
		q := selector.Next()
		seen[q.Name] = true
	}

	if len(seen) != len(profile.OLTPQueries) {
		t.Errorf("not all queries reached: got %d, expected %d", len(seen), len(profile.OLTPQueries))
		for _, q := range profile.OLTPQueries {
			if !seen[q.Name] {
				t.Errorf("query %s never selected", q.Name)
			}
		}
	}
}

func BenchmarkQuerySelectorNext(b *testing.B) {
	selector := NewQuerySelector(profile.OLTPQueries, 42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = selector.Next()
	}
}

func TestBinarySearchCorrectness(t *testing.T) {
	// Create queries with known weights
	queries := []profile.QueryTemplate{
		{Name: "a", Weight: 10},
		{Name: "b", Weight: 20},
		{Name: "c", Weight: 30},
		{Name: "d", Weight: 40},
	}

	selector := NewQuerySelector(queries, 42)

	// Verify cumulative weights: 10, 30, 60, 100
	// value 0-9 -> a, 10-29 -> b, 30-59 -> c, 60-99 -> d

	testCases := []struct {
		value    int
		expected string
	}{
		{0, "a"},
		{9, "a"},
		{10, "b"},
		{29, "b"},
		{30, "c"},
		{59, "c"},
		{60, "d"},
		{99, "d"},
	}

	for _, tc := range testCases {
		idx := selector.findQuery(tc.value)
		if queries[idx].Name != tc.expected {
			t.Errorf("findQuery(%d) = %s, expected %s", tc.value, queries[idx].Name, tc.expected)
		}
	}
}
