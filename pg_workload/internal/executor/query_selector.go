package executor

import (
	"math/rand"

	"github.com/myorg/pg_tuner/pg_workload/internal/profile"
)

// QuerySelector provides weighted random query selection.
type QuerySelector struct {
	queries     []profile.QueryTemplate
	weights     []int
	totalWeight int
	rng         *rand.Rand
}

// NewQuerySelector creates a new QuerySelector with the given queries and seed.
func NewQuerySelector(queries []profile.QueryTemplate, seed int64) *QuerySelector {
	if len(queries) == 0 {
		return &QuerySelector{}
	}

	s := &QuerySelector{
		queries: queries,
		weights: make([]int, len(queries)),
		rng:     rand.New(rand.NewSource(seed)),
	}

	// Build cumulative weights for efficient selection
	cumulative := 0
	for i, q := range queries {
		cumulative += q.Weight
		s.weights[i] = cumulative
	}
	s.totalWeight = cumulative

	return s
}

// Next returns the next query based on weighted random selection.
func (s *QuerySelector) Next() *profile.QueryTemplate {
	if len(s.queries) == 0 || s.totalWeight == 0 {
		return nil
	}

	// Generate random value in [0, totalWeight)
	value := s.rng.Intn(s.totalWeight)

	// Binary search for the query
	idx := s.findQuery(value)
	return &s.queries[idx]
}

// findQuery performs binary search to find the query index for a given value.
func (s *QuerySelector) findQuery(value int) int {
	low, high := 0, len(s.weights)-1

	for low < high {
		mid := (low + high) / 2
		if s.weights[mid] <= value {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return low
}

// TotalWeight returns the sum of all query weights.
func (s *QuerySelector) TotalWeight() int {
	return s.totalWeight
}

// QueryCount returns the number of queries.
func (s *QuerySelector) QueryCount() int {
	return len(s.queries)
}

// Reset reseeds the random number generator.
func (s *QuerySelector) Reset(seed int64) {
	s.rng = rand.New(rand.NewSource(seed))
}
