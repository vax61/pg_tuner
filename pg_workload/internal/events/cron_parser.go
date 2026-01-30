package events

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CronExpr represents a parsed cron expression.
type CronExpr struct {
	Minute     []int // 0-59
	Hour       []int // 0-23
	DayOfMonth []int // 1-31, -1 for L (last day)
	Month      []int // 1-12
	DayOfWeek  []int // 0-6 (0=Sunday)
}

// ParseCron parses a cron expression string.
// Supports: numbers, ranges (1-5), lists (1,3,5), steps (*/15), wildcard (*), L (last day)
// Format: minute hour day-of-month month day-of-week
// Examples:
//   - "0 2 * * *"     = every day at 02:00
//   - "*/15 * * * *"  = every 15 minutes
//   - "0 22 * * 1-5"  = 22:00 Mon-Fri
//   - "30 3 1 * *"    = 03:30 first of month
//   - "0 22 L * *"    = 22:00 last day of month
func ParseCron(expr string) (*CronExpr, error) {
	expr = strings.TrimSpace(expr)
	parts := strings.Fields(expr)

	if len(parts) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d", len(parts))
	}

	c := &CronExpr{}
	var err error

	// Minute (0-59)
	c.Minute, err = parseField(parts[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("invalid minute field: %w", err)
	}

	// Hour (0-23)
	c.Hour, err = parseField(parts[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("invalid hour field: %w", err)
	}

	// Day of month (1-31)
	c.DayOfMonth, err = parseDayOfMonth(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-month field: %w", err)
	}

	// Month (1-12)
	c.Month, err = parseField(parts[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("invalid month field: %w", err)
	}

	// Day of week (0-6)
	c.DayOfWeek, err = parseField(parts[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-week field: %w", err)
	}

	return c, nil
}

// parseDayOfMonth handles the day-of-month field which can include L for last day.
func parseDayOfMonth(field string) ([]int, error) {
	if field == "L" {
		return []int{-1}, nil // -1 represents last day
	}

	return parseField(field, 1, 31)
}

// parseField parses a single cron field.
func parseField(field string, min, max int) ([]int, error) {
	if field == "*" {
		return makeRange(min, max, 1), nil
	}

	// Check for step */n
	if strings.HasPrefix(field, "*/") {
		stepStr := field[2:]
		step, err := strconv.Atoi(stepStr)
		if err != nil {
			return nil, fmt.Errorf("invalid step value: %s", stepStr)
		}
		if step <= 0 {
			return nil, fmt.Errorf("step must be positive")
		}
		return makeRange(min, max, step), nil
	}

	// Check for range with step n-m/s
	if strings.Contains(field, "/") {
		parts := strings.Split(field, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid step format")
		}

		step, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid step value: %s", parts[1])
		}
		if step <= 0 {
			return nil, fmt.Errorf("step must be positive")
		}

		// Parse the range part
		rangeVals, err := parseRangeOrList(parts[0], min, max)
		if err != nil {
			return nil, err
		}

		// Apply step to range
		if len(rangeVals) > 0 {
			rangeMin := rangeVals[0]
			rangeMax := rangeVals[len(rangeVals)-1]
			return makeRange(rangeMin, rangeMax, step), nil
		}
		return nil, fmt.Errorf("empty range")
	}

	return parseRangeOrList(field, min, max)
}

// parseRangeOrList parses a field that may contain ranges (1-5) or lists (1,3,5).
func parseRangeOrList(field string, min, max int) ([]int, error) {
	var result []int

	// Split by comma for lists
	parts := strings.Split(field, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for range (n-m)
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range format: %s", part)
			}

			start, err := strconv.Atoi(rangeParts[0])
			if err != nil {
				return nil, fmt.Errorf("invalid range start: %s", rangeParts[0])
			}

			end, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid range end: %s", rangeParts[1])
			}

			if start > end {
				return nil, fmt.Errorf("range start must be <= end: %d-%d", start, end)
			}

			if start < min || end > max {
				return nil, fmt.Errorf("range %d-%d out of bounds [%d,%d]", start, end, min, max)
			}

			for i := start; i <= end; i++ {
				result = append(result, i)
			}
		} else {
			// Single value
			val, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid value: %s", part)
			}

			if val < min || val > max {
				return nil, fmt.Errorf("value %d out of bounds [%d,%d]", val, min, max)
			}

			result = append(result, val)
		}
	}

	// Remove duplicates and sort
	result = uniqueSorted(result)

	return result, nil
}

// makeRange creates a slice of integers from min to max with the given step.
func makeRange(min, max, step int) []int {
	var result []int
	for i := min; i <= max; i += step {
		result = append(result, i)
	}
	return result
}

// uniqueSorted removes duplicates and sorts the slice.
func uniqueSorted(vals []int) []int {
	if len(vals) == 0 {
		return vals
	}

	seen := make(map[int]bool)
	var result []int

	for _, v := range vals {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}

	sort.Ints(result)
	return result
}

// Matches checks if the given time matches this cron expression.
func (c *CronExpr) Matches(t time.Time) bool {
	// Check minute
	if !contains(c.Minute, t.Minute()) {
		return false
	}

	// Check hour
	if !contains(c.Hour, t.Hour()) {
		return false
	}

	// Check month
	if !contains(c.Month, int(t.Month())) {
		return false
	}

	// Check day of month (with L support)
	domMatches := c.matchesDayOfMonth(t)

	// Check day of week
	dowMatches := contains(c.DayOfWeek, int(t.Weekday()))

	// If both day fields are restricted, match if either matches
	// If only one is restricted, only that one needs to match
	domRestricted := !isAllValues(c.DayOfMonth, 1, 31) && !containsLastDay(c.DayOfMonth)
	dowRestricted := !isAllValues(c.DayOfWeek, 0, 6)

	if domRestricted && dowRestricted {
		return domMatches || dowMatches
	}
	if domRestricted {
		return domMatches
	}
	if dowRestricted {
		return dowMatches
	}

	// Both are wildcards or L
	if containsLastDay(c.DayOfMonth) {
		return domMatches
	}

	return true
}

// matchesDayOfMonth checks if the day of month matches.
func (c *CronExpr) matchesDayOfMonth(t time.Time) bool {
	// Handle L (last day of month)
	if containsLastDay(c.DayOfMonth) {
		return isLastDayOfMonth(t)
	}

	return contains(c.DayOfMonth, t.Day())
}

// containsLastDay checks if the slice contains -1 (L marker).
func containsLastDay(vals []int) bool {
	for _, v := range vals {
		if v == -1 {
			return true
		}
	}
	return false
}

// isLastDayOfMonth checks if t is the last day of its month.
func isLastDayOfMonth(t time.Time) bool {
	nextDay := t.AddDate(0, 0, 1)
	return nextDay.Month() != t.Month()
}

// isAllValues checks if the slice contains all values in the range.
func isAllValues(vals []int, min, max int) bool {
	if len(vals) != (max - min + 1) {
		return false
	}
	for i := min; i <= max; i++ {
		if !contains(vals, i) {
			return false
		}
	}
	return true
}

// contains checks if a slice contains a value.
func contains(vals []int, v int) bool {
	for _, val := range vals {
		if val == v {
			return true
		}
	}
	return false
}

// Next returns the next time after 'from' that matches this cron expression.
func (c *CronExpr) Next(from time.Time) time.Time {
	// Start from the next minute
	t := from.Truncate(time.Minute).Add(time.Minute)

	// Search for up to 4 years (handles leap years and all edge cases)
	maxIterations := 4 * 366 * 24 * 60 // 4 years in minutes

	for i := 0; i < maxIterations; i++ {
		if c.Matches(t) {
			return t
		}

		// Optimize: if month doesn't match, skip to next matching month
		if !contains(c.Month, int(t.Month())) {
			t = c.nextMatchingMonth(t)
			continue
		}

		// Optimize: if day doesn't match, skip to next day
		if !c.matchesDay(t) {
			t = t.AddDate(0, 0, 1)
			t = time.Date(t.Year(), t.Month(), t.Day(), c.Hour[0], c.Minute[0], 0, 0, t.Location())
			continue
		}

		// Optimize: if hour doesn't match, skip to next matching hour
		if !contains(c.Hour, t.Hour()) {
			t = c.nextMatchingHour(t)
			continue
		}

		// Just increment by minute
		t = t.Add(time.Minute)
	}

	// Return a far future time if no match found
	return time.Date(9999, 12, 31, 23, 59, 0, 0, time.UTC)
}

// matchesDay checks if day of month and day of week match.
func (c *CronExpr) matchesDay(t time.Time) bool {
	domMatches := c.matchesDayOfMonth(t)
	dowMatches := contains(c.DayOfWeek, int(t.Weekday()))

	domRestricted := !isAllValues(c.DayOfMonth, 1, 31) && !containsLastDay(c.DayOfMonth)
	dowRestricted := !isAllValues(c.DayOfWeek, 0, 6)

	if domRestricted && dowRestricted {
		return domMatches || dowMatches
	}
	if domRestricted {
		return domMatches
	}
	if dowRestricted {
		return dowMatches
	}
	if containsLastDay(c.DayOfMonth) {
		return domMatches
	}
	return true
}

// nextMatchingMonth skips to the first day of the next matching month.
func (c *CronExpr) nextMatchingMonth(t time.Time) time.Time {
	for {
		t = time.Date(t.Year(), t.Month()+1, 1, c.Hour[0], c.Minute[0], 0, 0, t.Location())
		if contains(c.Month, int(t.Month())) {
			return t
		}
		// Handle year wrap
		if t.Year() > 9999 {
			return time.Date(9999, 12, 31, 23, 59, 0, 0, time.UTC)
		}
	}
}

// nextMatchingHour skips to the next matching hour.
func (c *CronExpr) nextMatchingHour(t time.Time) time.Time {
	currentHour := t.Hour()

	// Find next hour today
	for _, h := range c.Hour {
		if h > currentHour {
			return time.Date(t.Year(), t.Month(), t.Day(), h, c.Minute[0], 0, 0, t.Location())
		}
	}

	// No matching hour today, go to next day
	nextDay := t.AddDate(0, 0, 1)
	return time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), c.Hour[0], c.Minute[0], 0, 0, t.Location())
}

// String returns the cron expression as a string.
func (c *CronExpr) String() string {
	return fmt.Sprintf("%s %s %s %s %s",
		fieldToString(c.Minute, 0, 59),
		fieldToString(c.Hour, 0, 23),
		dayOfMonthToString(c.DayOfMonth),
		fieldToString(c.Month, 1, 12),
		fieldToString(c.DayOfWeek, 0, 6))
}

// fieldToString converts a field back to cron string format.
func fieldToString(vals []int, min, max int) string {
	if len(vals) == 0 {
		return "*"
	}

	if isAllValues(vals, min, max) {
		return "*"
	}

	// Check for contiguous range
	if len(vals) > 2 && isContiguous(vals) {
		return fmt.Sprintf("%d-%d", vals[0], vals[len(vals)-1])
	}

	// Convert to list
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = strconv.Itoa(v)
	}
	return strings.Join(strs, ",")
}

// dayOfMonthToString handles L support.
func dayOfMonthToString(vals []int) string {
	if containsLastDay(vals) {
		return "L"
	}
	return fieldToString(vals, 1, 31)
}

// isContiguous checks if values form a contiguous range.
func isContiguous(vals []int) bool {
	for i := 1; i < len(vals); i++ {
		if vals[i] != vals[i-1]+1 {
			return false
		}
	}
	return true
}
