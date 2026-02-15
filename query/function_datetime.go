package query

import (
	"fmt"
	"strings"
	"time"
)

// Date/Time Functions

// Helper to parse date strings
func parseDate(v interface{}) (time.Time, error) {
	str, err := valueToString(v)
	if err != nil {
		return time.Time{}, err
	}

	layouts := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, str); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse date: %s", str)
}

// NowFunc returns the current timestamp
type NowFunc struct{}

func (f *NowFunc) Name() string  { return "NOW" }
func (f *NowFunc) MinArity() int { return 0 }
func (f *NowFunc) MaxArity() int { return 0 }
func (f *NowFunc) Evaluate(args []interface{}) (interface{}, error) {
	return time.Now().Format(time.RFC3339), nil
}

// CurrentDateFunc returns the current date
type CurrentDateFunc struct{}

func (f *CurrentDateFunc) Name() string  { return "CURRENT_DATE" }
func (f *CurrentDateFunc) MinArity() int { return 0 }
func (f *CurrentDateFunc) MaxArity() int { return 0 }
func (f *CurrentDateFunc) Evaluate(args []interface{}) (interface{}, error) {
	return time.Now().Format("2006-01-02"), nil
}

// CurrentTimeFunc returns the current time
type CurrentTimeFunc struct{}

func (f *CurrentTimeFunc) Name() string  { return "CURRENT_TIME" }
func (f *CurrentTimeFunc) MinArity() int { return 0 }
func (f *CurrentTimeFunc) MaxArity() int { return 0 }
func (f *CurrentTimeFunc) Evaluate(args []interface{}) (interface{}, error) {
	return time.Now().Format("15:04:05"), nil
}

// DateTruncFunc truncates a date to the specified unit
type DateTruncFunc struct{}

func (f *DateTruncFunc) Name() string  { return "DATE_TRUNC" }
func (f *DateTruncFunc) MinArity() int { return 2 }
func (f *DateTruncFunc) MaxArity() int { return 2 }
func (f *DateTruncFunc) Evaluate(args []interface{}) (interface{}, error) {
	unit, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_TRUNC: unit: %w", err)
	}

	date, err := parseDate(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_TRUNC: %w", err)
	}

	switch strings.ToLower(unit) {
	case "year":
		return time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.Location()).Format(time.RFC3339), nil
	case "month":
		return time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.Location()).Format(time.RFC3339), nil
	case "day":
		return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location()).Format(time.RFC3339), nil
	case "hour":
		return time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), 0, 0, 0, date.Location()).Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("DATE_TRUNC: invalid unit: %s", unit)
	}
}

// DatePartFunc extracts a part of a date
type DatePartFunc struct{}

func (f *DatePartFunc) Name() string  { return "DATE_PART" }
func (f *DatePartFunc) MinArity() int { return 2 }
func (f *DatePartFunc) MaxArity() int { return 2 }
func (f *DatePartFunc) Evaluate(args []interface{}) (interface{}, error) {
	unit, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_PART: unit: %w", err)
	}

	date, err := parseDate(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_PART: %w", err)
	}

	switch strings.ToLower(unit) {
	case "year":
		return int64(date.Year()), nil
	case "month":
		return int64(date.Month()), nil
	case "day":
		return int64(date.Day()), nil
	case "hour":
		return int64(date.Hour()), nil
	case "minute":
		return int64(date.Minute()), nil
	case "second":
		return int64(date.Second()), nil
	default:
		return nil, fmt.Errorf("DATE_PART: invalid unit: %s", unit)
	}
}

// DateAddFunc adds an interval to a date
type DateAddFunc struct{}

func (f *DateAddFunc) Name() string  { return "DATE_ADD" }
func (f *DateAddFunc) MinArity() int { return 3 }
func (f *DateAddFunc) MaxArity() int { return 3 }
func (f *DateAddFunc) Evaluate(args []interface{}) (interface{}, error) {
	date, err := parseDate(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: %w", err)
	}

	amount, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: amount: %w", err)
	}

	unit, err := valueToString(args[2])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: unit: %w", err)
	}

	switch strings.ToLower(unit) {
	case "year", "month", "day":
		// Check for reasonable bounds to prevent overflow
		if amount > float64(1<<30) || amount < float64(-(1<<30)) {
			return nil, fmt.Errorf("DATE_ADD: amount out of valid range")
		}
		switch strings.ToLower(unit) {
		case "year":
			return date.AddDate(int(amount), 0, 0).Format(time.RFC3339), nil
		case "month":
			return date.AddDate(0, int(amount), 0).Format(time.RFC3339), nil
		case "day":
			return date.AddDate(0, 0, int(amount)).Format(time.RFC3339), nil
		}
	case "hour":
		// Check bounds before multiplication to prevent overflow
		const maxHours = float64(1<<53) / float64(time.Hour)
		if amount > maxHours || amount < -maxHours {
			return nil, fmt.Errorf("DATE_ADD: amount out of valid range")
		}
		return date.Add(time.Duration(amount) * time.Hour).Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("DATE_ADD: invalid unit: %s", unit)
	}
	// This line is unreachable but kept to satisfy linter
	return nil, fmt.Errorf("DATE_ADD: unexpected error")
}

// DateSubFunc subtracts an interval from a date
type DateSubFunc struct{}

func (f *DateSubFunc) Name() string  { return "DATE_SUB" }
func (f *DateSubFunc) MinArity() int { return 3 }
func (f *DateSubFunc) MaxArity() int { return 3 }
func (f *DateSubFunc) Evaluate(args []interface{}) (interface{}, error) {
	date, err := parseDate(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_SUB: %w", err)
	}

	amount, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_SUB: amount: %w", err)
	}

	unit, err := valueToString(args[2])
	if err != nil {
		return nil, fmt.Errorf("DATE_SUB: unit: %w", err)
	}

	switch strings.ToLower(unit) {
	case "year", "month", "day":
		// Check for reasonable bounds to prevent overflow
		if amount > float64(1<<30) || amount < float64(-(1<<30)) {
			return nil, fmt.Errorf("DATE_SUB: amount out of valid range")
		}
		switch strings.ToLower(unit) {
		case "year":
			return date.AddDate(-int(amount), 0, 0).Format(time.RFC3339), nil
		case "month":
			return date.AddDate(0, -int(amount), 0).Format(time.RFC3339), nil
		case "day":
			return date.AddDate(0, 0, -int(amount)).Format(time.RFC3339), nil
		}
	case "hour":
		// Check bounds before multiplication to prevent overflow
		const maxHours = float64(1<<53) / float64(time.Hour)
		if amount > maxHours || amount < -maxHours {
			return nil, fmt.Errorf("DATE_SUB: amount out of valid range")
		}
		return date.Add(-time.Duration(amount) * time.Hour).Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("DATE_SUB: invalid unit: %s", unit)
	}
	return nil, fmt.Errorf("DATE_SUB: unexpected error")
}

// DateDiffFunc returns the difference between two dates in days
type DateDiffFunc struct{}

func (f *DateDiffFunc) Name() string  { return "DATE_DIFF" }
func (f *DateDiffFunc) MinArity() int { return 2 }
func (f *DateDiffFunc) MaxArity() int { return 2 }
func (f *DateDiffFunc) Evaluate(args []interface{}) (interface{}, error) {
	date1, err := parseDate(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_DIFF: first date: %w", err)
	}

	date2, err := parseDate(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_DIFF: second date: %w", err)
	}

	diff := date1.Sub(date2)
	// Use truncation (not rounding) to maintain consistent behavior
	// where partial days are counted as 0
	return int64(diff.Hours() / 24), nil
}

// YearFunc extracts the year from a date
type YearFunc struct{}

func (f *YearFunc) Name() string  { return "YEAR" }
func (f *YearFunc) MinArity() int { return 1 }
func (f *YearFunc) MaxArity() int { return 1 }
func (f *YearFunc) Evaluate(args []interface{}) (interface{}, error) {
	date, err := parseDate(args[0])
	if err != nil {
		return nil, fmt.Errorf("YEAR: %w", err)
	}
	return int64(date.Year()), nil
}

// MonthFunc extracts the month from a date
type MonthFunc struct{}

func (f *MonthFunc) Name() string  { return "MONTH" }
func (f *MonthFunc) MinArity() int { return 1 }
func (f *MonthFunc) MaxArity() int { return 1 }
func (f *MonthFunc) Evaluate(args []interface{}) (interface{}, error) {
	date, err := parseDate(args[0])
	if err != nil {
		return nil, fmt.Errorf("MONTH: %w", err)
	}
	return int64(date.Month()), nil
}
