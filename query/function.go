package query

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Function represents a scalar function that can be evaluated
type Function interface {
	// Name returns the function name (case-insensitive)
	Name() string
	// MinArity returns the minimum number of arguments (-1 for variadic with no minimum)
	MinArity() int
	// MaxArity returns the maximum number of arguments (-1 for unlimited)
	MaxArity() int
	// Evaluate evaluates the function with the given arguments
	Evaluate(args []interface{}) (interface{}, error)
}

// FunctionRegistry manages function lookup and registration
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]Function
}

// NewFunctionRegistry creates a new function registry
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]Function),
	}
}

// Register registers a function
func (r *FunctionRegistry) Register(f Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.functions[strings.ToUpper(f.Name())] = f
}

// Get retrieves a function by name (case-insensitive)
func (r *FunctionRegistry) Get(name string) (Function, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, exists := r.functions[strings.ToUpper(name)]
	return f, exists
}

// globalRegistry is the default function registry
var globalRegistry *FunctionRegistry

func init() {
	globalRegistry = NewFunctionRegistry()

	// Register string functions
	globalRegistry.Register(&UpperFunc{})
	globalRegistry.Register(&LowerFunc{})
	globalRegistry.Register(&ConcatFunc{})
	globalRegistry.Register(&LengthFunc{})
	globalRegistry.Register(&TrimFunc{})
	globalRegistry.Register(&LTrimFunc{})
	globalRegistry.Register(&RTrimFunc{})
	globalRegistry.Register(&SubstringFunc{})
	globalRegistry.Register(&ReplaceFunc{})
	globalRegistry.Register(&SplitFunc{})
	globalRegistry.Register(&ReverseFunc{})
	globalRegistry.Register(&ContainsFunc{})
	globalRegistry.Register(&StartsWithFunc{})
	globalRegistry.Register(&EndsWithFunc{})
	globalRegistry.Register(&RepeatFunc{})

	// Register math functions
	globalRegistry.Register(&AbsFunc{})
	globalRegistry.Register(&RoundFunc{})
	globalRegistry.Register(&FloorFunc{})
	globalRegistry.Register(&CeilFunc{})
	globalRegistry.Register(&ModFunc{})
	globalRegistry.Register(&SqrtFunc{})
	globalRegistry.Register(&PowFunc{})
	globalRegistry.Register(&SignFunc{})
	globalRegistry.Register(&TruncFunc{})
	globalRegistry.Register(&RandomFunc{})
	globalRegistry.Register(&MinFunc{})
	globalRegistry.Register(&MaxFunc{})

	// Register date/time functions
	globalRegistry.Register(&NowFunc{})
	globalRegistry.Register(&CurrentDateFunc{})
	globalRegistry.Register(&CurrentTimeFunc{})
	globalRegistry.Register(&DateTruncFunc{})
	globalRegistry.Register(&DatePartFunc{})
	globalRegistry.Register(&DateAddFunc{})
	globalRegistry.Register(&DateSubFunc{})
	globalRegistry.Register(&DateDiffFunc{})
	globalRegistry.Register(&YearFunc{})
	globalRegistry.Register(&MonthFunc{})

	// Register type conversion functions
	globalRegistry.Register(&CastFunc{})
	globalRegistry.Register(&TryCastFunc{})
	globalRegistry.Register(&ToStringFunc{})
	globalRegistry.Register(&ToNumberFunc{})
	globalRegistry.Register(&ToDateFunc{})

	// Register conditional functions
	globalRegistry.Register(&CoalesceFunc{})
	globalRegistry.Register(&NullIfFunc{})
}

// GetGlobalRegistry returns the global function registry
func GetGlobalRegistry() *FunctionRegistry {
	return globalRegistry
}

// Helper function to convert value to string
func valueToString(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%v", val), nil
	case bool:
		return fmt.Sprintf("%t", val), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", v)
	}
}

// Helper function to convert value to number
func valueToNumber(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case string:
		// Try to parse string as number
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to number", v)
	}
}

// String Functions

// UpperFunc converts a string to uppercase
type UpperFunc struct{}

func (f *UpperFunc) Name() string  { return "UPPER" }
func (f *UpperFunc) MinArity() int { return 1 }
func (f *UpperFunc) MaxArity() int { return 1 }
func (f *UpperFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("UPPER: %w", err)
	}
	return strings.ToUpper(str), nil
}

// LowerFunc converts a string to lowercase
type LowerFunc struct{}

func (f *LowerFunc) Name() string  { return "LOWER" }
func (f *LowerFunc) MinArity() int { return 1 }
func (f *LowerFunc) MaxArity() int { return 1 }
func (f *LowerFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("LOWER: %w", err)
	}
	return strings.ToLower(str), nil
}

// ConcatFunc concatenates multiple strings
type ConcatFunc struct{}

func (f *ConcatFunc) Name() string  { return "CONCAT" }
func (f *ConcatFunc) MinArity() int { return 1 }
func (f *ConcatFunc) MaxArity() int { return -1 } // variadic
func (f *ConcatFunc) Evaluate(args []interface{}) (interface{}, error) {
	var builder strings.Builder
	for i, arg := range args {
		str, err := valueToString(arg)
		if err != nil {
			return nil, fmt.Errorf("CONCAT: argument %d: %w", i+1, err)
		}
		builder.WriteString(str)
	}
	return builder.String(), nil
}

// LengthFunc returns the length of a string
type LengthFunc struct{}

func (f *LengthFunc) Name() string  { return "LENGTH" }
func (f *LengthFunc) MinArity() int { return 1 }
func (f *LengthFunc) MaxArity() int { return 1 }
func (f *LengthFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("LENGTH: %w", err)
	}
	return int64(len(str)), nil
}

// TrimFunc trims whitespace from both ends of a string
type TrimFunc struct{}

func (f *TrimFunc) Name() string  { return "TRIM" }
func (f *TrimFunc) MinArity() int { return 1 }
func (f *TrimFunc) MaxArity() int { return 1 }
func (f *TrimFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("TRIM: %w", err)
	}
	return strings.TrimSpace(str), nil
}

// Math Functions

// AbsFunc returns the absolute value of a number
type AbsFunc struct{}

func (f *AbsFunc) Name() string  { return "ABS" }
func (f *AbsFunc) MinArity() int { return 1 }
func (f *AbsFunc) MaxArity() int { return 1 }
func (f *AbsFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("ABS: %w", err)
	}
	return math.Abs(num), nil
}

// RoundFunc rounds a number to the specified number of decimal places
type RoundFunc struct{}

func (f *RoundFunc) Name() string  { return "ROUND" }
func (f *RoundFunc) MinArity() int { return 1 }
func (f *RoundFunc) MaxArity() int { return 2 }
func (f *RoundFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND: %w", err)
	}

	// Default to 0 decimal places
	decimals := 0.0
	if len(args) == 2 {
		decimals, err = valueToNumber(args[1])
		if err != nil {
			return nil, fmt.Errorf("ROUND: decimals argument: %w", err)
		}
	}

	multiplier := math.Pow(10, float64(decimals))
	return math.Round(num*multiplier) / multiplier, nil
}

// FloorFunc returns the largest integer less than or equal to a number
type FloorFunc struct{}

func (f *FloorFunc) Name() string  { return "FLOOR" }
func (f *FloorFunc) MinArity() int { return 1 }
func (f *FloorFunc) MaxArity() int { return 1 }
func (f *FloorFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("FLOOR: %w", err)
	}
	return math.Floor(num), nil
}

// CeilFunc returns the smallest integer greater than or equal to a number
type CeilFunc struct{}

func (f *CeilFunc) Name() string  { return "CEIL" }
func (f *CeilFunc) MinArity() int { return 1 }
func (f *CeilFunc) MaxArity() int { return 1 }
func (f *CeilFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("CEIL: %w", err)
	}
	return math.Ceil(num), nil
}

// ModFunc returns the remainder of division
type ModFunc struct{}

func (f *ModFunc) Name() string  { return "MOD" }
func (f *ModFunc) MinArity() int { return 2 }
func (f *ModFunc) MaxArity() int { return 2 }
func (f *ModFunc) Evaluate(args []interface{}) (interface{}, error) {
	dividend, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MOD: dividend: %w", err)
	}

	divisor, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MOD: divisor: %w", err)
	}

	if divisor == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}

	return math.Mod(dividend, divisor), nil
}

// Additional String Functions

// LTrimFunc trims whitespace from the left side of a string
type LTrimFunc struct{}

func (f *LTrimFunc) Name() string  { return "LTRIM" }
func (f *LTrimFunc) MinArity() int { return 1 }
func (f *LTrimFunc) MaxArity() int { return 1 }
func (f *LTrimFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("LTRIM: %w", err)
	}
	return strings.TrimLeft(str, " \t\n\r"), nil
}

// RTrimFunc trims whitespace from the right side of a string
type RTrimFunc struct{}

func (f *RTrimFunc) Name() string  { return "RTRIM" }
func (f *RTrimFunc) MinArity() int { return 1 }
func (f *RTrimFunc) MaxArity() int { return 1 }
func (f *RTrimFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("RTRIM: %w", err)
	}
	return strings.TrimRight(str, " \t\n\r"), nil
}

// SubstringFunc extracts a substring (1-indexed, SQL style)
type SubstringFunc struct{}

func (f *SubstringFunc) Name() string  { return "SUBSTRING" }
func (f *SubstringFunc) MinArity() int { return 2 }
func (f *SubstringFunc) MaxArity() int { return 3 }
func (f *SubstringFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING: %w", err)
	}

	start, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING: start: %w", err)
	}
	startIdx := int(start) - 1 // SQL uses 1-based indexing

	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(str) {
		return "", nil
	}

	if len(args) == 3 {
		length, err := valueToNumber(args[2])
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING: length: %w", err)
		}
		lengthInt := int(length)
		if lengthInt < 0 {
			return "", nil
		}
		endIdx := startIdx + lengthInt
		if endIdx > len(str) {
			endIdx = len(str)
		}
		return str[startIdx:endIdx], nil
	}

	return str[startIdx:], nil
}

// ReplaceFunc replaces occurrences of a substring
type ReplaceFunc struct{}

func (f *ReplaceFunc) Name() string  { return "REPLACE" }
func (f *ReplaceFunc) MinArity() int { return 3 }
func (f *ReplaceFunc) MaxArity() int { return 3 }
func (f *ReplaceFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("REPLACE: %w", err)
	}

	old, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("REPLACE: old: %w", err)
	}

	new, err := valueToString(args[2])
	if err != nil {
		return nil, fmt.Errorf("REPLACE: new: %w", err)
	}

	return strings.ReplaceAll(str, old, new), nil
}

// SplitFunc splits a string by a delimiter
type SplitFunc struct{}

func (f *SplitFunc) Name() string  { return "SPLIT" }
func (f *SplitFunc) MinArity() int { return 2 }
func (f *SplitFunc) MaxArity() int { return 2 }
func (f *SplitFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("SPLIT: %w", err)
	}

	delim, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("SPLIT: delimiter: %w", err)
	}

	return strings.Split(str, delim), nil
}

// ReverseFunc reverses a string
type ReverseFunc struct{}

func (f *ReverseFunc) Name() string  { return "REVERSE" }
func (f *ReverseFunc) MinArity() int { return 1 }
func (f *ReverseFunc) MaxArity() int { return 1 }
func (f *ReverseFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("REVERSE: %w", err)
	}

	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

// ContainsFunc checks if a string contains a substring
type ContainsFunc struct{}

func (f *ContainsFunc) Name() string  { return "CONTAINS" }
func (f *ContainsFunc) MinArity() int { return 2 }
func (f *ContainsFunc) MaxArity() int { return 2 }
func (f *ContainsFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("CONTAINS: %w", err)
	}

	substr, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("CONTAINS: substring: %w", err)
	}

	return strings.Contains(str, substr), nil
}

// StartsWithFunc checks if a string starts with a prefix
type StartsWithFunc struct{}

func (f *StartsWithFunc) Name() string  { return "STARTS_WITH" }
func (f *StartsWithFunc) MinArity() int { return 2 }
func (f *StartsWithFunc) MaxArity() int { return 2 }
func (f *StartsWithFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("STARTS_WITH: %w", err)
	}

	prefix, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("STARTS_WITH: prefix: %w", err)
	}

	return strings.HasPrefix(str, prefix), nil
}

// EndsWithFunc checks if a string ends with a suffix
type EndsWithFunc struct{}

func (f *EndsWithFunc) Name() string  { return "ENDS_WITH" }
func (f *EndsWithFunc) MinArity() int { return 2 }
func (f *EndsWithFunc) MaxArity() int { return 2 }
func (f *EndsWithFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("ENDS_WITH: %w", err)
	}

	suffix, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("ENDS_WITH: suffix: %w", err)
	}

	return strings.HasSuffix(str, suffix), nil
}

// RepeatFunc repeats a string n times
type RepeatFunc struct{}

func (f *RepeatFunc) Name() string  { return "REPEAT" }
func (f *RepeatFunc) MinArity() int { return 2 }
func (f *RepeatFunc) MaxArity() int { return 2 }
func (f *RepeatFunc) Evaluate(args []interface{}) (interface{}, error) {
	str, err := valueToString(args[0])
	if err != nil {
		return nil, fmt.Errorf("REPEAT: %w", err)
	}

	count, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("REPEAT: count: %w", err)
	}

	countInt := int(count)
	if countInt < 0 {
		return nil, fmt.Errorf("REPEAT: count must be non-negative, got %d", countInt)
	}
	if countInt > 1000000 {
		return nil, fmt.Errorf("REPEAT: count too large (max 1000000), got %d", countInt)
	}
	// Prevent memory exhaustion from large strings repeated many times
	const maxTotalBytes = 10 * 1024 * 1024 // 10MB
	if len(str)*countInt > maxTotalBytes {
		return nil, fmt.Errorf("REPEAT: result would be too large (max %d bytes), got %d * %d = %d bytes",
			maxTotalBytes, len(str), countInt, len(str)*countInt)
	}

	return strings.Repeat(str, countInt), nil
}

// Additional Math Functions

// SqrtFunc returns the square root
type SqrtFunc struct{}

func (f *SqrtFunc) Name() string  { return "SQRT" }
func (f *SqrtFunc) MinArity() int { return 1 }
func (f *SqrtFunc) MaxArity() int { return 1 }
func (f *SqrtFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("SQRT: %w", err)
	}
	if num < 0 {
		return nil, fmt.Errorf("SQRT: negative number")
	}
	return math.Sqrt(num), nil
}

// PowFunc returns x raised to the power of y
type PowFunc struct{}

func (f *PowFunc) Name() string  { return "POW" }
func (f *PowFunc) MinArity() int { return 2 }
func (f *PowFunc) MaxArity() int { return 2 }
func (f *PowFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("POW: base: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("POW: exponent: %w", err)
	}

	return math.Pow(x, y), nil
}

// SignFunc returns the sign of a number (-1, 0, or 1)
type SignFunc struct{}

func (f *SignFunc) Name() string  { return "SIGN" }
func (f *SignFunc) MinArity() int { return 1 }
func (f *SignFunc) MaxArity() int { return 1 }
func (f *SignFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("SIGN: %w", err)
	}

	if num < 0 {
		return float64(-1), nil
	}
	if num > 0 {
		return float64(1), nil
	}
	return float64(0), nil
}

// TruncFunc truncates a number to an integer
type TruncFunc struct{}

func (f *TruncFunc) Name() string  { return "TRUNC" }
func (f *TruncFunc) MinArity() int { return 1 }
func (f *TruncFunc) MaxArity() int { return 1 }
func (f *TruncFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("TRUNC: %w", err)
	}
	return math.Trunc(num), nil
}

// RandomFunc returns a random number between 0 and 1
type RandomFunc struct{}

func (f *RandomFunc) Name() string  { return "RANDOM" }
func (f *RandomFunc) MinArity() int { return 0 }
func (f *RandomFunc) MaxArity() int { return 0 }
func (f *RandomFunc) Evaluate(args []interface{}) (interface{}, error) {
	return rand.Float64(), nil
}

// MinFunc returns the minimum of two values
type MinFunc struct{}

func (f *MinFunc) Name() string  { return "MIN" }
func (f *MinFunc) MinArity() int { return 2 }
func (f *MinFunc) MaxArity() int { return 2 }
func (f *MinFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MIN: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MIN: %w", err)
	}

	return math.Min(x, y), nil
}

// MaxFunc returns the maximum of two values
type MaxFunc struct{}

func (f *MaxFunc) Name() string  { return "MAX" }
func (f *MaxFunc) MinArity() int { return 2 }
func (f *MaxFunc) MaxArity() int { return 2 }
func (f *MaxFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MAX: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MAX: %w", err)
	}

	return math.Max(x, y), nil
}

// Date/Time Functions

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
	case "year":
		return date.AddDate(int(amount), 0, 0).Format(time.RFC3339), nil
	case "month":
		return date.AddDate(0, int(amount), 0).Format(time.RFC3339), nil
	case "day":
		return date.AddDate(0, 0, int(amount)).Format(time.RFC3339), nil
	case "hour":
		return date.Add(time.Duration(amount) * time.Hour).Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("DATE_ADD: invalid unit: %s", unit)
	}
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
	case "year":
		return date.AddDate(-int(amount), 0, 0).Format(time.RFC3339), nil
	case "month":
		return date.AddDate(0, -int(amount), 0).Format(time.RFC3339), nil
	case "day":
		return date.AddDate(0, 0, -int(amount)).Format(time.RFC3339), nil
	case "hour":
		return date.Add(-time.Duration(amount) * time.Hour).Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("DATE_SUB: invalid unit: %s", unit)
	}
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

// Type Conversion Functions

// CastFunc converts a value to a specific type
type CastFunc struct{}

func (f *CastFunc) Name() string  { return "CAST" }
func (f *CastFunc) MinArity() int { return 2 }
func (f *CastFunc) MaxArity() int { return 2 }
func (f *CastFunc) Evaluate(args []interface{}) (interface{}, error) {
	value := args[0]
	typeName, err := valueToString(args[1])
	if err != nil {
		return nil, fmt.Errorf("CAST: type: %w", err)
	}

	switch strings.ToLower(typeName) {
	case "string":
		return valueToString(value)
	case "number":
		return valueToNumber(value)
	case "date":
		return parseDate(value)
	default:
		return nil, fmt.Errorf("CAST: unknown type: %s", typeName)
	}
}

// TryCastFunc converts a value to a specific type, returning null on error
type TryCastFunc struct{}

func (f *TryCastFunc) Name() string  { return "TRY_CAST" }
func (f *TryCastFunc) MinArity() int { return 2 }
func (f *TryCastFunc) MaxArity() int { return 2 }
func (f *TryCastFunc) Evaluate(args []interface{}) (interface{}, error) {
	value := args[0]
	typeName, err := valueToString(args[1])
	if err != nil {
		return nil, nil
	}

	switch strings.ToLower(typeName) {
	case "string":
		result, err := valueToString(value)
		if err != nil {
			return nil, nil
		}
		return result, nil
	case "number":
		result, err := valueToNumber(value)
		if err != nil {
			return nil, nil
		}
		return result, nil
	case "date":
		result, err := parseDate(value)
		if err != nil {
			return nil, nil
		}
		return result, nil
	default:
		return nil, nil
	}
}

// ToStringFunc converts a value to a string
type ToStringFunc struct{}

func (f *ToStringFunc) Name() string  { return "TO_STRING" }
func (f *ToStringFunc) MinArity() int { return 1 }
func (f *ToStringFunc) MaxArity() int { return 1 }
func (f *ToStringFunc) Evaluate(args []interface{}) (interface{}, error) {
	return valueToString(args[0])
}

// ToNumberFunc converts a value to a number
type ToNumberFunc struct{}

func (f *ToNumberFunc) Name() string  { return "TO_NUMBER" }
func (f *ToNumberFunc) MinArity() int { return 1 }
func (f *ToNumberFunc) MaxArity() int { return 1 }
func (f *ToNumberFunc) Evaluate(args []interface{}) (interface{}, error) {
	return valueToNumber(args[0])
}

// ToDateFunc converts a value to a date
type ToDateFunc struct{}

func (f *ToDateFunc) Name() string  { return "TO_DATE" }
func (f *ToDateFunc) MinArity() int { return 1 }
func (f *ToDateFunc) MaxArity() int { return 1 }
func (f *ToDateFunc) Evaluate(args []interface{}) (interface{}, error) {
	date, err := parseDate(args[0])
	if err != nil {
		return nil, err
	}
	return date.Format("2006-01-02"), nil
}

// Conditional Functions

// CoalesceFunc returns the first non-null value
type CoalesceFunc struct{}

func (f *CoalesceFunc) Name() string  { return "COALESCE" }
func (f *CoalesceFunc) MinArity() int { return 1 }
func (f *CoalesceFunc) MaxArity() int { return -1 }
func (f *CoalesceFunc) Evaluate(args []interface{}) (interface{}, error) {
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

// NullIfFunc returns null if two values are equal, otherwise returns the first value
type NullIfFunc struct{}

func (f *NullIfFunc) Name() string  { return "NULLIF" }
func (f *NullIfFunc) MinArity() int { return 2 }
func (f *NullIfFunc) MaxArity() int { return 2 }
func (f *NullIfFunc) Evaluate(args []interface{}) (interface{}, error) {
	// Handle nil values
	if args[0] == nil && args[1] == nil {
		return nil, nil
	}
	if args[0] == nil || args[1] == nil {
		return args[0], nil
	}

	// Use safe comparison via compare function to avoid panic on non-comparable types
	match, err := compare(args[0], TokenEqual, args[1])
	if err != nil {
		// If comparison fails, values are not equal
		return args[0], nil
	}

	if match {
		return nil, nil
	}
	return args[0], nil
}
