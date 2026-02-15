package query

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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
