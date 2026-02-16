package query

import (
	"fmt"
	"strings"
)

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
