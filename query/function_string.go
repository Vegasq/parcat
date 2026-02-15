package query

import (
	"fmt"
	"strings"
)

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
	// Convert to runes to handle multibyte UTF-8 characters correctly
	runes := []rune(str)
	startIdx := int(start) - 1 // SQL uses 1-based indexing

	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(runes) {
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
		if endIdx > len(runes) {
			endIdx = len(runes)
		}
		return string(runes[startIdx:endIdx]), nil
	}

	return string(runes[startIdx:]), nil
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
	// Prevent memory exhaustion from large strings repeated many times
	const maxTotalBytes = 10 * 1024 * 1024 // 10MB
	// Check for overflow before multiplication
	if len(str) > 0 && countInt > maxTotalBytes/len(str) {
		return nil, fmt.Errorf("REPEAT: result would be too large")
	}

	return strings.Repeat(str, countInt), nil
}
