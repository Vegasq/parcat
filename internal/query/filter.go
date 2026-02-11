package query

import (
	"fmt"
	"reflect"
)

// compare compares two values using the given operator
func compare(left interface{}, operator TokenType, right interface{}) (bool, error) {
	// Handle nil values
	if left == nil || right == nil {
		if operator == TokenEqual {
			return left == right, nil
		}
		if operator == TokenNotEqual {
			return left != right, nil
		}
		return false, nil
	}

	// Try numeric comparison
	leftNum, leftIsNum := toFloat64(left)
	rightNum, rightIsNum := toFloat64(right)

	if leftIsNum && rightIsNum {
		return compareNumbers(leftNum, operator, rightNum), nil
	}

	// Try string comparison
	leftStr, leftIsStr := toString(left)
	rightStr, rightIsStr := toString(right)

	if leftIsStr && rightIsStr {
		return compareStrings(leftStr, operator, rightStr), nil
	}

	// Try boolean comparison
	leftBool, leftIsBool := toBool(left)
	rightBool, rightIsBool := toBool(right)

	if leftIsBool && rightIsBool {
		return compareBools(leftBool, operator, rightBool), nil
	}

	// Type mismatch
	return false, fmt.Errorf("cannot compare %T with %T", left, right)
}

// toFloat64 converts a value to float64 if possible
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// toString converts a value to string if possible
func toString(v interface{}) (string, bool) {
	if str, ok := v.(string); ok {
		return str, true
	}
	return "", false
}

// toBool converts a value to bool if possible
func toBool(v interface{}) (bool, bool) {
	if b, ok := v.(bool); ok {
		return b, true
	}
	return false, false
}

// compareNumbers compares two numbers
func compareNumbers(left float64, operator TokenType, right float64) bool {
	switch operator {
	case TokenEqual:
		return left == right
	case TokenNotEqual:
		return left != right
	case TokenLess:
		return left < right
	case TokenGreater:
		return left > right
	case TokenLessEqual:
		return left <= right
	case TokenGreaterEqual:
		return left >= right
	default:
		return false
	}
}

// compareStrings compares two strings (case-sensitive)
func compareStrings(left string, operator TokenType, right string) bool {
	switch operator {
	case TokenEqual:
		return left == right
	case TokenNotEqual:
		return left != right
	case TokenLess:
		return left < right
	case TokenGreater:
		return left > right
	case TokenLessEqual:
		return left <= right
	case TokenGreaterEqual:
		return left >= right
	default:
		return false
	}
}

// compareBools compares two booleans
func compareBools(left bool, operator TokenType, right bool) bool {
	switch operator {
	case TokenEqual:
		return left == right
	case TokenNotEqual:
		return left != right
	default:
		return false
	}
}

// ApplyFilter applies a filter to rows
func ApplyFilter(rows []map[string]interface{}, filter Expression) ([]map[string]interface{}, error) {
	if filter == nil {
		return rows, nil
	}

	filtered := make([]map[string]interface{}, 0)
	for _, row := range rows {
		match, err := filter.Evaluate(row)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, row)
		}
	}

	return filtered, nil
}

// GetColumnNames returns all unique column names from rows
func GetColumnNames(rows []map[string]interface{}) []string {
	if len(rows) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	columns := make([]string, 0)

	for _, row := range rows {
		for col := range row {
			if !seen[col] {
				seen[col] = true
				columns = append(columns, col)
			}
		}
	}

	return columns
}

// init registers reflection for common types
func init() {
	// This ensures reflect package is properly initialized
	_ = reflect.TypeOf(0)
}
