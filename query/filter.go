package query

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// abs returns the absolute value of a float64
func abs(x float64) float64 {
	return math.Abs(x)
}

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
	const epsilon = 1e-9 // Use small epsilon for floating point comparison
	switch operator {
	case TokenEqual:
		// Use relative epsilon for large numbers, absolute for small
		diff := abs(left - right)
		maxAbs := max(abs(left), abs(right))
		// Use epsilon scaled by the larger of 1.0 or maxAbs for consistent comparison
		threshold := epsilon * max(1.0, maxAbs)
		return diff < threshold
	case TokenNotEqual:
		// Use relative epsilon for large numbers, absolute for small
		diff := abs(left - right)
		maxAbs := max(abs(left), abs(right))
		// Use epsilon scaled by the larger of 1.0 or maxAbs for consistent comparison
		threshold := epsilon * max(1.0, maxAbs)
		return diff >= threshold
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
	return ApplyFilterWithContext(rows, filter, nil)
}

// ApplyFilterWithContext applies a filter to rows with execution context for subquery support
func ApplyFilterWithContext(rows []map[string]interface{}, filter Expression, ctx *ExecutionContext) ([]map[string]interface{}, error) {
	if filter == nil {
		return rows, nil
	}

	filtered := make([]map[string]interface{}, 0)
	for _, row := range rows {
		var match bool
		var err error

		// Use context-aware evaluation if context is provided (handles nested subqueries in compound expressions)
		if ctx != nil {
			match, err = ctx.EvaluateExpression(row, filter)
		} else {
			match, err = filter.Evaluate(row)
		}

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

// ApplySelectListAfterWindows applies column projection after window functions have been computed
// Window expressions are treated as column references (already computed values)
func ApplySelectListAfterWindows(rows []map[string]interface{}, selectList []SelectItem) ([]map[string]interface{}, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	projected := make([]map[string]interface{}, 0, len(rows))

	for _, row := range rows {
		newRow := make(map[string]interface{})

		for _, item := range selectList {
			// Determine the column name for the result
			columnName := item.Alias
			if columnName == "" {
				if colRef, ok := item.Expr.(*ColumnRef); ok {
					columnName = colRef.Column
				} else if winExpr, ok := item.Expr.(*WindowExpr); ok {
					// Window functions already computed, use function name as column name
					columnName = winExpr.Function
				} else if funcCall, ok := item.Expr.(*FunctionCall); ok {
					columnName = funcCall.Name
				} else {
					columnName = fmt.Sprintf("col_%d", len(newRow))
				}
			}

			// For window expressions, the value is already in the row with the column name
			if _, ok := item.Expr.(*WindowExpr); ok {
				value, exists := row[columnName]
				if !exists {
					return nil, fmt.Errorf("window function result %q not found in row", columnName)
				}
				newRow[columnName] = value
			} else {
				// Evaluate other expressions normally
				value, err := item.Expr.EvaluateSelect(row)
				if err != nil {
					return nil, err
				}
				newRow[columnName] = value
			}
		}

		projected = append(projected, newRow)
	}

	return projected, nil
}

// ApplySelectList applies column projection to rows based on the SELECT list
func ApplySelectList(rows []map[string]interface{}, selectList []SelectItem) ([]map[string]interface{}, error) {
	return ApplySelectListWithContext(rows, selectList, nil)
}

// ApplySelectListWithContext applies column projection with execution context for scalar subquery support
func ApplySelectListWithContext(rows []map[string]interface{}, selectList []SelectItem, ctx *ExecutionContext) ([]map[string]interface{}, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	// If no select list or only SELECT *, return all columns
	if len(selectList) == 0 {
		return rows, nil
	}

	// Check if it's just SELECT *
	if len(selectList) == 1 {
		if colRef, ok := selectList[0].Expr.(*ColumnRef); ok && colRef.Column == "*" {
			return rows, nil
		}
	}

	projected := make([]map[string]interface{}, 0, len(rows))

	for _, row := range rows {
		newRow := make(map[string]interface{})

		for _, item := range selectList {
			// Special handling for SELECT * in mixed select lists
			if colRef, ok := item.Expr.(*ColumnRef); ok && colRef.Column == "*" {
				// Expand all columns from the row instead of treating * as a column
				for col, val := range row {
					newRow[col] = val
				}
				continue
			}

			// Evaluate the select expression
			var value interface{}
			var err error

			// Use context-aware evaluation if context is provided (handles nested subqueries in expressions)
			if ctx != nil {
				value, err = ctx.EvaluateSelectExpression(row, item.Expr)
			} else {
				value, err = item.Expr.EvaluateSelect(row)
			}

			if err != nil {
				return nil, err
			}

			// Determine the column name to use
			columnName := item.Alias
			if columnName == "" {
				// If no alias, try to derive name from expression
				if colRef, ok := item.Expr.(*ColumnRef); ok {
					columnName = colRef.Column
				} else if funcCall, ok := item.Expr.(*FunctionCall); ok {
					// For function calls, use the function name as column name
					columnName = funcCall.Name
				} else if _, ok := item.Expr.(*LiteralExpr); ok {
					// For literals, use a generated name
					columnName = fmt.Sprintf("literal_%d", len(newRow))
				} else {
					// Fallback: use a generated name
					columnName = fmt.Sprintf("col_%d", len(newRow))
				}
			}

			newRow[columnName] = value
		}

		projected = append(projected, newRow)
	}

	return projected, nil
}

// ApplyOrderBy sorts rows based on ORDER BY clause
func ApplyOrderBy(rows []map[string]interface{}, orderBy []OrderByItem) ([]map[string]interface{}, error) {
	if len(rows) == 0 || len(orderBy) == 0 {
		return rows, nil
	}

	// Create a copy to avoid modifying the original slice
	sorted := make([]map[string]interface{}, len(rows))
	copy(sorted, rows)

	// Sort the rows
	sort.Slice(sorted, func(i, j int) bool {
		for _, item := range orderBy {
			// Get values for the column
			valI, existsI := sorted[i][item.Column]
			valJ, existsJ := sorted[j][item.Column]

			// Handle missing columns (treat as NULL, which sorts first)
			if !existsI && !existsJ {
				continue // Both NULL, try next column
			}
			if !existsI {
				return !item.Desc // NULL sorts first (or last if DESC)
			}
			if !existsJ {
				return item.Desc // NULL sorts first (or last if DESC)
			}

			// Compare the values
			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			// Values are equal, continue to next ORDER BY column
		}
		return false // All columns equal
	})

	return sorted, nil
}

// compareValues compares two values and returns:
// -1 if a < b
//
//	0 if a == b
//
// +1 if a > b
func compareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	// Try string comparison
	aStr, aIsStr := toString(a)
	bStr, bIsStr := toString(b)
	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0
	}

	// Try boolean comparison
	aBool, aIsBool := toBool(a)
	bBool, bIsBool := toBool(b)
	if aIsBool && bIsBool {
		if !aBool && bBool {
			return -1 // false < true
		}
		if aBool && !bBool {
			return 1 // true > false
		}
		return 0
	}

	// Type mismatch or unsupported types - treat as equal
	return 0
}

// ApplyLimitOffset applies LIMIT and OFFSET to rows
func ApplyLimitOffset(rows []map[string]interface{}, limit *int64, offset *int64) ([]map[string]interface{}, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	start := int64(0)
	if offset != nil && *offset > 0 {
		start = *offset
	}

	// If offset is beyond the end, return empty
	if start >= int64(len(rows)) {
		return []map[string]interface{}{}, nil
	}

	end := int64(len(rows))
	if limit != nil {
		if *limit == 0 {
			// LIMIT 0 returns empty result
			return []map[string]interface{}{}, nil
		}
		if *limit > 0 {
			end = start + *limit
			if end > int64(len(rows)) {
				end = int64(len(rows))
			}
		}
	}

	return rows[start:end], nil
}

// ApplyDistinct removes duplicate rows
func ApplyDistinct(rows []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	// Use a map to track seen rows (hash-based deduplication)
	seen := make(map[string]bool)
	distinct := make([]map[string]interface{}, 0)

	for _, row := range rows {
		// Create a hash key from the row values
		key := rowToKey(row)
		if !seen[key] {
			seen[key] = true
			distinct = append(distinct, row)
		}
	}

	return distinct, nil
}

// rowToKey creates a unique string key from a row for deduplication
func rowToKey(row map[string]interface{}) string {
	// Get all column names sorted for consistent key generation
	columns := make([]string, 0, len(row))
	for col := range row {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build key from column values
	var key strings.Builder
	for i, col := range columns {
		if i > 0 {
			key.WriteString("\x00||\x00") // Use unlikely separator to avoid collisions
		}
		key.WriteString(col)
		key.WriteString("\x00:\x00")
		key.WriteString(fmt.Sprintf("%#v", row[col])) // Use %#v for better type differentiation
	}

	return key.String()
}

// matchLikePattern matches a string against a SQL LIKE pattern
// % matches any sequence of characters
// _ matches any single character
func matchLikePattern(str, pattern string) bool {
	// Convert pattern to segments split by %
	segments := strings.Split(pattern, "%")

	// Track position in the string
	pos := 0

	for i, segment := range segments {
		if segment == "" {
			// Empty segment means % was at start/end or consecutive %%
			continue
		}

		// Match the segment (handling _ wildcards)
		matchPos := findSegmentMatch(str[pos:], segment)
		if matchPos == -1 {
			return false
		}

		// For the first segment, it must match at the start (unless pattern starts with %)
		if i == 0 && !strings.HasPrefix(pattern, "%") && matchPos != 0 {
			return false
		}

		pos += matchPos + len(segment)
	}

	// For the last segment, it must match at the end (unless pattern ends with %)
	if !strings.HasSuffix(pattern, "%") && pos != len(str) {
		return false
	}

	return true
}

// findSegmentMatch finds the position where a segment matches in the string
// Returns -1 if no match found
// Handles _ wildcard matching any single character
func findSegmentMatch(str, segment string) int {
	if len(segment) == 0 {
		return 0
	}

	// If no _ wildcards, use simple string search
	if !strings.Contains(segment, "_") {
		idx := strings.Index(str, segment)
		return idx
	}

	// Handle _ wildcards
	for i := 0; i <= len(str)-len(segment); i++ {
		match := true
		for j := 0; j < len(segment); j++ {
			if segment[j] != '_' && str[i+j] != segment[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}

	return -1
}
