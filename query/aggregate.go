package query

import (
	"fmt"
	"strings"
)

// Group represents a group of rows for aggregation
type Group struct {
	Key    string                   // Hash key for the group
	Values map[string]interface{}   // Column values for GROUP BY columns
	Rows   []map[string]interface{} // All rows in the group
}

// ApplyGroupByAndAggregate applies GROUP BY and aggregation to rows
func ApplyGroupByAndAggregate(rows []map[string]interface{}, groupByColumns []string, selectList []SelectItem) ([]map[string]interface{}, error) {
	// Validate SELECT list before aggregation
	if err := validateSelectListWithGroupBy(selectList, groupByColumns); err != nil {
		return nil, err
	}

	// If no GROUP BY, treat all rows as one group (for aggregates without GROUP BY)
	// This should return one aggregate row even when input is empty (e.g., COUNT(*) = 0)
	if len(groupByColumns) == 0 {
		return aggregateWithoutGroupBy(rows, selectList)
	}

	// For GROUP BY queries, empty input returns empty output
	if len(rows) == 0 {
		return rows, nil
	}

	// Hash-based grouping
	groups := make(map[string]*Group)

	for _, row := range rows {
		// Compute group key from GROUP BY columns
		key, groupValues, err := computeGroupKey(row, groupByColumns)
		if err != nil {
			return nil, err
		}

		// Add row to group
		if group, exists := groups[key]; exists {
			group.Rows = append(group.Rows, row)
		} else {
			groups[key] = &Group{
				Key:    key,
				Values: groupValues,
				Rows:   []map[string]interface{}{row},
			}
		}
	}

	// Compute aggregates for each group
	result := make([]map[string]interface{}, 0, len(groups))
	for _, group := range groups {
		aggregatedRow, err := computeAggregates(group, selectList)
		if err != nil {
			return nil, err
		}
		result = append(result, aggregatedRow)
	}

	return result, nil
}

// computeGroupKey computes a hash key for a group based on GROUP BY columns
func computeGroupKey(row map[string]interface{}, groupByColumns []string) (string, map[string]interface{}, error) {
	var keyBuilder strings.Builder
	groupValues := make(map[string]interface{})

	for i, col := range groupByColumns {
		value, exists := row[col]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY column %q not found in row", col)
		}

		if i > 0 {
			keyBuilder.WriteString("\x00||\x00") // Use unlikely separator to avoid collisions
		}
		// Include column name in key to prevent cross-column collisions
		keyBuilder.WriteString(col)
		keyBuilder.WriteString("\x00:\x00")
		keyBuilder.WriteString(fmt.Sprintf("%#v", value)) // Use %#v for better type differentiation
		groupValues[col] = value
	}

	return keyBuilder.String(), groupValues, nil
}

// aggregateWithoutGroupBy handles aggregation without GROUP BY (all rows as one group)
func aggregateWithoutGroupBy(rows []map[string]interface{}, selectList []SelectItem) ([]map[string]interface{}, error) {
	group := &Group{
		Key:    "",
		Values: make(map[string]interface{}),
		Rows:   rows,
	}

	aggregatedRow, err := computeAggregates(group, selectList)
	if err != nil {
		return nil, err
	}

	return []map[string]interface{}{aggregatedRow}, nil
}

// computeAggregates computes aggregate values for a group
func computeAggregates(group *Group, selectList []SelectItem) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Compute each SELECT item
	// Note: GROUP BY columns are only included if explicitly selected
	for _, item := range selectList {
		var value interface{}
		var err error

		// Check if it's an aggregate expression
		if aggExpr, ok := item.Expr.(*AggregateExpr); ok {
			value, err = evaluateAggregate(aggExpr, group.Rows)
			if err != nil {
				return nil, err
			}
		} else if colRef, ok := item.Expr.(*ColumnRef); ok {
			// For non-aggregate columns, use the value from the first row in the group
			// (should be the same for all rows in the group if it's a GROUP BY column)
			if len(group.Rows) == 0 {
				return nil, fmt.Errorf("column %q not found in empty group", colRef.Column)
			}
			val, exists := group.Rows[0][colRef.Column]
			if !exists {
				return nil, fmt.Errorf("column %q not found", colRef.Column)
			}
			value = val
		} else {
			return nil, fmt.Errorf("non-aggregate expression in SELECT with GROUP BY is not supported")
		}

		// Determine column name
		columnName := item.Alias
		if columnName == "" {
			if aggExpr, ok := item.Expr.(*AggregateExpr); ok {
				columnName = strings.ToLower(aggExpr.Function)
			} else if colRef, ok := item.Expr.(*ColumnRef); ok {
				columnName = colRef.Column
			} else {
				columnName = fmt.Sprintf("col_%d", len(result))
			}
		}

		result[columnName] = value
	}

	return result, nil
}

// evaluateAggregate evaluates an aggregate function over a set of rows
func evaluateAggregate(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	switch aggExpr.Function {
	case "COUNT":
		return evaluateCount(aggExpr, rows)
	case "SUM":
		return evaluateSum(aggExpr, rows)
	case "AVG":
		return evaluateAvg(aggExpr, rows)
	case "MIN":
		return evaluateMin(aggExpr, rows)
	case "MAX":
		return evaluateMax(aggExpr, rows)
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", aggExpr.Function)
	}
}

// evaluateCount evaluates COUNT aggregate
func evaluateCount(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	// COUNT(*) counts all rows
	if aggExpr.Arg == nil {
		return int64(len(rows)), nil
	}

	// COUNT(column) counts non-null values
	count := int64(0)
	for _, row := range rows {
		value, err := aggExpr.Arg.EvaluateSelect(row)
		if err != nil {
			// Skip rows where column doesn't exist or errors
			continue
		}
		if value != nil {
			count++
		}
	}

	return count, nil
}

// evaluateSum evaluates SUM aggregate
func evaluateSum(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	if aggExpr.Arg == nil {
		return nil, fmt.Errorf("SUM requires an argument")
	}

	sum := 0.0
	hasValues := false

	for _, row := range rows {
		value, err := aggExpr.Arg.EvaluateSelect(row)
		if err != nil {
			continue
		}
		if value == nil {
			continue
		}

		num, err := valueToNumber(value)
		if err != nil {
			return nil, fmt.Errorf("SUM: %w", err)
		}

		sum += num
		hasValues = true
	}

	if !hasValues {
		return nil, nil // Return NULL if no values
	}

	return sum, nil
}

// evaluateAvg evaluates AVG aggregate
func evaluateAvg(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	if aggExpr.Arg == nil {
		return nil, fmt.Errorf("AVG requires an argument")
	}

	sum := 0.0
	count := int64(0)

	for _, row := range rows {
		value, err := aggExpr.Arg.EvaluateSelect(row)
		if err != nil {
			continue
		}
		if value == nil {
			continue
		}

		num, err := valueToNumber(value)
		if err != nil {
			return nil, fmt.Errorf("AVG: %w", err)
		}

		sum += num
		count++
	}

	if count == 0 {
		return nil, nil // Return NULL if no values
	}

	return sum / float64(count), nil
}

// evaluateMin evaluates MIN aggregate
func evaluateMin(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	if aggExpr.Arg == nil {
		return nil, fmt.Errorf("MIN requires an argument")
	}

	var min *float64

	for _, row := range rows {
		value, err := aggExpr.Arg.EvaluateSelect(row)
		if err != nil {
			continue
		}
		if value == nil {
			continue
		}

		num, err := valueToNumber(value)
		if err != nil {
			return nil, fmt.Errorf("MIN: %w", err)
		}

		if min == nil || num < *min {
			min = &num
		}
	}

	if min == nil {
		return nil, nil // Return NULL if no values
	}

	return *min, nil
}

// evaluateMax evaluates MAX aggregate
func evaluateMax(aggExpr *AggregateExpr, rows []map[string]interface{}) (interface{}, error) {
	if aggExpr.Arg == nil {
		return nil, fmt.Errorf("MAX requires an argument")
	}

	var max *float64

	for _, row := range rows {
		value, err := aggExpr.Arg.EvaluateSelect(row)
		if err != nil {
			continue
		}
		if value == nil {
			continue
		}

		num, err := valueToNumber(value)
		if err != nil {
			return nil, fmt.Errorf("MAX: %w", err)
		}

		if max == nil || num > *max {
			max = &num
		}
	}

	if max == nil {
		return nil, nil // Return NULL if no values
	}

	return *max, nil
}

// EvaluateHaving evaluates the HAVING clause on aggregated rows
func EvaluateHaving(rows []map[string]interface{}, having Expression) ([]map[string]interface{}, error) {
	if having == nil {
		return rows, nil
	}

	filtered := make([]map[string]interface{}, 0)
	for _, row := range rows {
		match, err := having.Evaluate(row)
		if err != nil {
			return nil, fmt.Errorf("HAVING: %w", err)
		}
		if match {
			filtered = append(filtered, row)
		}
	}

	return filtered, nil
}

// hasAggregateFunction checks if the SELECT list contains any aggregate functions
func HasAggregateFunction(selectList []SelectItem) bool {
	for _, item := range selectList {
		if _, ok := item.Expr.(*AggregateExpr); ok {
			return true
		}
	}
	return false
}

// validateSelectListWithGroupBy validates that non-aggregate columns in SELECT are in GROUP BY
func validateSelectListWithGroupBy(selectList []SelectItem, groupByColumns []string) error {
	// Build map of GROUP BY columns for fast lookup
	groupByMap := make(map[string]bool)
	for _, col := range groupByColumns {
		groupByMap[col] = true
	}

	// Check if there are any aggregate functions
	hasAggregates := HasAggregateFunction(selectList)

	for _, item := range selectList {
		// Skip aggregate expressions - they are valid with or without GROUP BY
		if _, ok := item.Expr.(*AggregateExpr); ok {
			continue
		}

		// For non-aggregate column references
		if colRef, ok := item.Expr.(*ColumnRef); ok {
			// If there are aggregates but no GROUP BY, non-aggregate columns are not allowed
			if hasAggregates && len(groupByColumns) == 0 {
				return fmt.Errorf("column %q must appear in GROUP BY clause or be used in an aggregate function", colRef.Column)
			}

			// If there is a GROUP BY, non-aggregate columns must be in the GROUP BY list
			if len(groupByColumns) > 0 && !groupByMap[colRef.Column] {
				return fmt.Errorf("column %q must appear in GROUP BY clause or be used in an aggregate function", colRef.Column)
			}
		}
		// Other expression types (function calls, etc.) are handled elsewhere
	}

	return nil
}
