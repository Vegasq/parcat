package query

import (
	"fmt"
	"sort"
	"strings"
)

// HasWindowFunction checks if the SELECT list contains any window functions
func HasWindowFunction(selectList []SelectItem) bool {
	for _, item := range selectList {
		if _, ok := item.Expr.(*WindowExpr); ok {
			return true
		}
	}
	return false
}

// HasSubqueryInWHERE checks if the WHERE clause contains any subqueries
func HasSubqueryInWHERE(filter Expression) bool {
	if filter == nil {
		return false
	}
	return hasSubqueryInExpression(filter)
}

// hasSubqueryInExpression recursively checks if an expression contains subqueries
func hasSubqueryInExpression(expr Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *InSubqueryExpr:
		return true
	case *ExistsExpr:
		return true
	case *BinaryExpr:
		return hasSubqueryInExpression(e.Left) || hasSubqueryInExpression(e.Right)
	default:
		return false
	}
}

// HasSubqueryInSELECT checks if the SELECT list contains any scalar subqueries
func HasSubqueryInSELECT(selectList []SelectItem) bool {
	for _, item := range selectList {
		if hasScalarSubquery(item.Expr) {
			return true
		}
	}
	return false
}

// hasScalarSubquery recursively checks if a SelectExpression contains scalar subqueries
func hasScalarSubquery(expr SelectExpression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *ScalarSubqueryExpr:
		return true
	case *FunctionCall:
		// Check function arguments
		for _, arg := range e.Args {
			if hasScalarSubquery(arg) {
				return true
			}
		}
		return false
	case *CaseExpr:
		// Check ELSE expression
		if e.ElseExpr != nil && hasScalarSubquery(e.ElseExpr) {
			return true
		}
		// Check each WHEN result
		for _, when := range e.WhenClauses {
			if hasScalarSubquery(when.Result) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// ApplyWindowFunctions processes window functions in the SELECT list
// This must be called AFTER WHERE filtering but BEFORE regular column projection
func ApplyWindowFunctions(rows []map[string]interface{}, selectList []SelectItem) ([]map[string]interface{}, error) {
	// Check if there are any window functions
	hasWindowFunc := false
	for _, item := range selectList {
		if _, ok := item.Expr.(*WindowExpr); ok {
			hasWindowFunc = true
			break
		}
	}

	if !hasWindowFunc {
		return rows, nil
	}

	// Create a result set with window function results added as new columns
	result := make([]map[string]interface{}, len(rows))
	for i := range rows {
		// Copy the original row
		result[i] = make(map[string]interface{})
		for k, v := range rows[i] {
			result[i][k] = v
		}
	}

	// Process each window function
	for _, item := range selectList {
		windowExpr, ok := item.Expr.(*WindowExpr)
		if !ok {
			continue
		}

		// Compute window function results
		values, err := computeWindowFunction(rows, windowExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to compute window function %s: %w", windowExpr.Function, err)
		}

		// Determine the column name to use
		columnName := item.Alias
		if columnName == "" {
			columnName = windowExpr.Function
		}

		// Add the results to each row
		for i, value := range values {
			result[i][columnName] = value
		}
	}

	return result, nil
}

// computeWindowFunction computes the result of a window function for all rows
func computeWindowFunction(rows []map[string]interface{}, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(rows) == 0 {
		return []interface{}{}, nil
	}

	spec := windowExpr.Window
	if spec == nil {
		return nil, fmt.Errorf("window function %s requires OVER clause", windowExpr.Function)
	}

	// Create partitions based on PARTITION BY
	partitions := partitionRows(rows, spec.PartitionBy)

	// For each partition, sort by ORDER BY and compute window function
	results := make([]interface{}, len(rows))

	for _, partition := range partitions {
		// Sort partition by ORDER BY
		sortedPartition := sortPartition(partition, spec.OrderBy)

		// Compute window function for this partition
		partitionResults, err := computeWindowFunctionForPartition(sortedPartition, windowExpr)
		if err != nil {
			return nil, err
		}

		// Map results back to original row indices
		for i, rowInfo := range sortedPartition {
			results[rowInfo.originalIndex] = partitionResults[i]
		}
	}

	return results, nil
}

// rowInfo stores a row with its original index
type rowInfo struct {
	row           map[string]interface{}
	originalIndex int
}

// partitionRows partitions rows based on PARTITION BY columns
func partitionRows(rows []map[string]interface{}, partitionBy []string) [][]rowInfo {
	if len(partitionBy) == 0 {
		// No PARTITION BY: all rows are in one partition
		partition := make([]rowInfo, len(rows))
		for i, row := range rows {
			partition[i] = rowInfo{row: row, originalIndex: i}
		}
		return [][]rowInfo{partition}
	}

	// Create partitions based on partition key
	partitionMap := make(map[string][]rowInfo)

	for i, row := range rows {
		key := makePartitionKey(row, partitionBy)
		partitionMap[key] = append(partitionMap[key], rowInfo{row: row, originalIndex: i})
	}

	// Convert map to slice
	partitions := make([][]rowInfo, 0, len(partitionMap))
	for _, partition := range partitionMap {
		partitions = append(partitions, partition)
	}

	return partitions
}

// makePartitionKey creates a partition key from a row
func makePartitionKey(row map[string]interface{}, columns []string) string {
	var keyBuilder strings.Builder
	for i, col := range columns {
		if i > 0 {
			keyBuilder.WriteString("\x00||\x00") // Use unlikely separator to avoid collisions
		}
		// Include column name in key to prevent cross-column collisions
		keyBuilder.WriteString(col)
		keyBuilder.WriteString("\x00:\x00")
		keyBuilder.WriteString(fmt.Sprintf("%#v", row[col])) // Use %#v for better type differentiation
	}
	return keyBuilder.String()
}

// sortPartition sorts a partition by ORDER BY columns
func sortPartition(partition []rowInfo, orderBy []OrderByItem) []rowInfo {
	if len(orderBy) == 0 {
		return partition
	}

	sorted := make([]rowInfo, len(partition))
	copy(sorted, partition)

	sort.Slice(sorted, func(i, j int) bool {
		for _, item := range orderBy {
			valI := sorted[i].row[item.Column]
			valJ := sorted[j].row[item.Column]

			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return sorted
}

// computeWindowFunctionForPartition computes a window function for a sorted partition
func computeWindowFunctionForPartition(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	function := windowExpr.Function

	switch function {
	case "ROW_NUMBER":
		return computeRowNumber(partition, windowExpr)
	case "RANK":
		return computeRank(partition, windowExpr)
	case "DENSE_RANK":
		return computeDenseRank(partition, windowExpr)
	case "NTILE":
		return computeNTile(partition, windowExpr)
	case "FIRST_VALUE":
		return computeFirstValue(partition, windowExpr)
	case "LAST_VALUE":
		return computeLastValue(partition, windowExpr)
	case "NTH_VALUE":
		return computeNthValue(partition, windowExpr)
	case "LAG":
		return computeLag(partition, windowExpr)
	case "LEAD":
		return computeLead(partition, windowExpr)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", function)
	}
}

// computeRowNumber computes ROW_NUMBER() for a partition
func computeRowNumber(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	results := make([]interface{}, len(partition))
	for i := range partition {
		results[i] = int64(i + 1)
	}
	return results, nil
}

// computeRank computes RANK() for a partition
func computeRank(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	spec := windowExpr.Window
	orderBy := spec.OrderBy

	if len(orderBy) == 0 {
		// Without ORDER BY, all rows have the same rank
		results := make([]interface{}, len(partition))
		for i := range results {
			results[i] = int64(1)
		}
		return results, nil
	}

	results := make([]interface{}, len(partition))
	rank := int64(1)

	for i := 0; i < len(partition); i++ {
		if i > 0 && !rowsEqualOnOrderBy(partition[i-1].row, partition[i].row, orderBy) {
			rank = int64(i + 1)
		}
		results[i] = rank
	}

	return results, nil
}

// computeDenseRank computes DENSE_RANK() for a partition
func computeDenseRank(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	spec := windowExpr.Window
	orderBy := spec.OrderBy

	if len(orderBy) == 0 {
		// Without ORDER BY, all rows have the same rank
		results := make([]interface{}, len(partition))
		for i := range results {
			results[i] = int64(1)
		}
		return results, nil
	}

	results := make([]interface{}, len(partition))
	rank := int64(1)

	for i := 0; i < len(partition); i++ {
		if i > 0 && !rowsEqualOnOrderBy(partition[i-1].row, partition[i].row, orderBy) {
			rank++
		}
		results[i] = rank
	}

	return results, nil
}

// computeNTile computes NTILE(n) for a partition
func computeNTile(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) != 1 {
		return nil, fmt.Errorf("NTILE requires exactly one argument")
	}

	// Evaluate the argument to get the number of tiles
	nArg, err := windowExpr.Args[0].EvaluateSelect(partition[0].row)
	if err != nil {
		return nil, fmt.Errorf("NTILE: failed to evaluate argument: %w", err)
	}

	nFloat, ok := toFloat64(nArg)
	if !ok {
		return nil, fmt.Errorf("NTILE: argument must be a number")
	}
	n := int64(nFloat)

	if n <= 0 {
		return nil, fmt.Errorf("NTILE: argument must be positive")
	}

	results := make([]interface{}, len(partition))
	rowCount := int64(len(partition))

	// SQL standard allows NTILE(n) where n > rowCount
	// In this case, each row gets its own tile number (1, 2, 3, ...)
	// and tiles beyond rowCount are empty
	if n > rowCount {
		// Each row gets its own tile number
		for i := int64(0); i < rowCount; i++ {
			results[i] = i + 1
		}
		return results, nil
	}

	// Calculate tile size
	tileSize := rowCount / n
	remainder := rowCount % n

	tile := int64(1)
	rowsInCurrentTile := int64(0)
	currentTileSize := tileSize
	if remainder > 0 {
		currentTileSize++
	}

	for i := int64(0); i < rowCount; i++ {
		if rowsInCurrentTile >= currentTileSize {
			tile++
			rowsInCurrentTile = 0
			if tile-1 < remainder {
				currentTileSize = tileSize + 1
			} else {
				currentTileSize = tileSize
			}
		}
		results[i] = tile
		rowsInCurrentTile++
	}

	return results, nil
}

// computeFirstValue computes FIRST_VALUE(expr) for a partition
func computeFirstValue(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) != 1 {
		return nil, fmt.Errorf("FIRST_VALUE requires exactly one argument")
	}

	// Evaluate the expression on the first row
	firstValue, err := windowExpr.Args[0].EvaluateSelect(partition[0].row)
	if err != nil {
		return nil, fmt.Errorf("FIRST_VALUE: failed to evaluate argument: %w", err)
	}

	// Return the first value for all rows
	results := make([]interface{}, len(partition))
	for i := range results {
		results[i] = firstValue
	}

	return results, nil
}

// computeLastValue computes LAST_VALUE(expr) for a partition
func computeLastValue(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) != 1 {
		return nil, fmt.Errorf("LAST_VALUE requires exactly one argument")
	}

	// Evaluate the expression on the last row
	lastValue, err := windowExpr.Args[0].EvaluateSelect(partition[len(partition)-1].row)
	if err != nil {
		return nil, fmt.Errorf("LAST_VALUE: failed to evaluate argument: %w", err)
	}

	// Return the last value for all rows
	results := make([]interface{}, len(partition))
	for i := range results {
		results[i] = lastValue
	}

	return results, nil
}

// computeNthValue computes NTH_VALUE(expr, n) for a partition
func computeNthValue(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) != 2 {
		return nil, fmt.Errorf("NTH_VALUE requires exactly two arguments")
	}

	// Evaluate the n argument
	nArg, err := windowExpr.Args[1].EvaluateSelect(partition[0].row)
	if err != nil {
		return nil, fmt.Errorf("NTH_VALUE: failed to evaluate position argument: %w", err)
	}

	nFloat, ok := toFloat64(nArg)
	if !ok {
		return nil, fmt.Errorf("NTH_VALUE: position argument must be a number")
	}
	n := int(nFloat)

	if n <= 0 || n > len(partition) {
		// Return NULL for all rows if n is out of range
		results := make([]interface{}, len(partition))
		return results, nil
	}

	// Evaluate the expression on the nth row (1-indexed)
	nthValue, err := windowExpr.Args[0].EvaluateSelect(partition[n-1].row)
	if err != nil {
		return nil, fmt.Errorf("NTH_VALUE: failed to evaluate argument: %w", err)
	}

	// Return the nth value for all rows
	results := make([]interface{}, len(partition))
	for i := range results {
		results[i] = nthValue
	}

	return results, nil
}

// computeLag computes LAG(expr, offset, default) for a partition
func computeLag(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) < 1 || len(windowExpr.Args) > 3 {
		return nil, fmt.Errorf("LAG requires 1-3 arguments")
	}

	// Get offset (default 1)
	offset := int64(1)
	if len(windowExpr.Args) >= 2 {
		offsetArg, err := windowExpr.Args[1].EvaluateSelect(partition[0].row)
		if err != nil {
			return nil, fmt.Errorf("LAG: failed to evaluate offset argument: %w", err)
		}
		offsetFloat, ok := toFloat64(offsetArg)
		if !ok {
			return nil, fmt.Errorf("LAG: offset argument must be a number")
		}
		offset = int64(offsetFloat)
		if offset < 0 {
			return nil, fmt.Errorf("LAG: offset must be non-negative, got %d", offset)
		}
	}

	// Get default value (default NULL)
	var defaultValue interface{} = nil
	if len(windowExpr.Args) == 3 {
		var err error
		defaultValue, err = windowExpr.Args[2].EvaluateSelect(partition[0].row)
		if err != nil {
			return nil, fmt.Errorf("LAG: failed to evaluate default argument: %w", err)
		}
	}

	results := make([]interface{}, len(partition))
	for i := range partition {
		lagIndex := int64(i) - offset
		if lagIndex < 0 {
			results[i] = defaultValue
		} else {
			value, err := windowExpr.Args[0].EvaluateSelect(partition[lagIndex].row)
			if err != nil {
				return nil, fmt.Errorf("LAG: failed to evaluate expression: %w", err)
			}
			results[i] = value
		}
	}

	return results, nil
}

// computeLead computes LEAD(expr, offset, default) for a partition
func computeLead(partition []rowInfo, windowExpr *WindowExpr) ([]interface{}, error) {
	if len(partition) == 0 {
		return []interface{}{}, nil
	}

	if len(windowExpr.Args) < 1 || len(windowExpr.Args) > 3 {
		return nil, fmt.Errorf("LEAD requires 1-3 arguments")
	}

	// Get offset (default 1)
	offset := int64(1)
	if len(windowExpr.Args) >= 2 {
		offsetArg, err := windowExpr.Args[1].EvaluateSelect(partition[0].row)
		if err != nil {
			return nil, fmt.Errorf("LEAD: failed to evaluate offset argument: %w", err)
		}
		offsetFloat, ok := toFloat64(offsetArg)
		if !ok {
			return nil, fmt.Errorf("LEAD: offset argument must be a number")
		}
		offset = int64(offsetFloat)
		if offset < 0 {
			return nil, fmt.Errorf("LEAD: offset must be non-negative, got %d", offset)
		}
	}

	// Get default value (default NULL)
	var defaultValue interface{} = nil
	if len(windowExpr.Args) == 3 {
		var err error
		defaultValue, err = windowExpr.Args[2].EvaluateSelect(partition[0].row)
		if err != nil {
			return nil, fmt.Errorf("LEAD: failed to evaluate default argument: %w", err)
		}
	}

	results := make([]interface{}, len(partition))
	for i := range partition {
		leadIndex := int64(i) + offset
		if leadIndex >= int64(len(partition)) {
			results[i] = defaultValue
		} else {
			value, err := windowExpr.Args[0].EvaluateSelect(partition[leadIndex].row)
			if err != nil {
				return nil, fmt.Errorf("LEAD: failed to evaluate expression: %w", err)
			}
			results[i] = value
		}
	}

	return results, nil
}

// rowsEqualOnOrderBy checks if two rows are equal on all ORDER BY columns
func rowsEqualOnOrderBy(row1, row2 map[string]interface{}, orderBy []OrderByItem) bool {
	for _, item := range orderBy {
		val1 := row1[item.Column]
		val2 := row2[item.Column]

		if compareValues(val1, val2) != 0 {
			return false
		}
	}
	return true
}
