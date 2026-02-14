package query

import (
	"fmt"

	"github.com/vegasq/parcat/reader"
)

// ExecutionContext holds the context for query execution
type ExecutionContext struct {
	// CTEs maps CTE names to their materialized results
	CTEs map[string][]map[string]interface{}
	// Reader for reading parquet files
	Reader *reader.Reader
	// InProgress tracks CTEs currently being materialized (for circular dependency detection)
	InProgress map[string]bool
	// AllCTENames tracks all CTE names defined in the query (for forward reference detection)
	AllCTENames map[string]bool
	// ScalarSubqueryCache caches results of non-correlated scalar subqueries to avoid re-execution
	ScalarSubqueryCache map[*ScalarSubqueryExpr]interface{}
}

// NewExecutionContext creates a new execution context
func NewExecutionContext(r *reader.Reader) *ExecutionContext {
	return &ExecutionContext{
		CTEs:                make(map[string][]map[string]interface{}),
		Reader:              r,
		InProgress:          make(map[string]bool),
		AllCTENames:         make(map[string]bool),
		ScalarSubqueryCache: make(map[*ScalarSubqueryExpr]interface{}),
	}
}

// NewChildContext creates a child context for nested queries with isolated CTE scope
// but inheriting access to parent CTEs
func (ctx *ExecutionContext) NewChildContext() *ExecutionContext {
	child := &ExecutionContext{
		CTEs:                make(map[string][]map[string]interface{}),
		Reader:              ctx.Reader,
		InProgress:          make(map[string]bool),
		AllCTENames:         make(map[string]bool),
		ScalarSubqueryCache: make(map[*ScalarSubqueryExpr]interface{}),
	}
	// Copy parent CTEs to make them accessible in child scope
	for name, rows := range ctx.CTEs {
		child.CTEs[name] = rows
	}
	// Copy parent AllCTENames to enable forward-reference detection in child scope
	for name := range ctx.AllCTENames {
		child.AllCTENames[name] = true
	}
	// Note: We don't copy ScalarSubqueryCache to child - each subquery context
	// should have its own cache since subquery results may differ in different contexts
	return child
}

// ExecuteQuery executes a query with CTE support
func ExecuteQuery(q *Query, r *reader.Reader) ([]map[string]interface{}, error) {
	ctx := NewExecutionContext(r)

	// Materialize CTEs first
	if len(q.CTEs) > 0 {
		if err := ctx.materializeCTEs(q.CTEs); err != nil {
			return nil, fmt.Errorf("failed to materialize CTEs: %w", err)
		}
	}

	// Execute the main query
	return ctx.executeSelect(q)
}

// materializeCTEs evaluates and materializes all CTEs
func (ctx *ExecutionContext) materializeCTEs(ctes []CTE) error {
	return ctx.MaterializeCTEs(ctes, func(q *Query, c *ExecutionContext) ([]map[string]interface{}, error) {
		return ctx.executeSelect(q)
	})
}

// MaterializeCTEs evaluates and materializes all CTEs with circular dependency detection
// The executeFn parameter allows callers to provide custom execution logic
func (ctx *ExecutionContext) MaterializeCTEs(ctes []CTE, executeFn func(*Query, *ExecutionContext) ([]map[string]interface{}, error)) error {
	// Track which CTEs are being defined in this batch to detect duplicates
	localCTENames := make(map[string]bool)

	// Build map of all CTE names for forward reference detection
	for _, cte := range ctes {
		// Check for duplicate in current batch (same WITH clause)
		if localCTENames[cte.Name] {
			return fmt.Errorf("duplicate CTE name in same WITH clause: %s", cte.Name)
		}
		localCTENames[cte.Name] = true
		ctx.AllCTENames[cte.Name] = true
	}

	materialize := func(name string, query *Query) error {
		// Check for cycle using the context's InProgress map
		if ctx.InProgress[name] {
			return fmt.Errorf("circular CTE dependency detected: %s", name)
		}

		// Mark as in progress before execution
		ctx.InProgress[name] = true
		defer func() { delete(ctx.InProgress, name) }()

		// Execute the CTE query using the provided executor function
		rows, err := executeFn(query, ctx)
		if err != nil {
			return fmt.Errorf("failed to execute CTE %s: %w", name, err)
		}

		// Store the materialized result (may shadow parent CTE - this is standard SQL behavior)
		ctx.CTEs[name] = rows
		return nil
	}

	for _, cte := range ctes {
		if err := materialize(cte.Name, cte.Query); err != nil {
			return err
		}
	}

	return nil
}

// executeSelect executes a SELECT query
func (ctx *ExecutionContext) executeSelect(q *Query) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}
	var err error

	// Read data from source (table, CTE, or subquery)
	if q.Subquery != nil {
		// FROM subquery - use child context if subquery has CTEs to prevent scope leaking
		var subqueryCtx *ExecutionContext
		if len(q.Subquery.CTEs) > 0 {
			subqueryCtx = ctx.NewChildContext()
			if err := subqueryCtx.materializeCTEs(q.Subquery.CTEs); err != nil {
				return nil, fmt.Errorf("failed to materialize CTEs in subquery: %w", err)
			}
		} else {
			subqueryCtx = ctx
		}
		rows, err = subqueryCtx.executeSelect(q.Subquery)
		if err != nil {
			return nil, fmt.Errorf("failed to execute FROM subquery: %w", err)
		}
	} else if q.TableName != "" {
		// Check if it's a CTE reference
		if cteRows, exists := ctx.CTEs[q.TableName]; exists {
			rows = cteRows
		} else if ctx.AllCTENames[q.TableName] {
			// This is a forward CTE reference (CTE defined but not yet materialized)
			return nil, fmt.Errorf("forward CTE reference: %s is defined but not yet materialized (CTEs must be referenced in order)", q.TableName)
		} else {
			// Read from parquet file
			rows, err = reader.ReadMultipleFiles(q.TableName)
			if err != nil {
				return nil, fmt.Errorf("failed to read table %s: %w", q.TableName, err)
			}
		}
	} else {
		return nil, fmt.Errorf("no data source specified (table, CTE, or subquery)")
	}

	// Apply table alias to main table rows if specified
	if q.TableAlias != "" {
		rows = applyTableAlias(rows, q.TableAlias)
	}

	// Execute JOINs if present
	if len(q.Joins) > 0 {
		for _, join := range q.Joins {
			rows, err = ctx.executeJoin(rows, q.TableAlias, join)
			if err != nil {
				return nil, fmt.Errorf("failed to execute JOIN: %w", err)
			}
		}
	}

	// Apply WHERE filter
	if q.Filter != nil {
		// Check if filter contains subqueries and evaluate them
		rows, err = ctx.applyFilterWithSubqueries(rows, q.Filter)
		if err != nil {
			return nil, fmt.Errorf("failed to apply filter: %w", err)
		}
	}

	// Apply window functions if present (before aggregation and projection)
	hasWindowFunc := HasWindowFunction(q.SelectList)
	if hasWindowFunc {
		rows, err = ApplyWindowFunctions(rows, q.SelectList)
		if err != nil {
			return nil, fmt.Errorf("failed to apply window functions: %w", err)
		}
		// After window functions, we need final projection but must not re-evaluate window exprs
		// ApplyWindowFunctions already added window results as columns
		// Now project to final SELECT list, treating window exprs as column references
		rows, err = ApplySelectListAfterWindows(rows, q.SelectList)
		if err != nil {
			return nil, fmt.Errorf("failed to apply select list after windows: %w", err)
		}
	} else if len(q.GroupBy) > 0 || HasAggregateFunction(q.SelectList) {
		// Apply GROUP BY and aggregation if present (BEFORE projection)
		rows, err = ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
		if err != nil {
			return nil, fmt.Errorf("failed to apply aggregation: %w", err)
		}

		// Apply HAVING filter if present
		if q.Having != nil {
			rows, err = EvaluateHaving(rows, q.Having)
			if err != nil {
				return nil, fmt.Errorf("failed to apply HAVING clause: %w", err)
			}
		}
	} else {
		// Apply SELECT list projection (only if no aggregation or windows) with context for scalar subquery support
		if len(q.SelectList) > 0 {
			rows, err = ApplySelectListWithContext(rows, q.SelectList, ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to apply select list: %w", err)
			}
		}
	}

	// Apply DISTINCT if present
	if q.Distinct {
		rows, err = ApplyDistinct(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to apply DISTINCT: %w", err)
		}
	}

	// Apply ORDER BY if present
	if len(q.OrderBy) > 0 {
		rows, err = ApplyOrderBy(rows, q.OrderBy)
		if err != nil {
			return nil, fmt.Errorf("failed to apply ORDER BY: %w", err)
		}
	}

	// Apply LIMIT/OFFSET if present
	if q.Limit != nil || q.Offset != nil {
		rows, err = ApplyLimitOffset(rows, q.Limit, q.Offset)
		if err != nil {
			return nil, fmt.Errorf("failed to apply LIMIT/OFFSET: %w", err)
		}
	}

	return rows, nil
}

// applyFilterWithSubqueries applies a filter expression with subquery support
func (ctx *ExecutionContext) applyFilterWithSubqueries(rows []map[string]interface{}, filter Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, row := range rows {
		match, err := ctx.EvaluateExpression(row, filter)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, row)
		}
	}

	return result, nil
}

// EvaluateExpression evaluates an expression with subquery support
func (ctx *ExecutionContext) EvaluateExpression(row map[string]interface{}, expr Expression) (bool, error) {
	switch e := expr.(type) {
	case *ExistsExpr:
		return ctx.evaluateExists(row, e)
	case *InSubqueryExpr:
		return ctx.evaluateInSubquery(row, e)
	case *BinaryExpr:
		// Recursively evaluate both sides with context to support nested subqueries
		left, err := ctx.EvaluateExpression(row, e.Left)
		if err != nil {
			return false, err
		}
		right, err := ctx.EvaluateExpression(row, e.Right)
		if err != nil {
			return false, err
		}
		switch e.Operator {
		case TokenAnd:
			return left && right, nil
		case TokenOr:
			return left || right, nil
		default:
			return false, fmt.Errorf("unsupported binary operator: %v", e.Operator)
		}
	default:
		// Use the standard Evaluate method for non-subquery expressions
		return expr.Evaluate(row)
	}
}

// evaluateExists evaluates an EXISTS expression
// NOTE: Correlated subqueries (subqueries that reference columns from outer query)
// are not currently supported. The subquery is executed independently without
// access to the outer row context.
func (ctx *ExecutionContext) evaluateExists(row map[string]interface{}, expr *ExistsExpr) (bool, error) {
	// Materialize subquery-local CTEs first if present
	var subqueryCtx *ExecutionContext
	if len(expr.Subquery.CTEs) > 0 {
		subqueryCtx = ctx.NewChildContext()
		if err := subqueryCtx.materializeCTEs(expr.Subquery.CTEs); err != nil {
			return false, fmt.Errorf("EXISTS subquery CTE materialization failed: %w", err)
		}
	} else {
		subqueryCtx = ctx
	}

	// Execute the subquery
	// TODO: Support correlated subqueries by passing outer row context
	rows, err := subqueryCtx.executeSelect(expr.Subquery)
	if err != nil {
		return false, fmt.Errorf("EXISTS subquery failed: %w", err)
	}

	// EXISTS is true if subquery returns any rows
	exists := len(rows) > 0

	// Apply negation if needed
	if expr.Negate {
		return !exists, nil
	}
	return exists, nil
}

// evaluateInSubquery evaluates an IN subquery expression
// NOTE: Correlated subqueries (subqueries that reference columns from outer query)
// are not currently supported. The subquery is executed independently without
// access to the outer row context.
func (ctx *ExecutionContext) evaluateInSubquery(row map[string]interface{}, expr *InSubqueryExpr) (bool, error) {
	// Get the column value
	value, exists := row[expr.Column]
	if !exists {
		return false, nil
	}

	// Materialize subquery-local CTEs first if present
	var subqueryCtx *ExecutionContext
	if len(expr.Subquery.CTEs) > 0 {
		subqueryCtx = ctx.NewChildContext()
		if err := subqueryCtx.materializeCTEs(expr.Subquery.CTEs); err != nil {
			return false, fmt.Errorf("IN subquery CTE materialization failed: %w", err)
		}
	} else {
		subqueryCtx = ctx
	}

	// Execute the subquery
	// TODO: Support correlated subqueries by passing outer row context
	rows, err := subqueryCtx.executeSelect(expr.Subquery)
	if err != nil {
		return false, fmt.Errorf("IN subquery failed: %w", err)
	}

	// Check if the subquery returns exactly one column
	if len(rows) > 0 {
		firstRow := rows[0]
		if len(firstRow) != 1 {
			return false, fmt.Errorf("IN subquery must return exactly one column, got %d", len(firstRow))
		}
	}

	// Check if value is in the subquery results
	found := false
	for _, subRow := range rows {
		// Get the single column value from the subquery result
		var subValue interface{}
		for _, v := range subRow {
			subValue = v
			break
		}

		// Compare values
		match, err := compare(value, TokenEqual, subValue)
		if err != nil {
			return false, err
		}
		if match {
			found = true
			break
		}
	}

	// Apply negation if needed
	if expr.Negate {
		return !found, nil
	}
	return found, nil
}

// EvaluateSelectExpression evaluates any SelectExpression with context support for nested subqueries
func (ctx *ExecutionContext) EvaluateSelectExpression(row map[string]interface{}, expr SelectExpression) (interface{}, error) {
	switch e := expr.(type) {
	case *ScalarSubqueryExpr:
		return ctx.EvaluateScalarSubquery(row, e)
	case *FunctionCall:
		// Look up the function in the registry
		registry := GetGlobalRegistry()
		fn, exists := registry.Get(e.Name)
		if !exists {
			return nil, fmt.Errorf("unknown function: %s", e.Name)
		}

		// Evaluate function arguments with context support
		args := make([]interface{}, len(e.Args))
		for i, arg := range e.Args {
			val, err := ctx.EvaluateSelectExpression(row, arg)
			if err != nil {
				return nil, fmt.Errorf("function %s: argument %d: %w", e.Name, i+1, err)
			}
			args[i] = val
		}

		// Check arity
		minArity := fn.MinArity()
		maxArity := fn.MaxArity()
		argCount := len(args)

		if minArity >= 0 && argCount < minArity {
			return nil, fmt.Errorf("function %s: expected at least %d arguments, got %d", e.Name, minArity, argCount)
		}
		if maxArity >= 0 && argCount > maxArity {
			return nil, fmt.Errorf("function %s: expected at most %d arguments, got %d", e.Name, maxArity, argCount)
		}

		// Call the function
		return fn.Evaluate(args)
	case *CaseExpr:
		// Evaluate WHEN clauses
		for _, whenClause := range e.WhenClauses {
			conditionMet, err := ctx.EvaluateExpression(row, whenClause.Condition)
			if err != nil {
				return nil, err
			}
			if conditionMet {
				result, err := ctx.EvaluateSelectExpression(row, whenClause.Result)
				if err != nil {
					return nil, err
				}
				return result, nil
			}
		}
		// Evaluate ELSE clause
		if e.ElseExpr != nil {
			result, err := ctx.EvaluateSelectExpression(row, e.ElseExpr)
			if err != nil {
				return nil, err
			}
			return result, nil
		}
		return nil, nil
	default:
		// For all other expressions, use the standard EvaluateSelect method
		return expr.EvaluateSelect(row)
	}
}

// EvaluateScalarSubquery evaluates a scalar subquery and returns its result
// NOTE: Correlated subqueries (subqueries that reference columns from outer query)
// are not currently supported. The subquery is executed independently without
// access to the outer row context.
// For non-correlated scalar subqueries, the result is cached to avoid re-execution per row.
func (ctx *ExecutionContext) EvaluateScalarSubquery(row map[string]interface{}, expr *ScalarSubqueryExpr) (interface{}, error) {
	// Check cache first for non-correlated subqueries
	// TODO: When we add correlated subquery support, we'll need to skip cache for those
	if cachedValue, exists := ctx.ScalarSubqueryCache[expr]; exists {
		return cachedValue, nil
	}

	// Materialize subquery-local CTEs first if present
	var subqueryCtx *ExecutionContext
	if len(expr.Query.CTEs) > 0 {
		subqueryCtx = ctx.NewChildContext()
		if err := subqueryCtx.materializeCTEs(expr.Query.CTEs); err != nil {
			return nil, fmt.Errorf("scalar subquery CTE materialization failed: %w", err)
		}
	} else {
		subqueryCtx = ctx
	}

	// Execute the subquery
	// TODO: Support correlated subqueries by passing outer row context
	rows, err := subqueryCtx.executeSelect(expr.Query)
	if err != nil {
		return nil, fmt.Errorf("scalar subquery failed: %w", err)
	}

	// Scalar subquery must return exactly one row and one column
	if len(rows) == 0 {
		// Cache NULL result
		ctx.ScalarSubqueryCache[expr] = nil
		return nil, nil // Return NULL if subquery returns no rows
	}

	if len(rows) > 1 {
		return nil, fmt.Errorf("scalar subquery returned more than one row")
	}

	row0 := rows[0]
	if len(row0) != 1 {
		return nil, fmt.Errorf("scalar subquery must return exactly one column, got %d", len(row0))
	}

	// Get and cache the single value
	var result interface{}
	for _, v := range row0 {
		result = v
		break
	}

	// Cache the result for subsequent rows
	ctx.ScalarSubqueryCache[expr] = result
	return result, nil
}

// executeJoin executes a JOIN operation
func (ctx *ExecutionContext) executeJoin(leftRows []map[string]interface{}, leftAlias string, join Join) ([]map[string]interface{}, error) {
	// Get right-side data
	var rightRows []map[string]interface{}
	var err error

	if join.Subquery != nil {
		// JOIN with subquery - use child context if subquery has CTEs to prevent scope leaking
		var subqueryCtx *ExecutionContext
		if len(join.Subquery.CTEs) > 0 {
			subqueryCtx = ctx.NewChildContext()
			if err := subqueryCtx.materializeCTEs(join.Subquery.CTEs); err != nil {
				return nil, fmt.Errorf("failed to materialize CTEs in JOIN subquery: %w", err)
			}
		} else {
			subqueryCtx = ctx
		}
		rightRows, err = subqueryCtx.executeSelect(join.Subquery)
		if err != nil {
			return nil, fmt.Errorf("failed to execute JOIN subquery: %w", err)
		}
	} else if join.TableName != "" {
		// Check if it's a CTE reference
		if cteRows, exists := ctx.CTEs[join.TableName]; exists {
			rightRows = cteRows
		} else if ctx.AllCTENames[join.TableName] {
			// This is a forward CTE reference (CTE defined but not yet materialized)
			return nil, fmt.Errorf("forward CTE reference in JOIN: %s is defined but not yet materialized (CTEs must be referenced in order)", join.TableName)
		} else {
			// Read from parquet file
			rightRows, err = reader.ReadMultipleFiles(join.TableName)
			if err != nil {
				return nil, fmt.Errorf("failed to read JOIN table %s: %w", join.TableName, err)
			}
		}
	} else {
		return nil, fmt.Errorf("JOIN requires table name or subquery")
	}

	// Apply alias to right table rows if specified
	if join.Alias != "" {
		rightRows = applyTableAlias(rightRows, join.Alias)
	}

	// Execute the appropriate join algorithm
	switch join.Type {
	case JoinInner:
		return executeInnerJoin(leftRows, rightRows, join.Condition)
	case JoinLeft:
		return executeLeftJoin(leftRows, rightRows, join.Condition)
	case JoinRight:
		return executeRightJoin(leftRows, rightRows, join.Condition)
	case JoinFull:
		return executeFullJoin(leftRows, rightRows, join.Condition)
	case JoinCross:
		return executeCrossJoin(leftRows, rightRows)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", join.Type)
	}
}

// applyTableAlias prefixes all column names with table alias
func applyTableAlias(rows []map[string]interface{}, alias string) []map[string]interface{} {
	if alias == "" {
		return rows
	}

	aliasedRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		aliasedRow := make(map[string]interface{})
		for col, val := range row {
			// Don't alias the special _file column
			if col == "_file" {
				aliasedRow[col] = val
			} else {
				aliasedRow[alias+"."+col] = val
			}
		}
		aliasedRows[i] = aliasedRow
	}
	return aliasedRows
}

// executeInnerJoin performs an INNER JOIN using hash join algorithm
func executeInnerJoin(leftRows, rightRows []map[string]interface{}, condition Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	// Use nested loop join for simplicity (can be optimized to hash join for equi-joins)
	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Merge rows
			merged, err := mergeRows(leftRow, rightRow)
			if err != nil {
				return nil, err
			}

			// Evaluate join condition
			match, err := condition.Evaluate(merged)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate JOIN condition: %w", err)
			}

			if match {
				result = append(result, merged)
			}
		}
	}

	return result, nil
}

// executeLeftJoin performs a LEFT OUTER JOIN
func executeLeftJoin(leftRows, rightRows []map[string]interface{}, condition Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	// Special case: if right side is empty, return all left rows unchanged
	// (we can't add NULL columns without knowing the right side schema)
	if len(rightRows) == 0 {
		return append([]map[string]interface{}{}, leftRows...), nil
	}

	for _, leftRow := range leftRows {
		matched := false

		for _, rightRow := range rightRows {
			// Merge rows
			merged, err := mergeRows(leftRow, rightRow)
			if err != nil {
				return nil, err
			}

			// Evaluate join condition
			match, err := condition.Evaluate(merged)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate JOIN condition: %w", err)
			}

			if match {
				result = append(result, merged)
				matched = true
			}
		}

		// If no match, include left row with NULL values for right columns
		if !matched {
			merged, err := mergeRows(leftRow, createNullRow(rightRows))
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeRightJoin performs a RIGHT OUTER JOIN
func executeRightJoin(leftRows, rightRows []map[string]interface{}, condition Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	// Special case: if left side is empty, return all right rows unchanged
	// (we can't add NULL columns without knowing the left side schema)
	if len(leftRows) == 0 {
		return append([]map[string]interface{}{}, rightRows...), nil
	}

	for _, rightRow := range rightRows {
		matched := false

		for _, leftRow := range leftRows {
			// Merge rows
			merged, err := mergeRows(leftRow, rightRow)
			if err != nil {
				return nil, err
			}

			// Evaluate join condition
			match, err := condition.Evaluate(merged)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate JOIN condition: %w", err)
			}

			if match {
				result = append(result, merged)
				matched = true
			}
		}

		// If no match, include right row with NULL values for left columns
		if !matched {
			merged, err := mergeRows(createNullRow(leftRows), rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeFullJoin performs a FULL OUTER JOIN
func executeFullJoin(leftRows, rightRows []map[string]interface{}, condition Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	// Special cases: if one side is empty, return the other side unchanged
	if len(leftRows) == 0 {
		return append([]map[string]interface{}{}, rightRows...), nil
	}
	if len(rightRows) == 0 {
		return append([]map[string]interface{}{}, leftRows...), nil
	}

	// Track which right rows have been matched
	rightMatched := make([]bool, len(rightRows))

	// Process left rows
	for _, leftRow := range leftRows {
		matched := false

		for i, rightRow := range rightRows {
			// Merge rows
			merged, err := mergeRows(leftRow, rightRow)
			if err != nil {
				return nil, err
			}

			// Evaluate join condition
			match, err := condition.Evaluate(merged)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate JOIN condition: %w", err)
			}

			if match {
				result = append(result, merged)
				matched = true
				rightMatched[i] = true
			}
		}

		// If no match, include left row with NULL values for right columns
		if !matched {
			merged, err := mergeRows(leftRow, createNullRow(rightRows))
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	// Add unmatched right rows with NULL values for left columns
	for i, rightRow := range rightRows {
		if !rightMatched[i] {
			merged, err := mergeRows(createNullRow(leftRows), rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeCrossJoin performs a CROSS JOIN (Cartesian product)
func executeCrossJoin(leftRows, rightRows []map[string]interface{}) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged, err := mergeRows(leftRow, rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// mergeRows combines two rows into one
// If both left and right have the same column name, returns an error
func mergeRows(left, right map[string]interface{}) (map[string]interface{}, error) {
	merged := make(map[string]interface{})

	// Copy left row
	for k, v := range left {
		merged[k] = v
	}

	// Copy right row - check for collisions (except _file which is allowed to be duplicated)
	for k, v := range right {
		if _, exists := merged[k]; exists {
			// Allow _file column to be duplicated - it's added by glob reads
			// When both sides have _file, we keep both but suffix them with the table position
			if k == "_file" {
				// Keep left as _file_left and right as _file_right
				if leftFile, ok := merged["_file"]; ok {
					delete(merged, "_file")
					merged["_file_left"] = leftFile
					merged["_file_right"] = v
				}
				continue
			}
			return nil, fmt.Errorf("column name collision in JOIN: %q exists in both tables. Use table aliases to disambiguate (e.g., SELECT t1.%s, t2.%s FROM ...)", k, k, k)
		}
		merged[k] = v
	}

	return merged, nil
}

// createNullRow creates a row with NULL values for all columns from a sample row set
func createNullRow(rows []map[string]interface{}) map[string]interface{} {
	if len(rows) == 0 {
		return make(map[string]interface{})
	}

	nullRow := make(map[string]interface{})
	// Use first row as template to get column names
	for col := range rows[0] {
		nullRow[col] = nil
	}

	return nullRow
}
