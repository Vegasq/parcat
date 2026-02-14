package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vegasq/parcat/output"
	"github.com/vegasq/parcat/query"
	"github.com/vegasq/parcat/reader"
)

var (
	queryFlag  = flag.String("q", "", "SQL query (e.g., \"select * from file.parquet where age > 30\")")
	formatFlag = flag.String("f", "jsonl", "Output format: json, jsonl, csv")
	limitFlag  = flag.Int("limit", 0, "Limit number of rows (0 = unlimited)")
	schemaFlag = flag.Bool("schema", false, "Show schema information instead of data")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <file.parquet>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "A tool to read and query Parquet files.\n\n")
		fmt.Fprintf(os.Stderr, "IMPORTANT: All flags must come BEFORE file arguments.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f csv data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -q \"select * from data.parquet where age > 30\" data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --schema data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f csv --schema data.parquet\n", os.Args[0])
	}

	flag.Parse()

	// Validate flag values
	if *limitFlag < 0 {
		fmt.Fprintf(os.Stderr, "Error: -limit must be non-negative, got %d\n", *limitFlag)
		os.Exit(1)
	}

	// Validate flag combinations
	if *schemaFlag && *queryFlag != "" {
		fmt.Fprintf(os.Stderr, "Error: --schema and -q cannot be used together\n")
		os.Exit(1)
	}

	// Get filename from positional args (optional if query has FROM clause)
	var filename string
	if flag.NArg() >= 1 {
		filename = flag.Arg(0)
	}

	// Handle schema mode
	if *schemaFlag {
		if filename == "" {
			fmt.Fprintf(os.Stderr, "Error: missing parquet file argument\n\n")
			flag.Usage()
			os.Exit(1)
		}
		handleSchemaMode(filename, *formatFlag)
		os.Exit(0)
	}

	// Parse query if specified to determine if we need a filename
	var q *query.Query
	if *queryFlag != "" {
		var err error
		q, err = query.Parse(*queryFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing query: %v\n\n", err)
			fmt.Fprintf(os.Stderr, "Query format: select * from file.parquet where <condition>\n")
			fmt.Fprintf(os.Stderr, "Example: select * from data.parquet where age > 30\n")
			os.Exit(1)
		}

		// If query specifies table name, use it instead of positional arg
		if q.TableName != "" && filename == "" {
			filename = q.TableName
		}
	}

	// Declare rows variable before conditional logic
	var rows []map[string]interface{}
	var err error

	// Materialize CTEs FIRST (before loading main table) as they may be referenced in FROM
	ctx := query.NewExecutionContext(nil)
	if q != nil && len(q.CTEs) > 0 {
		// Use the executor's CTE materialization logic which includes circular dependency detection
		if err := ctx.MaterializeCTEs(q.CTEs, executeCTEQuery); err != nil {
			fmt.Fprintf(os.Stderr, "Error materializing CTEs: %v\n", err)
			os.Exit(1)
		}
	}

	// Handle FROM subquery case
	if q != nil && q.Subquery != nil {
		// Execute the subquery to get initial rows
		rows, err = executeCTEQuery(q.Subquery, ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error executing FROM subquery: %v\n", err)
			os.Exit(1)
		}

		// Apply table alias if specified
		if q.TableAlias != "" {
			rows = applyTableAliasHelper(rows, q.TableAlias)
		}
	} else if q != nil && filename != "" && len(ctx.CTEs) > 0 {
		// Check if main table is a CTE reference
		if cteRows, exists := ctx.CTEs[q.TableName]; exists {
			rows = cteRows
			// Apply table alias if specified
			if q.TableAlias != "" {
				rows = applyTableAliasHelper(rows, q.TableAlias)
			}
		} else {
			// Not a CTE, read from file
			rows, err = reader.ReadMultipleFiles(filename)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Fprintf(os.Stderr, "Error: file '%s' not found\n", filename)
					fmt.Fprintf(os.Stderr, "Please check the file path and try again.\n")
				} else {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				}
				os.Exit(1)
			}
			// Apply table alias if specified
			if q.TableAlias != "" {
				rows = applyTableAliasHelper(rows, q.TableAlias)
			}
		}
	} else {
		// Ensure we have a filename
		if filename == "" {
			fmt.Fprintf(os.Stderr, "Error: missing parquet file argument\n\n")
			flag.Usage()
			os.Exit(1)
		}

		// Read all rows (supports glob patterns)
		rows, err = reader.ReadMultipleFiles(filename)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "Error: file '%s' not found\n", filename)
				fmt.Fprintf(os.Stderr, "Please check the file path and try again.\n")
			} else {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			}
			os.Exit(1)
		}
		// Apply table alias if specified
		if q != nil && q.TableAlias != "" {
			rows = applyTableAliasHelper(rows, q.TableAlias)
		}
	}

	// Apply query if specified
	if q != nil {
		// CTEs already materialized above in ctx
		// Table alias already applied in the paths above

		// Handle JOINs - need to read additional files
		if len(q.Joins) > 0 {

			// Execute JOINs
			for _, join := range q.Joins {
				var joinRows []map[string]interface{}

				if join.Subquery != nil {
					// JOIN with subquery
					joinRows, err = executeCTEQuery(join.Subquery, ctx)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error executing JOIN subquery: %v\n", err)
						os.Exit(1)
					}
				} else if join.TableName != "" {
					// Check if it's a CTE reference
					if cteRows, exists := ctx.CTEs[join.TableName]; exists {
						joinRows = cteRows
					} else if ctx.AllCTENames[join.TableName] {
						// This is a forward CTE reference (CTE defined but not yet materialized)
						fmt.Fprintf(os.Stderr, "Error: forward CTE reference in JOIN: %s is defined but not yet materialized (CTEs must be referenced in order)\n", join.TableName)
						os.Exit(1)
					} else {
						// Read from parquet file (supports glob)
						joinRows, err = reader.ReadMultipleFiles(join.TableName)
						if err != nil {
							fmt.Fprintf(os.Stderr, "Error reading JOIN table %s: %v\n", join.TableName, err)
							os.Exit(1)
						}
					}
				}

				// Apply alias to joined table if specified
				if join.Alias != "" {
					joinRows = applyTableAliasHelper(joinRows, join.Alias)
				}

				// Execute the join
				rows, err = executeJoinHelper(rows, joinRows, join)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error executing JOIN: %v\n", err)
					os.Exit(1)
				}
			}
		}

		// Apply WHERE filter first (with context for subquery support)
		if q.Filter != nil {
			rows, err = query.ApplyFilterWithContext(rows, q.Filter, ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying filter: %v\n", err)
				// List available columns to help user
				if len(rows) > 0 {
					columns := query.GetColumnNames(rows)
					fmt.Fprintf(os.Stderr, "\nAvailable columns: ")
					for i, col := range columns {
						if i > 0 {
							fmt.Fprintf(os.Stderr, ", ")
						}
						fmt.Fprintf(os.Stderr, "%s", col)
					}
					fmt.Fprintf(os.Stderr, "\n")
				}
				os.Exit(1)
			}
		}

		// Apply window functions if present (before aggregation and projection)
		hasWindowFunc := query.HasWindowFunction(q.SelectList)
		if hasWindowFunc {
			rows, err = query.ApplyWindowFunctions(rows, q.SelectList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying window functions: %v\n", err)
				os.Exit(1)
			}
			// After window functions, we need final projection but must not re-evaluate window exprs
			// ApplyWindowFunctions already added window results as columns
			// Now project to final SELECT list, treating window exprs as column references
			rows, err = query.ApplySelectListAfterWindows(rows, q.SelectList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying select list after windows: %v\n", err)
				os.Exit(1)
			}
		} else if len(q.GroupBy) > 0 || query.HasAggregateFunction(q.SelectList) {
			// Apply GROUP BY and aggregation if present
			rows, err = query.ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying aggregation: %v\n", err)
				os.Exit(1)
			}

			// Apply HAVING filter if present
			if q.Having != nil {
				rows, err = query.EvaluateHaving(rows, q.Having)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error applying HAVING clause: %v\n", err)
					os.Exit(1)
				}
			}
		} else {
			// Apply SELECT list projection (only if no aggregation or windows) with context for scalar subquery support
			if len(q.SelectList) > 0 {
				rows, err = query.ApplySelectListWithContext(rows, q.SelectList, ctx)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error applying select list: %v\n", err)
					os.Exit(1)
				}
			}
		}

		// Apply DISTINCT if present
		if q.Distinct {
			rows, err = query.ApplyDistinct(rows)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying DISTINCT: %v\n", err)
				os.Exit(1)
			}
		}

		// Apply ORDER BY if present
		if len(q.OrderBy) > 0 {
			rows, err = query.ApplyOrderBy(rows, q.OrderBy)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying ORDER BY: %v\n", err)
				os.Exit(1)
			}
		}

		// Apply SQL LIMIT and OFFSET if present
		if q.Limit != nil || q.Offset != nil {
			rows, err = query.ApplyLimitOffset(rows, q.Limit, q.Offset)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying LIMIT/OFFSET: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Apply flag-based limit only if SQL LIMIT was not specified
	if *limitFlag > 0 && (q == nil || q.Limit == nil) && len(rows) > *limitFlag {
		rows = rows[:*limitFlag]
	}

	// Format and output
	var formatter output.Formatter
	switch *formatFlag {
	case "json", "jsonl":
		formatter = output.NewJSONFormatter(os.Stdout)
	case "csv":
		formatter = output.NewCSVFormatter(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported format '%s'\n", *formatFlag)
		fmt.Fprintf(os.Stderr, "Supported formats: json, jsonl, csv\n")
		os.Exit(1)
	}

	if err := formatter.Format(rows); err != nil {
		fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
		os.Exit(1)
	}
}

// executeCTEQuery executes a CTE or subquery
func executeCTEQuery(q *query.Query, ctx *query.ExecutionContext) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}
	var err error

	// Materialize any CTEs defined in this subquery FIRST
	// Use a child context to prevent CTE scope leaking to parent
	if len(q.CTEs) > 0 {
		childCtx := ctx.NewChildContext()
		if err := childCtx.MaterializeCTEs(q.CTEs, executeCTEQuery); err != nil {
			return nil, fmt.Errorf("failed to materialize CTEs in subquery: %w", err)
		}
		// Use child context for the rest of this subquery
		ctx = childCtx
	}

	// Handle subqueries in FROM clause
	if q.Subquery != nil {
		rows, err = executeCTEQuery(q.Subquery, ctx)
		if err != nil {
			return nil, err
		}
	} else if q.TableName != "" {
		// Check if this table is currently being materialized (circular dependency)
		if ctx.InProgress[q.TableName] {
			return nil, fmt.Errorf("circular CTE dependency detected: %s references itself", q.TableName)
		}

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
				return nil, err
			}
		}
	} else {
		return nil, fmt.Errorf("no data source specified (table, CTE, or subquery)")
	}

	// Apply table alias to main table rows if specified (BEFORE filtering/joins)
	if q.TableAlias != "" {
		rows = applyTableAliasHelper(rows, q.TableAlias)
	}

	// Handle JOINs if present
	if len(q.Joins) > 0 {
		for _, join := range q.Joins {
			var joinRows []map[string]interface{}
			if join.Subquery != nil {
				joinRows, err = executeCTEQuery(join.Subquery, ctx)
				if err != nil {
					return nil, err
				}
			} else if join.TableName != "" {
				// Check for circular dependency
				if ctx.InProgress[join.TableName] {
					return nil, fmt.Errorf("circular CTE dependency detected: JOIN references %s which is currently being materialized", join.TableName)
				}

				if cteRows, exists := ctx.CTEs[join.TableName]; exists {
					joinRows = cteRows
				} else if ctx.AllCTENames[join.TableName] {
					// This is a forward CTE reference (CTE defined but not yet materialized)
					return nil, fmt.Errorf("forward CTE reference in JOIN: %s is defined but not yet materialized (CTEs must be referenced in order)", join.TableName)
				} else {
					joinRows, err = reader.ReadMultipleFiles(join.TableName)
					if err != nil {
						return nil, err
					}
				}
			}

			if join.Alias != "" {
				joinRows = applyTableAliasHelper(joinRows, join.Alias)
			}

			rows, err = executeJoinHelper(rows, joinRows, join)
			if err != nil {
				return nil, err
			}
		}
	}

	// Apply WHERE filter if present (with context for subquery support)
	if q.Filter != nil {
		rows, err = query.ApplyFilterWithContext(rows, q.Filter, ctx)
		if err != nil {
			return nil, err
		}
	}

	// Apply window functions if present (before aggregation and projection)
	hasWindowFunc := query.HasWindowFunction(q.SelectList)
	if hasWindowFunc {
		rows, err = query.ApplyWindowFunctions(rows, q.SelectList)
		if err != nil {
			return nil, err
		}
		// After window functions, we need final projection but must not re-evaluate window exprs
		// ApplyWindowFunctions already added window results as columns
		// Now project to final SELECT list, treating window exprs as column references
		rows, err = query.ApplySelectListAfterWindows(rows, q.SelectList)
		if err != nil {
			return nil, err
		}
	} else if len(q.GroupBy) > 0 || query.HasAggregateFunction(q.SelectList) {
		// Apply GROUP BY and aggregation if present (BEFORE projection)
		rows, err = query.ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
		if err != nil {
			return nil, err
		}

		// Apply HAVING filter if present
		if q.Having != nil {
			rows, err = query.EvaluateHaving(rows, q.Having)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// Apply SELECT list projection (only if no aggregation or windows) with context for scalar subquery support
		if len(q.SelectList) > 0 {
			rows, err = query.ApplySelectListWithContext(rows, q.SelectList, ctx)
			if err != nil {
				return nil, err
			}
		}
	}

	// Apply DISTINCT if present
	if q.Distinct {
		rows, err = query.ApplyDistinct(rows)
		if err != nil {
			return nil, err
		}
	}

	// Apply ORDER BY if present
	if len(q.OrderBy) > 0 {
		rows, err = query.ApplyOrderBy(rows, q.OrderBy)
		if err != nil {
			return nil, err
		}
	}

	// Apply LIMIT/OFFSET if present
	if q.Limit != nil || q.Offset != nil {
		rows, err = query.ApplyLimitOffset(rows, q.Limit, q.Offset)
		if err != nil {
			return nil, err
		}
	}

	return rows, nil
}

// applyTableAliasHelper prefixes all column names with table alias
func applyTableAliasHelper(rows []map[string]interface{}, alias string) []map[string]interface{} {
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

// executeJoinHelper executes a JOIN operation
func executeJoinHelper(leftRows, rightRows []map[string]interface{}, join query.Join) ([]map[string]interface{}, error) {
	switch join.Type {
	case query.JoinInner:
		return executeInnerJoinHelper(leftRows, rightRows, join.Condition)
	case query.JoinLeft:
		return executeLeftJoinHelper(leftRows, rightRows, join.Condition)
	case query.JoinRight:
		return executeRightJoinHelper(leftRows, rightRows, join.Condition)
	case query.JoinFull:
		return executeFullJoinHelper(leftRows, rightRows, join.Condition)
	case query.JoinCross:
		return executeCrossJoinHelper(leftRows, rightRows)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", join.Type)
	}
}

// executeInnerJoinHelper performs an INNER JOIN
func executeInnerJoinHelper(leftRows, rightRows []map[string]interface{}, condition query.Expression) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Merge rows
			merged, err := mergeRowsHelper(leftRow, rightRow)
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

// executeLeftJoinHelper performs a LEFT OUTER JOIN
func executeLeftJoinHelper(leftRows, rightRows []map[string]interface{}, condition query.Expression) ([]map[string]interface{}, error) {
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
			merged, err := mergeRowsHelper(leftRow, rightRow)
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
			merged, err := mergeRowsHelper(leftRow, createNullRowHelper(rightRows))
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeRightJoinHelper performs a RIGHT OUTER JOIN
func executeRightJoinHelper(leftRows, rightRows []map[string]interface{}, condition query.Expression) ([]map[string]interface{}, error) {
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
			merged, err := mergeRowsHelper(leftRow, rightRow)
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
			merged, err := mergeRowsHelper(createNullRowHelper(leftRows), rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeFullJoinHelper performs a FULL OUTER JOIN
func executeFullJoinHelper(leftRows, rightRows []map[string]interface{}, condition query.Expression) ([]map[string]interface{}, error) {
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
			merged, err := mergeRowsHelper(leftRow, rightRow)
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
			merged, err := mergeRowsHelper(leftRow, createNullRowHelper(rightRows))
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	// Add unmatched right rows with NULL values for left columns
	for i, rightRow := range rightRows {
		if !rightMatched[i] {
			merged, err := mergeRowsHelper(createNullRowHelper(leftRows), rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// executeCrossJoinHelper performs a CROSS JOIN (Cartesian product)
func executeCrossJoinHelper(leftRows, rightRows []map[string]interface{}) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			merged, err := mergeRowsHelper(leftRow, rightRow)
			if err != nil {
				return nil, err
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// mergeRowsHelper combines two rows into one
// If both left and right have the same column name, returns an error
func mergeRowsHelper(left, right map[string]interface{}) (map[string]interface{}, error) {
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

// createNullRowHelper creates a row with NULL values for all columns from a sample row set.
// For empty row sets, we need to infer columns from the schema or accept that we cannot
// create proper NULL rows. In practice, this means OUTER JOINs with one empty side will
// not include NULL columns for that side - this is a known limitation when joining empty
// result sets without schema information.
func createNullRowHelper(rows []map[string]interface{}) map[string]interface{} {
	if len(rows) == 0 {
		// Cannot create NULL row without knowing column names
		// This is a limitation: outer joins with empty sides won't have NULL columns
		return make(map[string]interface{})
	}

	nullRow := make(map[string]interface{})
	// Use first row as template to get column names
	for col := range rows[0] {
		nullRow[col] = nil
	}

	return nullRow
}

// handleSchemaMode handles the --schema flag by extracting and displaying schema information
func handleSchemaMode(filename string, format string) {
	// Resolve filename - for glob patterns, use first match
	var filePath string

	// Check if pattern contains glob wildcards
	if strings.ContainsAny(filename, "*?[]{}") {
		matches, err := filepath.Glob(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid glob pattern: %v\n", err)
			os.Exit(1)
		}

		if len(matches) == 0 {
			fmt.Fprintf(os.Stderr, "Error: no files match pattern: %s\n", filename)
			os.Exit(1)
		}

		filePath = matches[0]
		// Print informational message to stderr
		if len(matches) > 1 {
			fmt.Fprintf(os.Stderr, "# Showing schema from: %s (%d files matched)\n", filePath, len(matches))
		}
	} else {
		filePath = filename
	}

	// Extract schema information using reader package
	schemaInfos, err := reader.ExtractSchemaInfo(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: file '%s' not found\n", filePath)
			fmt.Fprintf(os.Stderr, "Please check the file path and try again.\n")
		} else {
			fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		}
		os.Exit(1)
	}

	// Convert reader.SchemaInfo to []map[string]interface{} for formatter compatibility
	rows := make([]map[string]interface{}, len(schemaInfos))
	for i, field := range schemaInfos {
		rows[i] = map[string]interface{}{
			"name":          field.Name,
			"type":          field.Type,
			"physical_type": field.PhysicalType,
			"logical_type":  field.LogicalType,
			"required":      field.Required,
			"optional":      field.Optional,
			"repeated":      field.Repeated,
		}
	}

	// Format and output
	var formatter output.Formatter
	switch format {
	case "json", "jsonl":
		formatter = output.NewJSONFormatter(os.Stdout)
	case "csv":
		formatter = output.NewCSVFormatter(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported format '%s'\n", format)
		fmt.Fprintf(os.Stderr, "Supported formats: json, jsonl, csv\n")
		os.Exit(1)
	}

	if err := formatter.Format(rows); err != nil {
		fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
		os.Exit(1)
	}
}
