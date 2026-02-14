// Package query provides SQL query parsing and execution for parquet data.
//
// This package implements a SQL-like query language with support for:
//   - SELECT with column projection and aliases
//   - WHERE clauses with complex conditions
//   - JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
//   - GROUP BY and HAVING for aggregations
//   - ORDER BY for sorting results
//   - LIMIT and OFFSET for pagination
//   - Common Table Expressions (CTEs with WITH clause)
//   - Subqueries (IN, EXISTS, scalar)
//   - Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
//   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//   - Built-in functions (string and math operations)
//   - Multi-file queries with glob patterns
//
// # Basic Usage
//
// Parse and execute a simple query:
//
//	query, err := query.Parse("SELECT name, age FROM data.parquet WHERE age > 30")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	reader, err := reader.NewReader("data.parquet")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
//
//	results, err := query.ExecuteQuery(query, reader)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Filter Operations
//
// Apply filters to existing row data:
//
//	rows := []map[string]interface{}{
//	    {"name": "alice", "age": 30},
//	    {"name": "bob", "age": 25},
//	}
//
//	expr := &query.ComparisonExpr{
//	    Column:   "age",
//	    Operator: query.TokenGreater,
//	    Value:    28,
//	}
//
//	filtered, err := query.ApplyFilter(rows, expr)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Aggregation and GROUP BY
//
// Execute queries with aggregation:
//
//	sql := `
//	    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
//	    FROM employees.parquet
//	    GROUP BY department
//	    HAVING avg_salary > 50000
//	`
//
//	query, err := query.Parse(sql)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Window Functions
//
// Use window functions for advanced analytics:
//
//	sql := `
//	    SELECT name, department, salary,
//	           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
//	    FROM employees.parquet
//	`
//
// # JOIN Operations
//
// Combine data from multiple files:
//
//	sql := `
//	    SELECT u.name, o.amount
//	    FROM users.parquet u
//	    JOIN orders.parquet o ON u.id = o.user_id
//	    WHERE o.amount > 100
//	`
//
// # Multi-file Queries
//
// Query multiple files using glob patterns:
//
//	sql := `
//	    SELECT date, COUNT(*) as count
//	    FROM 'logs/*.parquet'
//	    GROUP BY date
//	`
//
// # Common Table Expressions (CTEs)
//
// Use CTEs for complex queries:
//
//	sql := `
//	    WITH active_users AS (
//	        SELECT * FROM users.parquet WHERE active = true
//	    )
//	    SELECT status, COUNT(*) FROM active_users GROUP BY status
//	`
//
// # Supported Operators
//
// WHERE clause operators:
//   - Comparison: =, !=, <, >, <=, >=
//   - Logical: AND, OR
//   - Special: IN, LIKE, BETWEEN, IS NULL, IS NOT NULL
//   - Subquery: IN (subquery), EXISTS (subquery)
//
// # Built-in Functions
//
// String functions:
//   - UPPER(str), LOWER(str), TRIM(str)
//   - CONCAT(str1, str2, ...), LENGTH(str)
//
// Math functions:
//   - ABS(num), ROUND(num, decimals), FLOOR(num), CEIL(num)
//   - MOD(dividend, divisor)
//
// # Type System
//
// The query engine automatically handles type coercion for comparisons:
//   - String comparisons are case-sensitive
//   - Numeric values are converted to float64 for comparison
//   - Boolean values use direct equality
//   - Type mismatches return false
//
// # Performance Considerations
//
//   - Filters are applied during row reading when possible
//   - Aggregations load all data into memory
//   - Window functions require sorting and partitioning
//   - JOINs may require loading multiple files
//   - Use LIMIT to restrict result set size
//
// # Error Handling
//
// The package returns descriptive errors for:
//   - Syntax errors during parsing
//   - Invalid column references
//   - Type mismatches in comparisons
//   - Unsupported operations
//   - File access errors
package query
