# parcat

A Go library and CLI tool for reading, querying, and formatting Apache Parquet files with SQL-like query support.

## Features

- 📖 Read and output all rows from Parquet files
- 🔍 SQL-like query support with WHERE clauses
- 📊 Column projection (SELECT specific columns)
- 📈 Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- 👥 GROUP BY and HAVING clauses
- 🪟 Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
- 🔄 Common Table Expressions (CTEs with WITH clause)
- 🎯 Subqueries (IN, EXISTS, scalar subqueries)
- 🔧 Built-in functions (string and math operations)
- 📁 Multi-file queries with glob patterns
- 🔗 JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)
- 🔬 Schema introspection to inspect file structure
- 📋 Multiple output formats (JSON Lines, CSV)
- ⚡ Pure Go implementation with zero external dependencies (except parquet library)
- 🚀 Fast and efficient

## Installation

### As a Library

Add parcat to your Go project:

```bash
go get github.com/vegasq/parcat
```

### As a CLI Tool

Install the parcat command-line tool:

```bash
go install github.com/vegasq/parcat/cmd/parcat@latest
```

Or build from source:

```bash
git clone https://github.com/vegasq/parcat.git
cd parcat
go install ./cmd/parcat
```

## Quick Start

```go
package main

import (
    "log"
    "os"

    "github.com/vegasq/parcat/reader"
    "github.com/vegasq/parcat/output"
)

func main() {
    // Read a parquet file
    r, err := reader.NewReader("data.parquet")
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    rows, err := r.ReadAll()
    if err != nil {
        log.Fatal(err)
    }

    // Format as JSON
    formatter := output.NewJSONFormatter(os.Stdout)
    if err := formatter.Format(rows); err != nil {
        log.Fatal(err)
    }
}
```

## API Documentation

### Package: reader

The reader package provides functionality for reading Apache Parquet files.

#### Reading a Single File

```go
import "github.com/vegasq/parcat/reader"

// Open a parquet file
r, err := reader.NewReader("data.parquet")
if err != nil {
    log.Fatal(err)
}
defer r.Close()

// Read all rows
rows, err := r.ReadAll()
if err != nil {
    log.Fatal(err)
}

// Each row is a map[string]interface{}
for _, row := range rows {
    fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
}
```

#### Reading Multiple Files (Glob Patterns)

```go
// Read all parquet files matching a pattern
rows, err := reader.ReadMultipleFiles("data/*.parquet")
if err != nil {
    log.Fatal(err)
}

// Each row includes "_file" column with source file path
for _, row := range rows {
    fmt.Printf("From %s: %v\n", row["_file"], row)
}
```

#### Schema Introspection

```go
r, err := reader.NewReader("data.parquet")
if err != nil {
    log.Fatal(err)
}
defer r.Close()

schema := r.Schema()
for i := 0; i < schema.NumFields(); i++ {
    field := schema.Field(i)
    fmt.Printf("%s: %s\n", field.Name(), field.Type())
}
```

### Package: output

The output package provides formatters for converting parquet data to various formats.

#### JSON Lines Formatter

```go
import "github.com/vegasq/parcat/output"

formatter := output.NewJSONFormatter(os.Stdout)
if err := formatter.Format(rows); err != nil {
    log.Fatal(err)
}
```

#### CSV Formatter

```go
formatter := output.NewCSVFormatter(os.Stdout)
if err := formatter.Format(rows); err != nil {
    log.Fatal(err)
}
```

#### Writing to String

```go
var buf bytes.Buffer
formatter := output.NewJSONFormatter(&buf)
if err := formatter.Format(rows); err != nil {
    log.Fatal(err)
}
jsonString := buf.String()
```

### Package: query

The query package provides SQL query parsing and execution for parquet data.

#### Basic Query Execution

```go
import "github.com/vegasq/parcat/query"

// Parse SQL query
q, err := query.Parse("SELECT name, age FROM data.parquet WHERE age > 30")
if err != nil {
    log.Fatal(err)
}

// Execute query
r, err := reader.NewReader("data.parquet")
if err != nil {
    log.Fatal(err)
}
defer r.Close()

results, err := query.ExecuteQuery(q, r)
if err != nil {
    log.Fatal(err)
}
```

#### Filtering Rows

```go
// Create a filter expression
expr := &query.ComparisonExpr{
    Column:   "age",
    Operator: query.TokenGreater,
    Value:    30,
}

// Apply filter to rows
filtered, err := query.ApplyFilter(rows, expr)
if err != nil {
    log.Fatal(err)
}
```

## Complete Usage Examples

### Example 1: Read and Filter Data

```go
package main

import (
    "fmt"
    "log"

    "github.com/vegasq/parcat/reader"
    "github.com/vegasq/parcat/query"
)

func main() {
    // Read parquet file
    r, err := reader.NewReader("employees.parquet")
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    // Execute SQL query
    q, err := query.Parse("SELECT name, department, salary FROM employees.parquet WHERE salary > 50000")
    if err != nil {
        log.Fatal(err)
    }

    results, err := query.ExecuteQuery(q, r)
    if err != nil {
        log.Fatal(err)
    }

    // Process results
    for _, row := range results {
        fmt.Printf("%s (%s): $%.2f\n", row["name"], row["department"], row["salary"])
    }
}
```

### Example 2: Aggregate Data with GROUP BY

```go
package main

import (
    "fmt"
    "log"
    "os"

    "github.com/vegasq/parcat/reader"
    "github.com/vegasq/parcat/query"
    "github.com/vegasq/parcat/output"
)

func main() {
    r, err := reader.NewReader("sales.parquet")
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    // Aggregate query
    sql := `
        SELECT region, COUNT(*) as count, SUM(amount) as total
        FROM sales.parquet
        GROUP BY region
        HAVING total > 10000
    `

    q, err := query.Parse(sql)
    if err != nil {
        log.Fatal(err)
    }

    results, err := query.ExecuteQuery(q, r)
    if err != nil {
        log.Fatal(err)
    }

    // Output as CSV
    formatter := output.NewCSVFormatter(os.Stdout)
    if err := formatter.Format(results); err != nil {
        log.Fatal(err)
    }
}
```

### Example 3: Multi-file Query

```go
package main

import (
    "fmt"
    "log"

    "github.com/vegasq/parcat/query"
)

func main() {
    // Query across multiple files
    sql := `
        SELECT date, COUNT(*) as events
        FROM 'logs/*.parquet'
        WHERE severity = 'ERROR'
        GROUP BY date
        ORDER BY date DESC
    `

    q, err := query.Parse(sql)
    if err != nil {
        log.Fatal(err)
    }

    // For multi-file queries, pass nil for reader
    // The query executor will open files based on the pattern
    results, err := query.ExecuteQuery(q, nil)
    if err != nil {
        log.Fatal(err)
    }

    for _, row := range results {
        fmt.Printf("%s: %v errors\n", row["date"], row["events"])
    }
}
```

### Example 4: JOIN Operations

```go
package main

import (
    "log"
    "os"

    "github.com/vegasq/parcat/query"
    "github.com/vegasq/parcat/output"
)

func main() {
    sql := `
        SELECT u.name, u.email, o.order_date, o.total
        FROM users.parquet u
        JOIN orders.parquet o ON u.id = o.user_id
        WHERE o.total > 100
        ORDER BY o.total DESC
    `

    q, err := query.Parse(sql)
    if err != nil {
        log.Fatal(err)
    }

    results, err := query.ExecuteQuery(q, nil)
    if err != nil {
        log.Fatal(err)
    }

    formatter := output.NewJSONFormatter(os.Stdout)
    if err := formatter.Format(results); err != nil {
        log.Fatal(err)
    }
}
```

## CLI Tool Usage

The parcat CLI tool is available for quick command-line operations. After installing with `go install github.com/vegasq/parcat/cmd/parcat@latest`, you can use it directly from your terminal.

### Basic CLI Usage

**Read entire file:**
```bash
parcat data.parquet
```

**View schema:**
```bash
parcat --schema data.parquet
```

**Query with WHERE clause:**
```bash
parcat -q "select * from data.parquet where age > 30"
```

### Output Formats

**JSON Lines (default):**
```bash
parcat data.parquet
parcat -f jsonl data.parquet
```

**CSV:**
```bash
parcat -f csv data.parquet
```

### Schema Introspection

View the schema of a Parquet file without loading data:

```bash
# View schema in JSON format (default)
parcat --schema data.parquet

# View schema in CSV format
parcat --schema -f csv data.parquet

# View schema from glob pattern (uses first match)
parcat --schema 'data/*.parquet'
```

**JSON output example:**
```json
{"name":"id","type":"INT64","physical_type":"INT64","logical_type":"INT(64,true)","required":true,"optional":false,"repeated":false}
{"name":"name","type":"STRING","physical_type":"BYTE_ARRAY","logical_type":"STRING","required":true,"optional":false,"repeated":false}
{"name":"age","type":"INT32","physical_type":"INT32","logical_type":"INT(32,true)","required":true,"optional":false,"repeated":false}
```

**CSV output example:**
```csv
name,type,physical_type,logical_type,required,optional,repeated
id,INT64,INT64,"INT(64,true)",true,false,false
name,STRING,BYTE_ARRAY,STRING,true,false,false
age,INT32,INT32,"INT(32,true)",true,false,false
```

Schema information includes:
- **name**: Column name (uses dot notation for nested fields, e.g., `address.street`)
- **type**: User-friendly type (STRING, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, etc.)
- **physical_type**: Parquet physical type (BYTE_ARRAY, INT32, INT64, etc.)
- **logical_type**: Parquet logical type annotation
- **required**: Whether the field is required (non-null)
- **optional**: Whether the field is optional (nullable)
- **repeated**: Whether the field is an array/list

### Column Projection

Select specific columns instead of all columns:

```bash
# Select specific columns
parcat -q "select name, age from data.parquet"

# Select with aliases
parcat -q "select name as user_name, age as user_age from data.parquet"

# Combine with WHERE clause
parcat -q "select name, age from data.parquet where age > 30"
```

### Query with WHERE Clause

Filter rows using SQL-like WHERE clauses:

```bash
# Simple comparison
parcat -q "select * from data.parquet where age > 30"

# String matching
parcat -q "select * from data.parquet where name = 'alice'"

# AND condition
parcat -q "select * from data.parquet where age > 25 AND status = 'active'"

# OR condition
parcat -q "select * from data.parquet where age > 50 OR premium = true"

# Combine with CSV output
parcat -q "select * from data.parquet where age > 30" -f csv
```

### Using Functions

Transform data using built-in functions:

```bash
# String functions
parcat -q "select UPPER(name) as upper_name from data.parquet"
parcat -q "select LOWER(name), LENGTH(name) from data.parquet"
parcat -q "select CONCAT(first_name, ' ', last_name) as full_name from data.parquet"
parcat -q "select TRIM(name) from data.parquet"

# Math functions
parcat -q "select ABS(temperature) from data.parquet"
parcat -q "select ROUND(price, 2) as rounded_price from data.parquet"
parcat -q "select FLOOR(value), CEIL(value) from data.parquet"
parcat -q "select MOD(count, 10) from data.parquet"

# Combine functions with WHERE
parcat -q "select UPPER(name) from data.parquet where LENGTH(name) > 5"
```

### Aggregations and GROUP BY

Compute aggregate statistics over your data:

```bash
# Count all rows
parcat -q "select COUNT(*) from data.parquet"

# Aggregate functions
parcat -q "select SUM(sales) from data.parquet"
parcat -q "select AVG(age) from data.parquet"
parcat -q "select MIN(price), MAX(price) from data.parquet"

# GROUP BY clause
parcat -q "select status, COUNT(*) from data.parquet group by status"
parcat -q "select department, AVG(salary) from data.parquet group by department"

# Multiple GROUP BY columns
parcat -q "select department, status, COUNT(*) from data.parquet group by department, status"

# HAVING clause (filter after aggregation)
parcat -q "select status, COUNT(*) as total from data.parquet group by status having total > 10"

# Complex aggregation with aliases
parcat -q "select status, COUNT(*) as user_count, AVG(age) as avg_age from data.parquet group by status"
```

### Limit Output

Limit the number of rows returned:

```bash
parcat -limit 10 data.parquet
```

### Multi-File Queries

Query multiple parquet files at once using glob patterns:

```bash
# Read all parquet files in a directory
parcat -q "select * from 'data/*.parquet'"

# Read files matching a specific pattern
parcat -q "select * from 'logs/2024-*.parquet' where status = 'error'"

# Filter and aggregate across multiple files
parcat -q "select date, COUNT(*) as total from 'data/sales-*.parquet' group by date"
```

When reading multiple files, parcat automatically adds a `_file` column to each row indicating the source file:

```bash
parcat -q "select _file, name from 'data/*.parquet'"
```

### JOIN Operations

Combine data from multiple parquet files using JOIN operations:

```bash
# INNER JOIN - returns only matching rows
parcat -q "select u.name, o.amount from users.parquet u join orders.parquet o on u.id = o.user_id"

# LEFT JOIN - returns all rows from left table
parcat -q "select u.name, o.amount from users.parquet u left join orders.parquet o on u.id = o.user_id"

# RIGHT JOIN - returns all rows from right table
parcat -q "select u.name, o.amount from users.parquet u right join orders.parquet o on u.id = o.user_id"

# FULL OUTER JOIN - returns all rows from both tables
parcat -q "select u.name, o.amount from users.parquet u full outer join orders.parquet o on u.id = o.user_id"

# CROSS JOIN - cartesian product
parcat -q "select * from users.parquet cross join categories.parquet"

# Multiple JOINs
parcat -q "select u.name, o.amount, p.name as product from users.parquet u join orders.parquet o on u.id = o.user_id join products.parquet p on o.product_id = p.id"

# JOIN with WHERE clause
parcat -q "select u.name, o.amount from users.parquet u join orders.parquet o on u.id = o.user_id where o.amount > 100"

# JOIN with aggregations
parcat -q "select u.name, sum(o.amount) as total from users.parquet u left join orders.parquet o on u.id = o.user_id group by u.name"

# JOIN with subquery
parcat -q "select u.name, active_orders.total from users.parquet u join (select user_id, count(*) as total from orders.parquet where status = 'active' group by user_id) active_orders on u.id = active_orders.user_id"
```

## Query Syntax

### Basic Query Format

```sql
SELECT <columns> FROM <filename>
[JOIN <filename> ON <condition>]
[WHERE <condition>]
[GROUP BY <columns>]
[HAVING <condition>]
[ORDER BY <columns>]
[LIMIT <n>]
```

### Column Selection

- `*` - Select all columns
- `column1, column2` - Select specific columns
- `column AS alias` - Select column with alias
- `table.column` - Qualified column reference (required in JOINs)
- `FUNCTION(column)` - Apply function to column

### Table References

- `filename.parquet` - Single file
- `'pattern/*.parquet'` - Glob pattern (must be quoted)
- `table AS alias` - Table alias (e.g., `users.parquet u`)
- `(subquery) AS alias` - Subquery as table source

### Supported Operators

- `=` - Equal
- `!=` - Not equal
- `<` - Less than
- `>` - Greater than
- `<=` - Less than or equal
- `>=` - Greater than or equal
- `IN` - Value matches any in a list (e.g., `status IN ('active', 'pending')`)
- `LIKE` - Pattern matching with wildcards (e.g., `name LIKE 'John%'`)
- `BETWEEN` - Range comparison (e.g., `age BETWEEN 18 AND 65`)
- `IS NULL` - Check for null values
- `IS NOT NULL` - Check for non-null values

### Logical Operators

- `AND` - Both conditions must be true
- `OR` - At least one condition must be true

### JOIN Types

- `INNER JOIN` or `JOIN` - Returns only matching rows from both tables
- `LEFT JOIN` or `LEFT OUTER JOIN` - Returns all rows from left table, matching rows from right
- `RIGHT JOIN` or `RIGHT OUTER JOIN` - Returns all rows from right table, matching rows from left
- `FULL JOIN` or `FULL OUTER JOIN` - Returns all rows from both tables
- `CROSS JOIN` - Cartesian product of both tables (no ON clause)

### Built-in Functions

For a complete reference of all 44 built-in functions with detailed examples, see [docs/FUNCTIONS.md](docs/FUNCTIONS.md).

#### Summary

#### String Functions
- `UPPER(str)` - Convert string to uppercase
- `LOWER(str)` - Convert string to lowercase
- `CONCAT(str1, str2, ...)` - Concatenate strings (variadic)
- `LENGTH(str)` - Get string length
- `TRIM(str)` - Remove leading and trailing whitespace

#### Math Functions
- `ABS(num)` - Absolute value
- `ROUND(num [, decimals])` - Round to specified decimal places (default: 0)
- `FLOOR(num)` - Round down to nearest integer
- `CEIL(num)` - Round up to nearest integer
- `MOD(dividend, divisor)` - Modulo (remainder of division)

#### Aggregate Functions
- `COUNT(*)` - Count all rows
- `COUNT(column)` - Count non-null values in column
- `SUM(column)` - Sum of numeric values
- `AVG(column)` - Average of numeric values
- `MIN(column)` - Minimum value
- `MAX(column)` - Maximum value

#### Window Functions
Window functions perform calculations across rows related to the current row. They require an OVER clause that defines the window specification.

**Ranking Functions:**
- `ROW_NUMBER() OVER (...)` - Sequential row number within partition
- `RANK() OVER (...)` - Rank with gaps for ties
- `DENSE_RANK() OVER (...)` - Rank without gaps for ties
- `NTILE(n) OVER (...)` - Divide rows into n buckets

**Value Functions:**
- `FIRST_VALUE(expr) OVER (...)` - First value in window
- `LAST_VALUE(expr) OVER (...)` - Last value in window
- `NTH_VALUE(expr, n) OVER (...)` - Nth value in window (1-indexed)

**Offset Functions:**
- `LAG(expr [, offset [, default]]) OVER (...)` - Value from previous row (default offset: 1)
- `LEAD(expr [, offset [, default]]) OVER (...)` - Value from next row (default offset: 1)

**Window Specification:**
- `PARTITION BY col1, col2, ...` - Divide rows into partitions (optional)
- `ORDER BY col1 [ASC|DESC], ...` - Define ordering within partition (optional)
- `ROWS/RANGE BETWEEN ...` - Define frame bounds (optional, not fully implemented)

### Value Types

- **Strings**: Use single or double quotes (`'alice'` or `"alice"`)
- **Numbers**: Integers or floats (`30`, `3.14`, `-5`)
- **Booleans**: `true` or `false`

### Query Examples

```sql
-- Select all columns
select * from users.parquet

-- Select specific columns
select name, age from users.parquet

-- Select with aliases
select name as user_name, age as years from users.parquet

-- With WHERE clause
select * from users.parquet where age > 30
select name, age from users.parquet where name = 'alice' AND active = true

-- Using operators
select * from users.parquet where status IN ('active', 'pending')
select * from users.parquet where name LIKE 'John%'
select * from users.parquet where age BETWEEN 18 AND 65
select * from users.parquet where email IS NOT NULL

-- Using DISTINCT
select DISTINCT status from users.parquet
select DISTINCT department, status from users.parquet

-- Using CASE expressions
select name,
       CASE
         WHEN age < 18 THEN 'minor'
         WHEN age < 65 THEN 'adult'
         ELSE 'senior'
       END as age_group
from users.parquet

-- Using functions
select UPPER(name) as upper_name from users.parquet
select CONCAT(first, ' ', last) as full_name from users.parquet
select ROUND(price, 2) as rounded_price from products.parquet

-- Functions with WHERE
select LOWER(name) from users.parquet where LENGTH(name) > 5
select name, ABS(balance) from accounts.parquet where balance < 0

-- Aggregations
select COUNT(*) from users.parquet
select SUM(sales), AVG(sales) from orders.parquet
select MIN(price), MAX(price) from products.parquet

-- GROUP BY
select status, COUNT(*) as total from users.parquet group by status
select department, AVG(salary) as avg_salary from employees.parquet group by department

-- GROUP BY with multiple columns
select department, status, COUNT(*) from users.parquet group by department, status

-- HAVING clause
select status, COUNT(*) as total from users.parquet group by status having total > 10
select department, AVG(salary) as avg_sal from employees.parquet group by department having avg_sal > 50000

-- Window Functions
-- Ranking within a partition
select name, department, salary,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
from employees.parquet

-- Global ranking
select name, score,
       RANK() OVER (ORDER BY score DESC) as rank,
       DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank
from students.parquet

-- Dividing into quartiles
select name, score,
       NTILE(4) OVER (ORDER BY score) as quartile
from students.parquet

-- Accessing values from other rows
select date, value,
       LAG(value, 1) OVER (ORDER BY date) as prev_value,
       LEAD(value, 1) OVER (ORDER BY date) as next_value
from timeseries.parquet

-- First and last values in window
select product, date, price,
       FIRST_VALUE(price) OVER (PARTITION BY product ORDER BY date) as first_price,
       LAST_VALUE(price) OVER (PARTITION BY product ORDER BY date) as latest_price
from prices.parquet

-- Common Table Expressions (CTEs)
-- Simple CTE
WITH active_users AS (
    SELECT * FROM users.parquet WHERE active = true
)
SELECT status, COUNT(*) FROM active_users GROUP BY status

-- Multiple CTEs
WITH
    active_users AS (SELECT * FROM users.parquet WHERE active = true),
    premium_users AS (SELECT * FROM users.parquet WHERE plan = 'premium')
SELECT * FROM active_users

-- Subqueries in FROM clause
SELECT * FROM (
    SELECT name, age FROM users.parquet WHERE age > 30
) WHERE age < 50

-- Subqueries with IN
SELECT * FROM users.parquet
WHERE department IN (SELECT dept FROM large_depts.parquet)

-- Subqueries with EXISTS
SELECT * FROM users.parquet u
WHERE EXISTS (
    SELECT 1 FROM orders.parquet o WHERE o.user_id = u.id
)

-- Scalar subqueries in SELECT
SELECT name,
       (SELECT COUNT(*) FROM orders.parquet) as total_orders
FROM users.parquet

-- Multi-file queries with glob patterns
SELECT * FROM 'data/*.parquet' WHERE date > '2024-01-01'
SELECT _file, COUNT(*) FROM 'logs/2024-*.parquet' GROUP BY _file

-- JOIN queries
-- INNER JOIN
SELECT u.name, o.amount
FROM users.parquet u
JOIN orders.parquet o ON u.id = o.user_id

-- LEFT JOIN with aggregation
SELECT u.name, COUNT(o.id) as order_count
FROM users.parquet u
LEFT JOIN orders.parquet o ON u.id = o.user_id
GROUP BY u.name

-- Multiple JOINs
SELECT u.name, o.order_date, p.name as product
FROM users.parquet u
JOIN orders.parquet o ON u.id = o.user_id
JOIN products.parquet p ON o.product_id = p.id
WHERE o.amount > 100

-- FULL OUTER JOIN
SELECT COALESCE(u.name, 'Unknown') as user_name,
       COALESCE(o.amount, 0) as amount
FROM users.parquet u
FULL OUTER JOIN orders.parquet o ON u.id = o.user_id

-- JOIN with subquery
SELECT u.name, active_orders.count
FROM users.parquet u
JOIN (
    SELECT user_id, COUNT(*) as count
    FROM orders.parquet
    WHERE status = 'active'
    GROUP BY user_id
) active_orders ON u.id = active_orders.user_id
```

## Command Line Options

```
Usage: parcat [options] <file.parquet>

Options:
  -q string
        SQL query (e.g., "select * from file.parquet where age > 30")
  -f string
        Output format: json, jsonl, csv (default "jsonl")
  -limit int
        Limit number of rows (0 = unlimited)
  -schema
        Show schema information instead of data

Examples:
  parcat data.parquet
  parcat -f csv data.parquet
  parcat -q "select * from data.parquet where age > 30" data.parquet
  parcat --schema data.parquet
  parcat -f csv --schema data.parquet
```

## Type Handling

### Parquet to Output Type Mapping

- **INT32/INT64** → Integer
- **FLOAT/DOUBLE** → Float
- **BYTE_ARRAY** → String
- **BOOLEAN** → Boolean
- **Complex/Nested** → Preserved in JSON, flattened in CSV

### Comparison Type Coercion

- **String comparisons**: Case-sensitive
- **Numeric comparisons**: Automatic conversion to float64
- **Boolean comparisons**: Direct equality
- **Type mismatch**: Returns false with warning

## Examples

### Read entire file as JSON Lines
```bash
parcat users.parquet
```

**Output:**
```json
{"id":1,"name":"alice","age":30,"active":true}
{"id":2,"name":"bob","age":25,"active":false}
{"id":3,"name":"charlie","age":35,"active":true}
```

### Read as CSV
```bash
parcat -f csv users.parquet
```

**Output:**
```csv
active,age,id,name
true,30,1,alice
false,25,2,bob
true,35,3,charlie
```

### Filter by age
```bash
parcat -q "select * from users.parquet where age > 30" users.parquet
```

**Output:**
```json
{"id":3,"name":"charlie","age":35,"active":true}
```

### Complex query with AND
```bash
parcat -q "select * from users.parquet where age > 25 AND active = true" users.parquet
```

**Output:**
```json
{"id":1,"name":"alice","age":30,"active":true}
{"id":3,"name":"charlie","age":35,"active":true}
```

### Limit results
```bash
parcat -limit 2 users.parquet
```

## Error Handling

### File Not Found
```bash
$ parcat nonexistent.parquet
Error: failed to open file: open nonexistent.parquet: no such file or directory
```

### Invalid Parquet File
```bash
$ parcat invalid.txt
Error: failed to open parquet file: ...
```

### Query Syntax Error
```bash
$ parcat -q "invalid query" data.parquet
Error parsing query: query must start with SELECT: ...
```

### Column Not Found
When a column doesn't exist, the filter returns false for that row (no error, just filters it out).

## Architecture

```
parcat/
├── cmd/parcat/                     # CLI tool
│   └── main.go                     # CLI entry point
├── reader/                         # Parquet file reading (public API)
│   ├── parquet.go                  # Core reader implementation
│   ├── schema.go                   # Schema introspection
│   ├── doc.go                      # Package documentation
│   └── *_test.go                   # Reader tests
├── query/                          # SQL query engine (public API)
│   ├── lexer.go                    # Query tokenization
│   ├── parser.go                   # Query parsing
│   ├── executor.go                 # Query execution
│   ├── filter.go                   # Filter evaluation
│   ├── aggregate.go                # Aggregation and GROUP BY
│   ├── window.go                   # Window functions
│   ├── function.go                 # Built-in functions
│   ├── types.go                    # AST types
│   ├── doc.go                      # Package documentation
│   └── *_test.go                   # Query tests
├── output/                         # Output formatters (public API)
│   ├── formatter.go                # Formatter interface
│   ├── json.go                     # JSON Lines output
│   ├── csv.go                      # CSV output
│   ├── doc.go                      # Package documentation
│   └── *_test.go                   # Output tests
└── docs/                           # Documentation
    └── FUNCTIONS.md                # Complete function reference
```

## Breaking Changes in v0.x

**Package structure reorganization** (affects users of the Go library):

Prior versions used internal packages that were not importable:
- `internal/reader` → Now `reader` (public API)
- `internal/query` → Now `query` (public API)
- `internal/output` → Now `output` (public API)

**Migration guide**:
```go
// Old imports (v0.0.x - will not work)
import "github.com/vegasq/parcat/internal/reader"
import "github.com/vegasq/parcat/internal/query"

// New imports (v0.1.0+)
import "github.com/vegasq/parcat/reader"
import "github.com/vegasq/parcat/query"
import "github.com/vegasq/parcat/output"
```

This change makes parcat fully usable as a Go library with a stable, documented public API.

## Dependencies

- [github.com/segmentio/parquet-go](https://github.com/segmentio/parquet-go) - Pure Go Parquet library

## Testing

This project maintains high test coverage to ensure reliability and correctness.

### Running Tests

Run the full test suite:
```bash
go test ./...
```

Run tests with race detection:
```bash
go test -race ./...
```

Run tests with verbose output:
```bash
go test -v ./...
```

Run tests for a specific package:
```bash
go test ./query
go test ./reader
go test ./output
go test ./cmd/parcat
```

### Test Coverage

Generate and view test coverage:
```bash
# Generate coverage report
go test -coverprofile=coverage.out ./...

# View coverage summary
go tool cover -func=coverage.out

# Generate HTML coverage report
go tool cover -html=coverage.out -o coverage.html
```

Current test coverage:
- Overall statement coverage: 75+%
- query package: 80+%
- reader package: 80+%
- output package: 83+%
- cmd/parcat package: 50+%

### Code Quality

Run linter checks:
```bash
go vet ./...
```

If golangci-lint is installed:
```bash
golangci-lint run
```

### Writing Tests

When contributing new features or bug fixes:
- Add tests for all new functionality
- Ensure all tests pass before submitting
- Maintain or improve test coverage
- Test edge cases and error conditions

Test files are located alongside the code they test (e.g., `query/function.go` has tests in `query/function_test.go`).

### Test Helper Infrastructure

The project includes comprehensive test helpers for creating parquet test files in `query/testdata_helpers.go`. When writing tests that require parquet data:

- Use `createBasicParquetFile()` for simple test data with common types (int, string, float, bool)
- Use `createComplexParquetFile()` for tests requiring nullable fields, timestamps, or arrays
- Use `createEmptyParquetFile()` for edge case testing
- Use `createNamedBasicParquetFile()` for multi-file tests (e.g., JOINs)

Example:
```go
func TestQuery(t *testing.T) {
    rows := []BasicDataRow{
        {ID: 1, Name: "Alice", Age: 30, Salary: 50000, Active: true, Score: 85.5},
        {ID: 2, Name: "Bob", Age: 25, Salary: 45000, Active: false, Score: 72.3},
    }
    testFile := createBasicParquetFile(t, rows)
    // Test implementation - file is automatically cleaned up
}
```

For detailed patterns and conventions, see `CLAUDE.md`.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Versioning and Releases

This library follows [Semantic Versioning](https://semver.org/). Version tags are created in the format `vMAJOR.MINOR.PATCH` (e.g., `v1.0.0`, `v1.2.3`).

### Continuous Integration

This project uses GitHub Actions for automated testing and releases:

- **Test Workflow** (`.github/workflows/test.yml`): Runs on every push to `main` and `lexicon` branches
  - Executes all tests with race detection
  - Generates coverage reports
  - Uploads coverage to Codecov
  - Runs golangci-lint for code quality

- **Release Workflow** (`.github/workflows/release.yml`): Triggers on version tags
  - Validates semantic version tag format
  - Runs full test suite with 80% coverage requirement
  - Generates changelog from git commits
  - Creates GitHub release automatically
  - Makes version available via `go get`

All pull requests to `main` are automatically tested before merging.

### Creating a New Release

To create a new release:

1. Ensure all tests pass locally:
   ```bash
   go test ./... -v -race
   ```

2. Ensure code quality checks pass:
   ```bash
   golangci-lint run
   ```

3. Update the version in your code if needed and commit any final changes:
   ```bash
   git add .
   git commit -m "chore: prepare for release vX.Y.Z"
   git push origin main
   ```

4. Create and push a version tag:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0: Initial release"
   git push origin v0.1.0
   ```

5. The GitHub Actions release workflow will automatically:
   - Validate the semantic version tag format
   - Run all tests with race detection
   - Verify test coverage is at least 80%
   - Run the linter
   - Generate a changelog from commit messages
   - Create a GitHub release with release notes
   - Make the version available via `go get`

### Version Tag Format

Version tags must follow semantic versioning:
- Release versions: `v1.0.0`, `v1.2.3`, `v2.0.0`
- Pre-release versions: `v1.0.0-alpha.1`, `v1.0.0-beta.2`, `v1.0.0-rc.1`

Invalid formats will be rejected by the release workflow.

### Installing a Specific Version

Users can install specific versions using Go modules:

```bash
# Install latest version
go get github.com/vegasq/parcat

# Install specific version
go get github.com/vegasq/parcat@v0.1.0

# Install specific pre-release
go get github.com/vegasq/parcat@v0.1.0-beta.1
```

## License

MIT License

## Future Enhancements

Potential features for future versions:
- ✅ ~~SELECT specific columns (column projection)~~ - **IMPLEMENTED**
- ✅ ~~Basic functions (string and math)~~ - **IMPLEMENTED**
- ✅ ~~Aggregate functions (COUNT, SUM, AVG, MIN, MAX)~~ - **IMPLEMENTED**
- ✅ ~~GROUP BY clause~~ - **IMPLEMENTED**
- ✅ ~~HAVING clause~~ - **IMPLEMENTED**
- ✅ ~~ORDER BY clause~~ - **IMPLEMENTED**
- ✅ ~~LIMIT/OFFSET in SQL (instead of -limit flag)~~ - **IMPLEMENTED**
- ✅ ~~DISTINCT keyword~~ - **IMPLEMENTED**
- ✅ ~~IN operator for queries~~ - **IMPLEMENTED**
- ✅ ~~LIKE operator for pattern matching~~ - **IMPLEMENTED**
- ✅ ~~BETWEEN operator~~ - **IMPLEMENTED**
- ✅ ~~Window functions~~ - **IMPLEMENTED**
- ✅ ~~CTEs (WITH clauses)~~ - **IMPLEMENTED**
- ✅ ~~Subqueries (IN, EXISTS, scalar)~~ - **IMPLEMENTED**
- ✅ ~~Multiple file support (glob patterns)~~ - **IMPLEMENTED**
- ✅ ~~JOINs (INNER, LEFT, RIGHT, FULL, CROSS)~~ - **IMPLEMENTED**
- ✅ ~~Schema introspection command~~ - **IMPLEMENTED**
- Statistics command
- Pretty table output format
- Streaming for very large files
- Recursive CTEs

## Author

Created by vegasq

## See Also

- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [parquet-tools](https://github.com/apache/parquet-mr/tree/master/parquet-tools) - Official Java-based Parquet tools
