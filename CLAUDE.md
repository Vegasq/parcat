# Parcat Development Guide for AI Assistants

This document provides patterns and conventions for AI coding assistants working on the parcat project.

## Project Overview

Parcat is a Go library and CLI tool for reading, querying, and formatting Apache Parquet files with SQL-like query support. The codebase is organized into three main packages:
- `reader/` - Parquet file reading functionality
- `query/` - SQL query parsing and execution engine
- `output/` - Output formatters (JSON, CSV)

## Code Organization

### Package Structure
```
parcat/
├── cmd/parcat/              # CLI tool
├── reader/                  # Public API for reading parquet files
├── query/                   # Public API for SQL query engine
├── output/                  # Public API for output formatters
└── docs/                    # Documentation
```

### Import Conventions
Always use public API imports:
```go
import "github.com/vegasq/parcat/reader"
import "github.com/vegasq/parcat/query"
import "github.com/vegasq/parcat/output"
```

### Query Package File Organization

The query package is organized by functional areas to improve maintainability. Files are kept under 1000 lines and focused on specific responsibilities.

#### Core Components (200-600 lines each)
- `lexer.go` - SQL tokenization
- `parser.go` - Main SQL parsing logic (SELECT, FROM, WHERE, JOIN, GROUP BY, etc.)
- `parser_expression.go` - Expression parsing (binary operations, comparisons, literals)
- `parser_function.go` - Function call and window function parsing
- `executor.go` - Query execution orchestration
- `filter.go` - WHERE clause evaluation
- `aggregate.go` - GROUP BY and aggregation functions
- `window.go` - Window functions (ROW_NUMBER, RANK, etc.)
- `types.go` - AST types and structures

#### Built-in Functions (organized by category)
- `function.go` - Function registry and core function infrastructure
- `function_string.go` - String functions (UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LENGTH, etc.)
- `function_math.go` - Math functions (ABS, CEIL, FLOOR, ROUND, SQRT, POW, MOD, etc.)
- `function_datetime.go` - Date/time functions (EXTRACT, DATE_TRUNC, NOW, etc.)
- `function_convert.go` - Type conversion functions (CAST, COALESCE, etc.)

#### Test Files (organized by focus area)

**Unit Tests:**
- `function_string_test.go` - Tests for string functions
- `function_math_test.go` - Tests for math functions
- `function_datetime_test.go` - Tests for date/time functions
- `function_convert_test.go` - Tests for conversion functions
- `parser_test.go` - Core parser tests (SELECT, JOIN, GROUP BY parsing)
- `parser_expression_test.go` - Expression parsing tests
- `executor_test.go` - Core executor tests
- `executor_join_test.go` - Join execution tests
- `executor_cte_test.go` - CTE and subquery execution tests

**Integration Tests:**
- `integration_parquet_test.go` - Test helpers and basic integration tests
- `integration_filter_test.go` - Filter, projection, and DISTINCT tests
- `integration_aggregate_test.go` - GROUP BY, HAVING, and aggregate function tests
- `integration_join_test.go` - All JOIN type tests (INNER, LEFT, RIGHT, FULL, CROSS)
- `integration_advanced_test.go` - CTE, subquery, window function, and CASE tests
- `integration_orderby_test.go` - ORDER BY, LIMIT, and OFFSET tests

#### Adding New Functions

When adding new built-in functions, place them in the appropriate category file:

1. **String functions** (UPPER, LOWER, SUBSTRING, etc.) -> `function_string.go`
   - Tests go in `function_string_test.go`

2. **Math functions** (ABS, ROUND, SQRT, etc.) -> `function_math.go`
   - Tests go in `function_math_test.go`

3. **Date/time functions** (EXTRACT, DATE_TRUNC, etc.) -> `function_datetime.go`
   - Tests go in `function_datetime_test.go`

4. **Conversion functions** (CAST, COALESCE, etc.) -> `function_convert.go`
   - Tests go in `function_convert_test.go`

Each function must:
- Be registered in the FunctionRegistry (in `function.go`)
- Have a corresponding implementation function
- Include comprehensive unit tests
- Include integration tests in the appropriate `integration_*_test.go` file

## Testing Patterns

### Test File Organization
- Test files are located alongside the code they test
- Tests are split by functional area matching their implementation files
- Unit tests (e.g., `function_string_test.go`) test individual functions in isolation
- Integration tests (e.g., `integration_filter_test.go`) test end-to-end SQL query execution
- Integration tests use real parquet files created via helper functions in `testdata_helpers.go`
- See "Query Package File Organization" section for the complete test file structure

### Creating Test Parquet Files

The `query/testdata_helpers.go` file provides helper functions for creating parquet test files. This pattern should be followed for all tests requiring parquet data.

#### Basic Test Data Structure
```go
// Use BasicDataRow for most tests
type BasicDataRow struct {
    ID     int64   `parquet:"id"`
    Name   string  `parquet:"name"`
    Age    int64   `parquet:"age"`
    Salary float64 `parquet:"salary"`
    Active bool    `parquet:"active"`
    Score  float64 `parquet:"score"`
}
```

#### Complex Test Data Structure
```go
// Use ComplexDataRow for tests requiring nullable fields, timestamps, or arrays
type ComplexDataRow struct {
    ID        int64      `parquet:"id"`
    Name      string     `parquet:"name"`
    Age       *int64     `parquet:"age,optional"`
    Timestamp time.Time  `parquet:"timestamp"`
    Salary    *float64   `parquet:"salary,optional"`
    Active    *bool      `parquet:"active,optional"`
    Tags      []string   `parquet:"tags,list"`
    Score     *float64   `parquet:"score,optional"`
}
```

#### Helper Functions

**Creating a basic parquet file:**
```go
func TestExample(t *testing.T) {
    rows := []BasicDataRow{
        {ID: 1, Name: "Alice", Age: 30, Salary: 50000, Active: true, Score: 85.5},
        {ID: 2, Name: "Bob", Age: 25, Salary: 45000, Active: false, Score: 72.3},
    }
    testFile := createBasicParquetFile(t, rows)
    // testFile is automatically cleaned up via t.TempDir()
}
```

**Creating multiple files for join tests:**
```go
func TestJoin(t *testing.T) {
    tmpDir := t.TempDir()

    usersFile := createNamedBasicParquetFile(t, tmpDir, "users.parquet", usersRows)
    ordersFile := createNamedBasicParquetFile(t, tmpDir, "orders.parquet", ordersRows)

    sql := fmt.Sprintf("SELECT * FROM %s JOIN %s ON ...", usersFile, ordersFile)
    // ... test implementation
}
```

**Creating empty files for edge case tests:**
```go
func TestEmptyFile(t *testing.T) {
    emptyFile := createEmptyParquetFile(t)
    // Test edge case behavior with empty parquet file
}
```

**Working with nullable fields:**
```go
func TestNullValues(t *testing.T) {
    rows := []ComplexDataRow{
        {
            ID:     1,
            Name:   "Alice",
            Age:    int64Ptr(30),      // Non-null
            Salary: float64Ptr(50000), // Non-null
            Active: boolPtr(true),     // Non-null
        },
        {
            ID:     2,
            Name:   "Bob",
            Age:    nil,               // NULL value
            Salary: nil,               // NULL value
            Active: nil,               // NULL value
        },
    }
    testFile := createComplexParquetFile(t, rows)
    // Test null handling
}
```

#### Key Patterns

1. **Use t.TempDir() for automatic cleanup:**
   - All helper functions use `t.TempDir()` or accept a directory from it
   - No manual cleanup required - Go testing framework handles it

2. **Use parquet.NewGenericWriter for type-safe writing:**
   ```go
   writer := parquet.NewGenericWriter[BasicDataRow](f)
   if _, err := writer.Write(rows); err != nil {
       t.Fatalf("failed to write test data: %v", err)
   }
   ```

3. **Always use t.Helper() in helper functions:**
   - Ensures test failures report the correct line number
   - Applied to all helper functions in testdata_helpers.go

4. **Pointer helper functions for nullable fields:**
   - `int64Ptr(v)` - creates *int64
   - `float64Ptr(v)` - creates *float64
   - `boolPtr(v)` - creates *bool

### Test Coverage Requirements

- Minimum overall coverage: 75%
- Query package target: 80%+
- Reader package target: 80%+
- Output package target: 83%+

### Running Tests

```bash
# Full test suite
go test ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# With race detection
go test -race ./...

# Specific package
go test ./query
```

## Query Engine Architecture

### Key Components
- `lexer.go` - SQL tokenization
- `parser.go` - Main SQL parsing (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY)
- `parser_expression.go` - Expression parsing (operators, comparisons, literals)
- `parser_function.go` - Function call and window function parsing
- `executor.go` - Query execution orchestration
- `filter.go` - WHERE clause evaluation
- `aggregate.go` - GROUP BY and aggregation functions
- `window.go` - Window functions
- `function.go` - Function registry and infrastructure
- `function_string.go` - String function implementations
- `function_math.go` - Math function implementations
- `function_datetime.go` - Date/time function implementations
- `function_convert.go` - Type conversion function implementations

### Adding New Features

When adding new SQL features:
1. Add tokens to lexer if needed
2. Update AST types in `types.go`
3. Add parsing logic to the appropriate parser file:
   - Main clauses (SELECT, JOIN, etc.) -> `parser.go`
   - Expressions (operators, comparisons) -> `parser_expression.go`
   - Functions and window functions -> `parser_function.go`
4. Implement execution logic in `executor.go` or dedicated file
5. Add unit tests to the appropriate test file
6. Add integration tests to the appropriate `integration_*_test.go` file
7. Update documentation in README.md

## Code Quality Standards

### Linting
```bash
go vet ./...
golangci-lint run  # If installed
```

### Error Handling
- Always check and handle errors
- Use descriptive error messages
- Wrap errors with context using fmt.Errorf

### Documentation
- All public functions must have godoc comments
- Package-level documentation in doc.go files
- Keep README.md synchronized with features

## Release Process

1. Ensure all tests pass: `go test ./... -v -race`
2. Check code quality: `golangci-lint run`
3. Update version and commit changes
4. Create and push tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
5. GitHub Actions handles the rest (testing, coverage check, release creation)

## Common Pitfalls

### Parquet Type Handling
- Be aware of type coercion between parquet types and Go types
- Handle nullable fields properly with pointer types
- Test with both required and optional fields

### Query Execution
- Remember that parquet columns may not be in a predictable order
- Always test with real parquet files, not just in-memory maps
- Consider performance with large files (1000+ rows)

### Testing
- Don't manually create test parquet files - use helper functions
- Always test edge cases (empty files, null values, large datasets)
- Test both success and error conditions

## File Naming Conventions

- Test files: `*_test.go`
- Category-specific files: `<base>_<category>.go` (e.g., `function_string.go`, `function_math.go`)
- Category-specific tests: `<base>_<category>_test.go` (e.g., `function_string_test.go`)
- Integration tests: `integration_<category>_test.go` (e.g., `integration_filter_test.go`)
- Parser components: `parser_<component>.go` (e.g., `parser_expression.go`, `parser_function.go`)
- Executor components: `executor_<component>_test.go` (e.g., `executor_join_test.go`)
- Test helpers: `testdata_helpers.go`
- Documentation: `doc.go` (package docs), `*.md` (guides)

## Plans and Progress Tracking

Completed refactoring plans and architectural decisions are documented in:
- `docs/plans/completed/` - Successfully completed refactoring plans
- Each plan file includes the date, objectives, tasks completed, and verification steps

When undertaking major refactoring work:
1. Create a plan in `docs/plans/` with objectives and tasks
2. Complete tasks incrementally with full test verification between each
3. Move plan to `docs/plans/completed/` when finished
4. Update CLAUDE.md with any new patterns or conventions discovered

## Best Practices

1. **Keep functions focused** - Single responsibility principle
2. **Use table-driven tests** - For testing multiple scenarios
3. **Prefer composition over inheritance** - Go idiom
4. **Handle nil cases** - Always check for nil pointers
5. **Use meaningful variable names** - Avoid single-letter names except in small scopes
6. **Comment complex logic** - Explain why, not what
7. **Write tests first for bug fixes** - Ensure the bug is fixed and stays fixed
