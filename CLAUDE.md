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

## Testing Patterns

### Test File Organization
- Test files are located alongside the code they test (e.g., `query/function.go` has tests in `query/function_test.go`)
- Integration tests use real parquet files created via helper functions
- Unit tests test individual functions in isolation

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
- `lexer.go` - Tokenization
- `parser.go` - SQL parsing to AST
- `executor.go` - Query execution
- `filter.go` - WHERE clause evaluation
- `aggregate.go` - GROUP BY and aggregation
- `window.go` - Window functions
- `function.go` - Built-in functions

### Adding New Features

When adding new SQL features:
1. Add tokens to lexer if needed
2. Update AST types in `types.go`
3. Add parsing logic to `parser.go`
4. Implement execution logic in `executor.go` or dedicated file
5. Add comprehensive tests in `integration_parquet_test.go`
6. Update documentation in README.md

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
- Integration tests: `integration_*_test.go`
- Test helpers: `testdata_helpers.go`
- Documentation: `doc.go` (package docs), `*.md` (guides)

## Best Practices

1. **Keep functions focused** - Single responsibility principle
2. **Use table-driven tests** - For testing multiple scenarios
3. **Prefer composition over inheritance** - Go idiom
4. **Handle nil cases** - Always check for nil pointers
5. **Use meaningful variable names** - Avoid single-letter names except in small scopes
6. **Comment complex logic** - Explain why, not what
7. **Write tests first for bug fixes** - Ensure the bug is fixed and stays fixed
