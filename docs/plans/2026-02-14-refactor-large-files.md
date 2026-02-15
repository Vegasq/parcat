# Refactor Large Files into Smaller Modules

Refactor large files (>1000 lines) into smaller, focused modules to improve maintainability and readability.

## Files to Refactor

- query/parser.go (1633 lines) - split by functional areas
- query/function.go (1080 lines) - split by function categories
- query/integration_parquet_test.go (3126 lines) - split by test categories
- query/executor_test.go (2240 lines) - split by test categories
- query/function_test.go (1424 lines) - split by function categories
- query/parser_test.go (1029 lines) - split by parsing areas

## Approach

- Follow Go best practices: keep related code in focused files
- Maintain existing public API - no breaking changes
- Split by logical domain boundaries identified through code analysis
- Each new file should be 200-600 lines ideally
- Test files follow same split pattern as their implementation files
- **Testing approach**: Regular (refactor code, then update/verify tests)
- Complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Files Affected

### Production code:
- Modify: `query/function.go` (split into 4 files)
- Create: `query/function_string.go` (string functions)
- Create: `query/function_math.go` (math functions)
- Create: `query/function_datetime.go` (date/time functions)
- Create: `query/function_convert.go` (type conversion functions)
- Modify: `query/parser.go` (split into 3 files)
- Create: `query/parser_expression.go` (expression parsing)
- Create: `query/parser_function.go` (function and window parsing)

### Test code:
- Modify: `query/function_test.go` (split into 4 files)
- Create: `query/function_string_test.go`
- Create: `query/function_math_test.go`
- Create: `query/function_datetime_test.go`
- Create: `query/function_convert_test.go`
- Modify: `query/integration_parquet_test.go` (split into 5 files)
- Create: `query/integration_filter_test.go` (filter, projection, distinct tests)
- Create: `query/integration_aggregate_test.go` (groupby, having, aggregate tests)
- Create: `query/integration_join_test.go` (all join tests)
- Create: `query/integration_advanced_test.go` (CTE, subquery, window, case tests)
- Create: `query/integration_orderby_test.go` (orderby, limit, offset tests)
- Modify: `query/executor_test.go` (split into 3 files)
- Create: `query/executor_join_test.go` (join tests)
- Create: `query/executor_cte_test.go` (CTE and subquery tests)
- Modify: `query/parser_test.go` (split into 2 files)
- Create: `query/parser_expression_test.go`

## Implementation Tasks

### Task 1: Refactor query/function.go

- [ ] Split query/function.go into category files (string, math, datetime, convert)
- [ ] Move function implementations to new files preserving all functionality
- [ ] Ensure all function types remain exported and accessible
- [ ] Verify FunctionRegistry still works with split files
- [ ] Run query package tests - must pass before task 2

### Task 2: Refactor query/function_test.go

- [ ] Split query/function_test.go by function category
- [ ] Move string function tests to function_string_test.go
- [ ] Move math function tests to function_math_test.go
- [ ] Move datetime function tests to function_datetime_test.go
- [ ] Move conversion function tests to function_convert_test.go
- [ ] Run query package tests - must pass before task 3

### Task 3: Refactor query/parser.go

- [ ] Split query/parser.go into focused files
- [ ] Create parser_expression.go with expression parsing methods
- [ ] Create parser_function.go with function/window parsing methods
- [ ] Keep main query parsing in parser.go
- [ ] Ensure Parser type and methods remain properly connected
- [ ] Run query package tests - must pass before task 4

### Task 4: Refactor query/integration_parquet_test.go

- [ ] Split query/integration_parquet_test.go by test category
- [ ] Move filter/projection/distinct tests to integration_filter_test.go
- [ ] Move groupby/having/aggregate tests to integration_aggregate_test.go
- [ ] Move join tests to integration_join_test.go
- [ ] Move CTE/subquery/window/case tests to integration_advanced_test.go
- [ ] Move orderby/limit/offset tests to integration_orderby_test.go
- [ ] Keep test helper functions in integration_parquet_test.go or move to testdata_helpers.go
- [ ] Run query package tests - must pass before task 5

### Task 5: Refactor query/executor_test.go

- [ ] Split query/executor_test.go by test category
- [ ] Move join-related tests to executor_join_test.go
- [ ] Move CTE and subquery tests to executor_cte_test.go
- [ ] Keep core executor tests in executor_test.go
- [ ] Run query package tests - must pass before task 6

### Task 6: Refactor query/parser_test.go

- [ ] Split query/parser_test.go by parsing area
- [ ] Move expression parsing tests to parser_expression_test.go
- [ ] Keep main parser tests in parser_test.go
- [ ] Run query package tests - must pass

## Verification

- [ ] Run full test suite: `go test ./...`
- [ ] Verify test coverage remains at 80%+: `go test -coverprofile=coverage.out ./query && go tool cover -func coverage.out`
- [ ] Run `go vet ./...`
- [ ] Verify no build errors: `go build ./...`
- [ ] Check that all files are under 1000 lines: `find query -name "*.go" -exec wc -l {} + | sort -rn`

## Documentation

- [ ] Update CLAUDE.md to document the new file organization pattern
- [ ] Update CLAUDE.md with guidance on where to add new functions (by category)
- [ ] Move this plan to `docs/plans/completed/`
