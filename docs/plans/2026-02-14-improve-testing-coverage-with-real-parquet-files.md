# Comprehensive Test Coverage Improvement for Parcat Query Engine

Comprehensive test coverage improvement for parcat query engine using real parquet files

## Overview

**Files involved:**
- Create: `query/testdata_helpers.go`
- Create: `query/integration_parquet_test.go`
- Modify: `query/integration_test.go`
- Reference: `cmd/parcat/integration_test.go` (pattern for creating parquet files)

**Related patterns:**
- Follow existing TestRow pattern from cmd/parcat/integration_test.go
- Use parquet.NewGenericWriter for type-safe parquet generation
- Use t.TempDir() for test file cleanup
- Group tests by query feature (CTEs, joins, aggregations, etc.)

**Dependencies:**
- github.com/parquet-go/parquet-go (already in use)

## Approach

- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**
- Create reusable test data generators for different parquet schemas
- Test each major query feature (WHERE, GROUP BY, JOIN, CTE, etc.) against real parquet files

## Implementation Tasks

### Files
- Create: `query/testdata_helpers.go`
- Create: `query/integration_parquet_test.go`
- Modify: `query/integration_test.go`

### Task 1: Create test data helper infrastructure

- [x] Create query/testdata_helpers.go with parquet file generation functions
- [x] Implement BasicDataRow struct (ID, Name, Age, Salary, Active bool, Score float64)
- [x] Implement ComplexDataRow struct (nested fields, arrays, timestamps, nullables)
- [x] Add helper function createBasicParquetFile(t, rows) string
- [x] Add helper function createComplexParquetFile(t, rows) string
- [x] Add helper function createEmptyParquetFile(t) string for edge cases
- [x] Write unit tests for helper functions
- [x] Run project test suite - must pass before task 2

### Task 2: Implement basic query feature tests with real parquet files

- [x] Create query/integration_parquet_test.go
- [x] Add TestParquetFilter with WHERE clause variations (=, !=, <, >, <=, >=, AND, OR, NOT)
- [x] Add TestParquetProjection with column selection and aliasing
- [x] Add TestParquetDistinct for DISTINCT keyword
- [x] Add TestParquetLimitOffset for pagination
- [x] Add TestParquetOrderBy with ASC/DESC and multiple columns
- [x] Write tests for this task - verify all scenarios pass
- [x] Run project test suite - must pass before task 3

### Task 3: Implement aggregation and grouping tests

- [x] Add TestParquetGroupBy with various grouping scenarios
- [x] Add TestParquetHaving for post-aggregation filtering
- [x] Add TestParquetAggregates (COUNT, SUM, AVG, MIN, MAX)
- [x] Add TestParquetGroupByMultipleColumns
- [x] Add TestParquetAggregateWithNulls for null handling
- [x] Write tests for this task - verify all scenarios pass
- [x] Run project test suite - must pass before task 4

### Task 4: Implement join tests with multiple parquet files

- [x] Add TestParquetInnerJoin
- [x] Add TestParquetLeftJoin
- [x] Add TestParquetRightJoin
- [x] Add TestParquetFullJoin
- [x] Add TestParquetCrossJoin
- [x] Add TestParquetMultipleJoins (3+ table joins)
- [x] Write tests for this task - verify all scenarios pass
- [x] Run project test suite - must pass before task 5

### Task 5: Implement advanced query feature tests

- [x] Add TestParquetCTE (WITH clause with single and multiple CTEs)
- [x] Add TestParquetSubquery (in SELECT, FROM, and WHERE)
- [x] Add TestParquetWindowFunctions (ROW_NUMBER, RANK, LAG, LEAD, SUM OVER)
- [x] Add TestParquetCaseExpression
- [x] Add TestParquetComplexExpressions (nested functions, arithmetic)
- [x] Write tests for this task - verify all scenarios pass
- [x] Run project test suite - must pass before task 6

### Task 6: Implement edge case and schema variety tests

- [x] Add TestParquetNullValues (filtering, aggregating nulls)
- [x] Add TestParquetEmptyFile
- [x] Add TestParquetComplexSchema (nested structs, arrays, maps if supported)
- [x] Add TestParquetLargeDataset (1000+ rows for performance)
- [x] Add TestParquetMixedTypes (all supported data types in one file)
- [x] Write tests for this task - verify all scenarios pass
- [x] Run project test suite - must pass before task 7

## Validation

- [ ] Manual test: Run full test suite with go test -v ./query/...
- [ ] Run full test suite: go test ./...
- [ ] Run linter: go vet ./...
- [ ] Verify test coverage: go test -cover ./query/... (target 85%+)
- [ ] Check coverage report: go test -coverprofile=coverage.out ./query/... && go tool cover -html=coverage.out

## Completion

- [ ] Update README.md if testing documentation needed
- [ ] Update CLAUDE.md with test file generation patterns
- [ ] Move this plan to docs/plans/completed/
