# Review and Improve Test Coverage for Parcat SQL Library

## Overview
Review and improve test coverage for parcat SQL library

**Files involved:**
- query/function.go (date/time functions, type conversion functions)
- query/executor.go (EvaluateSelectExpression, executeSelect)
- query/filter.go (ApplySelectListAfterWindows, window-related functions)
- query/window.go (subquery detection functions)
- cmd/parcat/main.go (CLI binary - integration tests)

**Coverage status:**
- cmd/parcat: 0.0% (needs integration tests)
- output: 83.3% (good)
- query: 67.6% (target: 80+%)
- reader: 76.6% (target: 80+%)

## Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Files

**Modify:**
- `query/function_test.go`
- `query/executor_test.go`
- `query/filter_test.go`
- `query/window_test.go`

**Create:**
- `cmd/parcat/integration_test.go`

## Implementation Tasks

### Task 1: Add tests for date/time functions in query/function.go
- [x] Test functions: NowFunc, CurrentDateFunc, CurrentTimeFunc, DateTruncFunc, DatePartFunc, DateAddFunc, DateSubFunc, DateDiffFunc, YearFunc, MonthFunc, parseDate
- [x] Target: 0% to 70+% for these functions
- [x] Write tests in query/function_test.go
- [x] Run: `go test -v ./query -run TestDate`
- [x] run project test suite - must pass before task 2

### Task 2: Add tests for type conversion functions
- [x] Test functions: CastFunc, TryCastFunc, ToDateFunc, SplitFunc, RandomFunc, MinFunc, MaxFunc
- [x] Write tests in query/function_test.go
- [x] Run: `go test -v ./query -run TestConversion`
- [x] run project test suite - must pass before task 3

### Task 3: Improve executor.go coverage
- [x] Add tests for EvaluateSelectExpression edge cases (8.3% to 60+%)
- [x] Add tests for executeSelect with complex scenarios
- [x] Add tests for valueToNumber and valueToString helper functions
- [x] Write tests in query/executor_test.go
- [x] Run: `go test -v ./query -run TestEvaluate`
- [x] run project test suite - must pass before task 4

### Task 4: Add window function subquery detection tests
- [x] Test functions: HasSubqueryInWHERE, hasSubqueryInExpression, HasSubqueryInSELECT, hasScalarSubquery
- [x] Write tests in query/window_test.go
- [x] Run: `go test -v ./query -run TestSubquery`
- [x] run project test suite - must pass before task 5

### Task 5: Add parser window frame tests
- [x] Test functions: parseWindowFrame, parseFrameBound (currently 0%)
- [x] Write tests in query/parser_test.go
- [x] Run: `go test -v ./query -run TestWindow`
- [x] run project test suite - must pass before task 6

### Task 6: Add CLI integration tests
- [x] Create cmd/parcat/integration_test.go
- [x] Test main execution paths with actual parquet files
- [x] Test schema mode, join operations, CTE queries
- [x] Target: 0% to 50+% for cmd/parcat
- [x] Run: `go test -v ./cmd/parcat`
- [x] run project test suite - must pass before verification

### Task 7: Verify overall coverage improvement
- [x] Run: `go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out`
- [x] Target: query package 67.6% to 80+%
- [x] Target: overall statement coverage 59.4% to 75+%
- [x] Generate HTML report: `go tool cover -html=coverage.out -o coverage.html`
- [x] run project test suite - must pass before finalization

## Validation

- [x] manual test: run CLI with various SQL queries on test parquet files
- [x] run full test suite: `go test -race ./...`
- [x] run linter: `go vet ./...`
- [x] verify test coverage meets 75+% overall

## Finalization

- [ ] update README.md with testing instructions and coverage badges if applicable
- [ ] move this plan to `docs/plans/completed/`
