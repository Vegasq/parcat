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
- [ ] Add tests for EvaluateSelectExpression edge cases (8.3% to 60+%)
- [ ] Add tests for executeSelect with complex scenarios
- [ ] Add tests for valueToNumber and valueToString helper functions
- [ ] Write tests in query/executor_test.go
- [ ] Run: `go test -v ./query -run TestEvaluate`
- [ ] run project test suite - must pass before task 4

### Task 4: Add window function subquery detection tests
- [ ] Test functions: HasSubqueryInWHERE, hasSubqueryInExpression, HasSubqueryInSELECT, hasScalarSubquery
- [ ] Write tests in query/window_test.go
- [ ] Run: `go test -v ./query -run TestSubquery`
- [ ] run project test suite - must pass before task 5

### Task 5: Add parser window frame tests
- [ ] Test functions: parseWindowFrame, parseFrameBound (currently 0%)
- [ ] Write tests in query/parser_test.go
- [ ] Run: `go test -v ./query -run TestWindow`
- [ ] run project test suite - must pass before task 6

### Task 6: Add CLI integration tests
- [ ] Create cmd/parcat/integration_test.go
- [ ] Test main execution paths with actual parquet files
- [ ] Test schema mode, join operations, CTE queries
- [ ] Target: 0% to 50+% for cmd/parcat
- [ ] Run: `go test -v ./cmd/parcat`
- [ ] run project test suite - must pass before verification

### Task 7: Verify overall coverage improvement
- [ ] Run: `go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out`
- [ ] Target: query package 67.6% to 80+%
- [ ] Target: overall statement coverage 59.4% to 75+%
- [ ] Generate HTML report: `go tool cover -html=coverage.out -o coverage.html`
- [ ] run project test suite - must pass before finalization

## Validation

- [ ] manual test: run CLI with various SQL queries on test parquet files
- [ ] run full test suite: `go test -race ./...`
- [ ] run linter: `go vet ./...`
- [ ] verify test coverage meets 75+% overall

## Finalization

- [ ] update README.md with testing instructions and coverage badges if applicable
- [ ] move this plan to `docs/plans/completed/`
