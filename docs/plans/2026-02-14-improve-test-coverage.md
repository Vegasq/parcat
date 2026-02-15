# Improve Test Coverage Plan

**Date:** 2026-02-14

## Overview

Improve test coverage across parcat library, focusing on untested CLI functions, query engine edge cases, and reader schema handling

## Context

- Files involved:
  - cmd/parcat/integration_test.go (expand)
  - query/filter_test.go (add ApplySelectListAfterWindows tests)
  - query/function_*_test.go (add MinArity/MaxArity tests)
  - reader/schema_test.go (expand getUserFriendlyType tests)
- Related patterns: Use table-driven tests, helper functions from testdata_helpers.go
- Dependencies: parquet-go library for test file creation

## Approach

- **Testing approach**: Regular (code exists, add tests)
- Complete each task fully before moving to the next
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Tasks

**Files:**
- Modify: `cmd/parcat/integration_test.go`
- Modify: `query/filter_test.go`
- Modify: `query/function_string_test.go`
- Modify: `query/function_math_test.go`
- Modify: `query/function_datetime_test.go`
- Modify: `query/function_convert_test.go`
- Modify: `reader/schema_test.go`

### Task 1: Add CLI join helper tests

- [x] Add CLI join helper tests in cmd/parcat/integration_test.go
  - Test executeLeftJoinHelper, executeRightJoinHelper, executeFullJoinHelper
  - Test executeCTEQuery with nested CTEs
  - Test error conditions and edge cases
- [x] run `go test ./cmd/parcat` - must pass

### Task 2: Add ApplySelectListAfterWindows tests

- [x] Add ApplySelectListAfterWindows tests in query/filter_test.go
  - Test window expression projection
  - Test mixed window and regular expressions
  - Test error conditions (missing window results)
- [x] run `go test ./query -run TestApplySelectListAfterWindows` - must pass

### Task 3: Add MinArity/MaxArity tests for all function types

- [x] Add MinArity/MaxArity tests for all function types
  - Add to function_string_test.go for string functions
  - Add to function_math_test.go for math functions
  - Add to function_datetime_test.go for datetime functions
  - Add to function_convert_test.go for convert functions
  - Use table-driven approach to test all function implementations
- [x] run `go test ./query -run TestMinMaxArity` - must pass

### Task 4: Expand reader schema tests

- [ ] Expand reader schema tests in reader/schema_test.go
  - Test getUserFriendlyType with all logical types (UUID, ENUM, JSON, BSON)
  - Test getUserFriendlyType with INT96 and FixedLenByteArray physical types
  - Test GROUP type handling
  - Test UNKNOWN type fallback
- [ ] run `go test ./reader -run TestGetUserFriendlyType` - must pass

## Verification

- [ ] run `go test ./...` - all tests pass
- [ ] run `go test -coverprofile=coverage.out ./...`
- [ ] verify cmd/parcat coverage improved from 13.6% to 40%+
- [ ] verify query package maintains 78%+ coverage
- [ ] verify reader package maintains 76%+ coverage
- [ ] verify overall coverage reaches 75%+
- [ ] run `go vet ./...` - no issues

## Finalization

- [ ] update CLAUDE.md if new test patterns discovered
- [ ] move this plan to `docs/plans/completed/`
