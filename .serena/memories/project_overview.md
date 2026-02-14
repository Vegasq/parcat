# Parcat Project Overview

## Purpose
A GNU-inspired CLI tool to read and query Apache Parquet files, similar to how `cat` works for text files. Supports SQL-like queries with WHERE, JOIN, GROUP BY, window functions, CTEs, etc.

## Tech Stack
- Language: Go 1.24.3
- Main dependency: github.com/segmentio/parquet-go
- Pure Go implementation with minimal external dependencies

## Structure
- `cmd/parcat/main.go` - CLI entry point
- `reader/` - Parquet file reading (public API)
- `query/` - Query parsing and execution (public API - lexer, parser, filter, executor, aggregates, window functions)
- `output/` - Output formatters (public API - JSON, CSV)
- `docs/` - Documentation including FUNCTIONS.md reference
