package query

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// BasicDataRow defines a simple test data structure with common data types
type BasicDataRow struct {
	ID     int64   `parquet:"id"`
	Name   string  `parquet:"name"`
	Age    int64   `parquet:"age"`
	Salary float64 `parquet:"salary"`
	Active bool    `parquet:"active"`
	Score  float64 `parquet:"score"`
}

// ComplexDataRow defines a more complex test data structure with nullable and timestamp fields
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

// createBasicParquetFile creates a temporary parquet file with BasicDataRow structure
// Returns the path to the created file
func createBasicParquetFile(t *testing.T, rows []BasicDataRow) string {
	t.Helper()
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_basic.parquet")

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[BasicDataRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return testFile
}

// createComplexParquetFile creates a temporary parquet file with ComplexDataRow structure
// Returns the path to the created file
func createComplexParquetFile(t *testing.T, rows []ComplexDataRow) string {
	t.Helper()
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_complex.parquet")

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[ComplexDataRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return testFile
}

// createEmptyParquetFile creates a temporary empty parquet file with BasicDataRow structure
// This is useful for testing edge cases with empty files
func createEmptyParquetFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_empty.parquet")

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create empty test file: %v", err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[BasicDataRow](f)
	// Write empty slice - no rows
	if _, err := writer.Write([]BasicDataRow{}); err != nil {
		t.Fatalf("failed to write empty data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return testFile
}

// createNamedBasicParquetFile creates a parquet file with a specific name in a temp directory
// Useful for tests that need specific file names (e.g., join tests with multiple files)
func createNamedBasicParquetFile(t *testing.T, dir, filename string, rows []BasicDataRow) string {
	t.Helper()
	testFile := filepath.Join(dir, filename)

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file %s: %v", filename, err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[BasicDataRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data to %s: %v", filename, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer for %s: %v", filename, err)
	}

	return testFile
}

// createNamedComplexParquetFile creates a complex parquet file with a specific name
func createNamedComplexParquetFile(t *testing.T, dir, filename string, rows []ComplexDataRow) string {
	t.Helper()
	testFile := filepath.Join(dir, filename)

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file %s: %v", filename, err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[ComplexDataRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data to %s: %v", filename, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer for %s: %v", filename, err)
	}

	return testFile
}

// int64Ptr returns a pointer to an int64 value
func int64Ptr(v int64) *int64 {
	return &v
}

// float64Ptr returns a pointer to a float64 value
func float64Ptr(v float64) *float64 {
	return &v
}

// boolPtr returns a pointer to a bool value
func boolPtr(v bool) *bool {
	return &v
}
