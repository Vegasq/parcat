package reader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestReadMultipleFiles_SingleFile(t *testing.T) {
	// Create a temporary test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Test reading single file (no glob)
	result, err := ReadMultipleFiles(testFile)
	if err != nil {
		t.Fatalf("ReadMultipleFiles() error = %v", err)
	}

	if len(result) != 2 {
		t.Errorf("ReadMultipleFiles() returned %d rows, want 2", len(result))
	}

	// For single file reads (no glob), _file should NOT be added
	// This prevents overwriting user data and changing output shape
	if _, hasFile := result[0]["_file"]; hasFile {
		t.Errorf("ReadMultipleFiles() single file should not add _file column, but found: %v", result[0]["_file"])
	}
}

func TestReadMultipleFiles_GlobPattern(t *testing.T) {
	// Create temporary test directory
	tmpDir := t.TempDir()

	// Create test data type
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	// Create multiple test files
	files := []struct {
		name string
		rows []Row
	}{
		{"file1.parquet", []Row{{ID: 1, Name: "Alice"}}},
		{"file2.parquet", []Row{{ID: 2, Name: "Bob"}}},
		{"file3.parquet", []Row{{ID: 3, Name: "Charlie"}}},
	}

	for _, tf := range files {
		testFile := filepath.Join(tmpDir, tf.name)

		f, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("failed to create test file %s: %v", tf.name, err)
		}

		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(tf.rows); err != nil {
			t.Fatalf("failed to write test data to %s: %v", tf.name, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer for %s: %v", tf.name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file %s: %v", tf.name, err)
		}
	}

	// Test reading with glob pattern
	pattern := filepath.Join(tmpDir, "*.parquet")
	result, err := ReadMultipleFiles(pattern)
	if err != nil {
		t.Fatalf("ReadMultipleFiles() error = %v", err)
	}

	if len(result) != 3 {
		t.Errorf("ReadMultipleFiles() returned %d rows, want 3", len(result))
	}

	// Check that _file column is present and unique
	fileSet := make(map[string]bool)
	for _, row := range result {
		if file, ok := row["_file"]; ok {
			if fileStr, ok := file.(string); ok {
				fileSet[fileStr] = true
			} else {
				t.Errorf("_file column is not a string: %T", file)
			}
		} else {
			t.Errorf("_file column missing in row")
		}
	}

	if len(fileSet) != 3 {
		t.Errorf("Expected rows from 3 different files, got %d", len(fileSet))
	}
}

func TestReadMultipleFiles_NoMatch(t *testing.T) {
	tmpDir := t.TempDir()
	pattern := filepath.Join(tmpDir, "*.parquet")

	// Try to read from pattern with no matches
	_, err := ReadMultipleFiles(pattern)
	if err == nil {
		t.Errorf("ReadMultipleFiles() expected error for no matching files, got nil")
	}
}

func TestReadMultipleFiles_SpecificPattern(t *testing.T) {
	// Create temporary test directory
	tmpDir := t.TempDir()

	// Create test data type
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	// Create test files with different prefixes
	files := []struct {
		name string
		rows []Row
	}{
		{"data-2024.parquet", []Row{{ID: 1, Name: "Alice"}}},
		{"data-2025.parquet", []Row{{ID: 2, Name: "Bob"}}},
		{"other-2024.parquet", []Row{{ID: 3, Name: "Charlie"}}},
	}

	for _, tf := range files {
		testFile := filepath.Join(tmpDir, tf.name)

		f, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("failed to create test file %s: %v", tf.name, err)
		}

		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(tf.rows); err != nil {
			t.Fatalf("failed to write test data to %s: %v", tf.name, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer for %s: %v", tf.name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file %s: %v", tf.name, err)
		}
	}

	// Test reading only files matching "data-*"
	pattern := filepath.Join(tmpDir, "data-*.parquet")
	result, err := ReadMultipleFiles(pattern)
	if err != nil {
		t.Fatalf("ReadMultipleFiles() error = %v", err)
	}

	if len(result) != 2 {
		t.Errorf("ReadMultipleFiles() returned %d rows, want 2", len(result))
	}
}
