package reader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestSchemaMode_MultipleGlobMatches(t *testing.T) {
	// Test that when multiple files match glob pattern, first file is used
	tmpDir := t.TempDir()

	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	// Create multiple parquet files
	for i := 1; i <= 3; i++ {
		testFile := filepath.Join(tmpDir, "test"+string(rune('0'+i))+".parquet")
		rows := []Row{{ID: int64(i), Name: "test"}}

		f, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("failed to create test file %d: %v", i, err)
		}

		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(rows); err != nil {
			t.Fatalf("failed to write test data %d: %v", i, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer %d: %v", i, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file %d: %v", i, err)
		}
	}

	// Extract schema from first match
	pattern := filepath.Join(tmpDir, "test*.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(matches) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(matches))
	}

	// Extract schema from first file
	schemaInfos, err := ExtractSchemaInfo(matches[0])
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Verify we got schema
	if len(schemaInfos) != 2 {
		t.Errorf("ExtractSchemaInfo() returned %d fields, want 2", len(schemaInfos))
	}
}

func TestSchemaMode_InvalidGlobPattern(t *testing.T) {
	// Test invalid glob pattern handling
	pattern := "[invalid"
	_, err := filepath.Glob(pattern)
	if err == nil {
		t.Skip("This platform doesn't validate glob patterns")
	}
	// On platforms that validate, error should be returned
}

func TestSchemaMode_NoGlobMatches(t *testing.T) {
	// Test when no files match the pattern
	tmpDir := t.TempDir()
	pattern := filepath.Join(tmpDir, "nonexistent*.parquet")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}

	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestSchemaMode_PermissionDenied(t *testing.T) {
	// Test file access permission denied scenario
	// This test requires file system permissions manipulation
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "restricted.parquet")

	type Row struct {
		ID int64 `parquet:"id"`
	}

	rows := []Row{{ID: 1}}

	// Create test file
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

	// Remove read permissions
	if err := os.Chmod(testFile, 0000); err != nil {
		t.Skipf("cannot change file permissions: %v", err)
	}
	defer func() { _ = os.Chmod(testFile, 0644) }() // Restore permissions for cleanup

	// Try to extract schema - should fail with permission error
	_, err = ExtractSchemaInfo(testFile)
	if err == nil {
		t.Errorf("ExtractSchemaInfo() expected permission error, got nil")
	}
}

func TestSchemaMode_CorruptedParquetFile(t *testing.T) {
	// Test handling of corrupted parquet file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "corrupted.parquet")

	type Row struct {
		ID int64 `parquet:"id"`
	}

	rows := []Row{{ID: 1}}

	// Create valid parquet file
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

	// Corrupt the file by truncating it
	if err := os.Truncate(testFile, 100); err != nil {
		t.Fatalf("failed to truncate file: %v", err)
	}

	// Try to extract schema - should fail
	_, err = ExtractSchemaInfo(testFile)
	if err == nil {
		t.Errorf("ExtractSchemaInfo() expected error for corrupted file, got nil")
	}
}
