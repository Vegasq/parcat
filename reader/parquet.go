// Package reader provides functionality for reading Apache Parquet files.
//
// It uses the segmentio/parquet-go library to read parquet files and returns
// rows as maps for flexible data access.
package reader

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Reader reads parquet files and returns rows as maps.
//
// It maintains both an OS file handle and a parquet file handle to enable
// proper resource cleanup.
type Reader struct {
	file   *os.File
	pqFile *parquet.File
}

// NewReader creates a new parquet reader for the specified file path.
//
// The file is opened and validated as a parquet file. Returns an error if
// the file doesn't exist or is not a valid parquet file.
//
// Example:
//
//	reader, err := NewReader("data.parquet")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	pqFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	return &Reader{
		file:   file,
		pqFile: pqFile,
	}, nil
}

// ReadAll reads all rows from the parquet file into memory.
//
// Each row is returned as a map where keys are column names and values are
// the column values. The entire file is loaded into memory, so this method
// may not be suitable for very large files.
//
// Returns an error if any row fails to read.
func (r *Reader) ReadAll() ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0)

	reader := parquet.NewReader(r.pqFile)
	defer func() { _ = reader.Close() }()

	for {
		row := make(map[string]interface{})
		err := reader.Read(&row)
		if err != nil {
			// Use errors.Is for proper EOF detection
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// Schema returns the parquet file schema.
//
// The schema contains metadata about the columns, types, and structure
// of the parquet file.
func (r *Reader) Schema() *parquet.Schema {
	return r.pqFile.Schema()
}

// Close closes the parquet reader and releases associated resources.
//
// Should be called when done reading to avoid resource leaks. It is safe
// to call Close multiple times.
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// ReadMultipleFiles reads all rows from multiple parquet files matching a glob pattern.
//
// The pattern can include wildcards:
//   - * matches any sequence of non-separator characters
//   - ? matches any single non-separator character
//   - [range] matches any character in range
//   - {a,b} matches either a or b
//
// Examples:
//   - "data/*.parquet" - all parquet files in data directory
//   - "data/2024-*.parquet" - parquet files starting with 2024- in data directory
//   - "data/*/*.parquet" - parquet files in subdirectories of data
//
// Each row is tagged with a "_file" column containing the source file path.
// Returns an error if no files match the pattern or if any file fails to read.
func ReadMultipleFiles(pattern string) ([]map[string]interface{}, error) {
	// Check if pattern contains glob wildcards
	if !strings.ContainsAny(pattern, "*?[]{}") {
		// Not a glob pattern, read single file
		r, err := NewReader(pattern)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()

		rows, err := r.ReadAll()
		if err != nil {
			return nil, err
		}

		// Only tag rows with _file if reading multiple files (glob pattern)
		// Don't add _file for single file reads to avoid changing output shape
		// and potentially overwriting existing _file column

		return rows, nil
	}

	// Expand glob pattern
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no files match pattern: %s", pattern)
	}

	// Limit number of files to prevent resource exhaustion
	const maxFiles = 1000
	if len(matches) > maxFiles {
		return nil, fmt.Errorf("glob pattern matched too many files (%d), maximum is %d", len(matches), maxFiles)
	}

	// Read all matching files
	var allRows []map[string]interface{}
	for _, filePath := range matches {
		r, err := NewReader(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
		}

		rows, readErr := r.ReadAll()
		closeErr := r.Close()

		// Preserve the first error encountered
		if readErr != nil {
			return nil, fmt.Errorf("failed to read rows from %s: %w", filePath, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("failed to close %s: %w", filePath, closeErr)
		}

		// Tag each row with the source file (only for multi-file reads)
		// Always set _file column to track source file
		for i := range rows {
			rows[i]["_file"] = filePath
		}

		allRows = append(allRows, rows...)
	}

	return allRows, nil
}
