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

	"github.com/segmentio/parquet-go"
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
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	pqFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		file.Close()
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
	defer reader.Close()

	for {
		row := make(map[string]interface{})
		err := reader.Read(&row)
		if err != nil {
			// Use errors.Is for proper EOF detection
			if errors.Is(err, io.EOF) || err.Error() == "EOF" {
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
