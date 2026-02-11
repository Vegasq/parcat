// Package output provides formatters for converting parquet data to various output formats.
//
// Currently supported formats:
//   - JSON Lines: One JSON object per line
//   - CSV: Comma-separated values with header row
//
// Example usage:
//
//	formatter := output.NewJSONFormatter(os.Stdout)
//	if err := formatter.Format(rows); err != nil {
//	    log.Fatal(err)
//	}
package output

import "io"

// Formatter defines the interface for output formatters.
//
// Implementers must provide Format to convert rows to the target format
// and SetOutput to change the output destination.
type Formatter interface {
	// Format writes rows in the formatter's specific format
	Format(rows []map[string]interface{}) error

	// SetOutput changes the output writer
	SetOutput(w io.Writer)
}
