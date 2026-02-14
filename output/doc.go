// Package output provides formatters for converting parquet data to various output formats.
//
// This package defines the Formatter interface and provides implementations
// for common output formats like JSON Lines and CSV. All formatters work
// with rows represented as []map[string]interface{}.
//
// # Supported Formats
//
//   - JSON Lines: One JSON object per line (suitable for streaming)
//   - CSV: Comma-separated values with header row
//
// # Basic Usage
//
// Using the JSON formatter:
//
//	formatter := output.NewJSONFormatter(os.Stdout)
//	if err := formatter.Format(rows); err != nil {
//	    log.Fatal(err)
//	}
//
// Using the CSV formatter:
//
//	formatter := output.NewCSVFormatter(os.Stdout)
//	if err := formatter.Format(rows); err != nil {
//	    log.Fatal(err)
//	}
//
// # Writing to Different Destinations
//
// Change output destination dynamically:
//
//	formatter := output.NewJSONFormatter(os.Stdout)
//
//	// Write to file
//	file, err := os.Create("output.json")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer file.Close()
//
//	formatter.SetOutput(file)
//	if err := formatter.Format(rows); err != nil {
//	    log.Fatal(err)
//	}
//
// # Using as String
//
// Write to a bytes buffer to get string output:
//
//	var buf bytes.Buffer
//	formatter := output.NewCSVFormatter(&buf)
//	if err := formatter.Format(rows); err != nil {
//	    log.Fatal(err)
//	}
//	csvString := buf.String()
//
// # Formatter Interface
//
// Implement custom formatters by satisfying the Formatter interface:
//
//	type Formatter interface {
//	    Format(rows []map[string]interface{}) error
//	    SetOutput(w io.Writer)
//	}
//
// # Type Handling
//
// The formatters handle common Go types automatically:
//   - Strings, numbers (int, float), booleans are output directly
//   - JSON formatter preserves nested objects and arrays
//   - CSV formatter flattens nested structures
//   - Null/nil values are handled appropriately for each format
package output
