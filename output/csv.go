package output

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strings"
)

// CSVFormatter outputs rows as CSV format
type CSVFormatter struct {
	writer io.Writer
}

// NewCSVFormatter creates a new CSV formatter
func NewCSVFormatter(w io.Writer) *CSVFormatter {
	return &CSVFormatter{writer: w}
}

// SetOutput sets the output writer
func (c *CSVFormatter) SetOutput(w io.Writer) {
	c.writer = w
}

// Format writes rows as CSV
func (c *CSVFormatter) Format(rows []map[string]interface{}) error {
	csvWriter := csv.NewWriter(c.writer)

	if len(rows) == 0 {
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return fmt.Errorf("failed to flush CSV writer: %w", err)
		}
		return nil
	}

	// Extract all unique column names from all rows (in case of heterogeneous schemas)
	// This handles cases like OUTER JOINs or sparse data where different rows may have different columns
	columnSet := make(map[string]bool)
	for _, row := range rows {
		for col := range row {
			columnSet[col] = true
		}
	}

	// Convert to sorted slice for consistent ordering
	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Write header
	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	// Write rows
	for _, row := range rows {
		record := make([]string, len(columns))
		for i, col := range columns {
			record[i] = formatValue(row[col])
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	// Flush and check for errors
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return nil
}

// formatValue converts a value to string for CSV output
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		// Sanitize against CSV injection by prefixing dangerous characters
		// that could trigger formula execution in spreadsheet applications
		if len(val) > 0 {
			firstChar := val[0]
			if firstChar == '=' || firstChar == '+' || firstChar == '-' || firstChar == '@' || firstChar == '\t' || firstChar == '\r' || firstChar == '\n' || firstChar == '|' {
				// Escape existing single quotes and prefix with quote to prevent formula injection
				return "'" + strings.ReplaceAll(val, "'", "''")
			}
		}
		return val
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	default:
		// For complex types, use JSON representation
		return fmt.Sprintf("%v", val)
	}
}
