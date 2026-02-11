package output

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
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
	if len(rows) == 0 {
		return nil
	}

	csvWriter := csv.NewWriter(c.writer)
	defer csvWriter.Flush()

	// Extract column names from first row and sort for consistent ordering
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
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

	return nil
}

// formatValue converts a value to string for CSV output
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
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
