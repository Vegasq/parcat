package output

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
)

func TestCSVFormatter_Format(t *testing.T) {
	tests := []struct {
		name      string
		rows      []map[string]interface{}
		wantLines int
		wantErr   bool
	}{
		{
			name:      "empty rows",
			rows:      []map[string]interface{}{},
			wantLines: 0,
		},
		{
			name: "single row",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "age": int32(30)},
			},
			wantLines: 2, // header + 1 data row
		},
		{
			name: "multiple rows",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "age": int32(30)},
				{"id": int64(2), "name": "bob", "age": int32(25)},
			},
			wantLines: 3, // header + 2 data rows
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			formatter := NewCSVFormatter(&buf)

			err := formatter.Format(tt.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("Format() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			output := buf.String()
			if tt.wantLines == 0 {
				if output != "" {
					t.Errorf("Format() output should be empty for empty rows")
				}
				return
			}

			// Parse CSV to verify format
			reader := csv.NewReader(strings.NewReader(output))
			records, err := reader.ReadAll()
			if err != nil {
				t.Errorf("Format() produced invalid CSV: %v", err)
				return
			}

			if len(records) != tt.wantLines {
				t.Errorf("Format() produced %d lines, want %d", len(records), tt.wantLines)
			}
		})
	}
}

func TestCSVFormatter_ColumnOrder(t *testing.T) {
	// CSV columns should be sorted alphabetically for consistency
	rows := []map[string]interface{}{
		{"z_last": "value1", "a_first": "value2", "m_middle": "value3"},
	}

	var buf bytes.Buffer
	formatter := NewCSVFormatter(&buf)

	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) < 1 {
		t.Fatal("No header row in CSV output")
	}

	header := records[0]
	if len(header) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(header))
	}

	// Verify alphabetical order
	if header[0] != "a_first" {
		t.Errorf("First column should be 'a_first', got %q", header[0])
	}
	if header[1] != "m_middle" {
		t.Errorf("Second column should be 'm_middle', got %q", header[1])
	}
	if header[2] != "z_last" {
		t.Errorf("Third column should be 'z_last', got %q", header[2])
	}
}

func TestCSVFormatter_TypeFormatting(t *testing.T) {
	rows := []map[string]interface{}{
		{
			"string": "alice",
			"int":    int64(42),
			"float":  float64(3.14),
			"bool":   true,
			"nil":    nil,
		},
	}

	var buf bytes.Buffer
	formatter := NewCSVFormatter(&buf)

	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 records (header + data), got %d", len(records))
	}

	dataRow := records[1]

	// Find value by header
	header := records[0]
	getValue := func(col string) string {
		for i, h := range header {
			if h == col {
				return dataRow[i]
			}
		}
		return ""
	}

	if getValue("string") != "alice" {
		t.Errorf("string column should be 'alice', got %q", getValue("string"))
	}
	if getValue("int") != "42" {
		t.Errorf("int column should be '42', got %q", getValue("int"))
	}
	if getValue("float") != "3.14" {
		t.Errorf("float column should be '3.14', got %q", getValue("float"))
	}
	if getValue("bool") != "true" {
		t.Errorf("bool column should be 'true', got %q", getValue("bool"))
	}
	if getValue("nil") != "" {
		t.Errorf("nil column should be empty, got %q", getValue("nil"))
	}
}

func TestCSVFormatter_SpecialCharacters(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice, Bob", "quote": `He said "hello"`, "newline": "line1\nline2"},
	}

	var buf bytes.Buffer
	formatter := NewCSVFormatter(&buf)

	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	// CSV library should handle escaping automatically
	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV with special characters: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// Verify the values are correctly escaped and unescaped
	dataRow := records[1]
	header := records[0]

	getValue := func(col string) string {
		for i, h := range header {
			if h == col {
				return dataRow[i]
			}
		}
		return ""
	}

	// The CSV reader should unescape these correctly
	if getValue("name") != "Alice, Bob" {
		t.Errorf("comma in value not handled correctly")
	}
	if getValue("quote") != `He said "hello"` {
		t.Errorf("quotes in value not handled correctly")
	}
}

func TestCSVFormatter_SetOutput(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	formatter := NewCSVFormatter(&buf1)

	rows := []map[string]interface{}{
		{"id": int64(1), "name": "alice"},
	}

	// Write to first buffer
	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	if buf1.Len() == 0 {
		t.Error("First buffer should have content")
	}

	// Change output and write again
	formatter.SetOutput(&buf2)
	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	if buf2.Len() == 0 {
		t.Error("Second buffer should have content")
	}
}
