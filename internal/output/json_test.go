package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONFormatter_Format(t *testing.T) {
	tests := []struct {
		name    string
		rows    []map[string]interface{}
		wantErr bool
	}{
		{
			name: "empty rows",
			rows: []map[string]interface{}{},
		},
		{
			name: "single row",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "age": int32(30)},
			},
		},
		{
			name: "multiple rows",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "age": int32(30)},
				{"id": int64(2), "name": "bob", "age": int32(25)},
			},
		},
		{
			name: "various types",
			rows: []map[string]interface{}{
				{
					"id":     int64(1),
					"name":   "alice",
					"age":    int32(30),
					"score":  float64(95.5),
					"active": true,
				},
			},
		},
		{
			name: "nil values",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": nil, "age": int32(30)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			formatter := NewJSONFormatter(&buf)

			err := formatter.Format(tt.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("Format() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Verify output is valid JSON Lines
			output := buf.String()
			lines := strings.Split(strings.TrimSpace(output), "\n")

			// Empty rows should produce no output
			if len(tt.rows) == 0 {
				if output != "" {
					t.Errorf("Format() output should be empty for empty rows, got %q", output)
				}
				return
			}

			if len(lines) != len(tt.rows) {
				t.Errorf("Format() produced %d lines, want %d", len(lines), len(tt.rows))
			}

			// Verify each line is valid JSON
			for i, line := range lines {
				var decoded map[string]interface{}
				if err := json.Unmarshal([]byte(line), &decoded); err != nil {
					t.Errorf("Format() line %d is not valid JSON: %v", i, err)
				}
			}
		})
	}
}

func TestJSONFormatter_SetOutput(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	formatter := NewJSONFormatter(&buf1)

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

func TestJSONFormatter_OutputFormat(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1), "name": "alice", "active": true},
		{"id": int64(2), "name": "bob", "active": false},
	}

	var buf bytes.Buffer
	formatter := NewJSONFormatter(&buf)

	if err := formatter.Format(rows); err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Verify JSON Lines format (one object per line, no trailing comma)
	for i, line := range lines {
		// Should not end with comma
		if strings.HasSuffix(line, ",") {
			t.Errorf("Line %d should not end with comma", i)
		}

		// Should be valid JSON object
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i, err)
		}

		// Verify content
		if obj["id"] == nil {
			t.Errorf("Line %d missing 'id' field", i)
		}
		if obj["name"] == nil {
			t.Errorf("Line %d missing 'name' field", i)
		}
		if obj["active"] == nil {
			t.Errorf("Line %d missing 'active' field", i)
		}
	}
}
