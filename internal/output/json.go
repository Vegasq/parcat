package output

import (
	"encoding/json"
	"io"
)

// JSONFormatter outputs rows as JSON Lines format
type JSONFormatter struct {
	writer io.Writer
}

// NewJSONFormatter creates a new JSON Lines formatter
func NewJSONFormatter(w io.Writer) *JSONFormatter {
	return &JSONFormatter{writer: w}
}

// SetOutput sets the output writer
func (j *JSONFormatter) SetOutput(w io.Writer) {
	j.writer = w
}

// Format writes rows as JSON Lines (one JSON object per line)
func (j *JSONFormatter) Format(rows []map[string]interface{}) error {
	encoder := json.NewEncoder(j.writer)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return nil
}
