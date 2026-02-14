package query

import (
	"testing"
)

// TestEndToEndProjection tests the complete flow: parse -> filter -> project
func TestEndToEndProjection(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		rows    []map[string]interface{}
		want    []map[string]interface{}
		wantErr bool
	}{
		{
			name:  "select all columns",
			query: "select * from data.parquet",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			wantErr: false,
		},
		{
			name:  "select specific columns",
			query: "select name, age from data.parquet",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30, "city": "NYC"},
				{"name": "bob", "age": 25, "city": "LA"},
			},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			wantErr: false,
		},
		{
			name:  "select with alias",
			query: "select name as user_name, age as years from data.parquet",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
			},
			want: []map[string]interface{}{
				{"user_name": "alice", "years": 30},
			},
			wantErr: false,
		},
		{
			name:  "select with where clause",
			query: "select name, age from data.parquet where age > 25",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
				{"name": "charlie", "age": 35},
			},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "charlie", "age": 35},
			},
			wantErr: false,
		},
		{
			name:  "select single column with where",
			query: "select name from data.parquet where age >= 30",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
				{"name": "charlie", "age": 35},
			},
			want: []map[string]interface{}{
				{"name": "alice"},
				{"name": "charlie"},
			},
			wantErr: false,
		},
		{
			name:  "select with alias and where",
			query: "select name as user from data.parquet where age < 30",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			want: []map[string]interface{}{
				{"user": "bob"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Apply filter
			rows := tt.rows
			if q.Filter != nil {
				rows, err = ApplyFilter(rows, q.Filter)
				if err != nil {
					t.Errorf("ApplyFilter() error = %v", err)
					return
				}
			}

			// Apply select list
			if len(q.SelectList) > 0 {
				rows, err = ApplySelectList(rows, q.SelectList)
				if err != nil {
					t.Errorf("ApplySelectList() error = %v", err)
					return
				}
			}

			// Verify results
			if len(rows) != len(tt.want) {
				t.Errorf("got %d rows, want %d rows", len(rows), len(tt.want))
				return
			}

			for i, gotRow := range rows {
				wantRow := tt.want[i]
				if len(gotRow) != len(wantRow) {
					t.Errorf("Row %d: got %d columns, want %d columns", i, len(gotRow), len(wantRow))
					t.Logf("Got: %v", gotRow)
					t.Logf("Want: %v", wantRow)
					continue
				}

				for key, wantVal := range wantRow {
					gotVal, exists := gotRow[key]
					if !exists {
						t.Errorf("Row %d: column %q not found in result", i, key)
						continue
					}
					if gotVal != wantVal {
						t.Errorf("Row %d: column %q = %v, want %v", i, key, gotVal, wantVal)
					}
				}
			}
		})
	}
}
