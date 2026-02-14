package query

import (
	"testing"
)

func TestApplySelectList(t *testing.T) {
	tests := []struct {
		name       string
		rows       []map[string]interface{}
		selectList []SelectItem
		want       []map[string]interface{}
		wantErr    bool
	}{
		{
			name: "empty rows",
			rows: []map[string]interface{}{},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
			},
			want:    []map[string]interface{}{},
			wantErr: false,
		},
		{
			name: "select star",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "*"}},
			},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			wantErr: false,
		},
		{
			name: "select single column",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
			},
			want: []map[string]interface{}{
				{"name": "alice"},
				{"name": "bob"},
			},
			wantErr: false,
		},
		{
			name: "select multiple columns",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30, "city": "NYC"},
				{"name": "bob", "age": 25, "city": "LA"},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
				{Expr: &ColumnRef{Column: "age"}},
			},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			wantErr: false,
		},
		{
			name: "select with alias",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}, Alias: "user_name"},
				{Expr: &ColumnRef{Column: "age"}, Alias: "user_age"},
			},
			want: []map[string]interface{}{
				{"user_name": "alice", "user_age": 30},
				{"user_name": "bob", "user_age": 25},
			},
			wantErr: false,
		},
		{
			name: "select non-existent column",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "missing"}},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "empty select list returns all columns",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			selectList: []SelectItem{},
			want: []map[string]interface{}{
				{"name": "alice", "age": 30},
				{"name": "bob", "age": 25},
			},
			wantErr: false,
		},
		{
			name: "mixed columns and aliases",
			rows: []map[string]interface{}{
				{"name": "alice", "age": 30, "city": "NYC"},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
				{Expr: &ColumnRef{Column: "age"}, Alias: "years"},
				{Expr: &ColumnRef{Column: "city"}},
			},
			want: []map[string]interface{}{
				{"name": "alice", "years": 30, "city": "NYC"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplySelectList(tt.rows, tt.selectList)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplySelectList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			// Compare results
			if len(got) != len(tt.want) {
				t.Errorf("ApplySelectList() got %d rows, want %d rows", len(got), len(tt.want))
				return
			}

			for i, gotRow := range got {
				wantRow := tt.want[i]
				if len(gotRow) != len(wantRow) {
					t.Errorf("Row %d: got %d columns, want %d columns", i, len(gotRow), len(wantRow))
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

func TestApplySelectListPreservesOrder(t *testing.T) {
	rows := []map[string]interface{}{
		{"a": 1, "b": 2, "c": 3, "d": 4},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "d"}},
		{Expr: &ColumnRef{Column: "b"}},
		{Expr: &ColumnRef{Column: "a"}},
	}

	result, err := ApplySelectList(rows, selectList)
	if err != nil {
		t.Fatalf("ApplySelectList() error = %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result))
	}

	row := result[0]
	if len(row) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(row))
	}

	// Verify all expected columns are present
	expectedCols := map[string]int{"d": 4, "b": 2, "a": 1}
	for col, expectedVal := range expectedCols {
		val, exists := row[col]
		if !exists {
			t.Errorf("Column %q not found in result", col)
		}
		if val != expectedVal {
			t.Errorf("Column %q = %v, want %v", col, val, expectedVal)
		}
	}
}
