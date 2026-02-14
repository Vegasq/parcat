package query

import (
	"testing"
)

func TestParser_SelectList_SingleColumn(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantColumn string
		wantErr    bool
	}{
		{
			name:       "single column",
			query:      "select name from data.parquet",
			wantColumn: "name",
			wantErr:    false,
		},
		{
			name:       "select star",
			query:      "select * from data.parquet",
			wantColumn: "*",
			wantErr:    false,
		},
		{
			name:       "column with underscore",
			query:      "select user_id from data.parquet",
			wantColumn: "user_id",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(q.SelectList) != 1 {
					t.Errorf("expected 1 select item, got %d", len(q.SelectList))
					return
				}
				colRef, ok := q.SelectList[0].Expr.(*ColumnRef)
				if !ok {
					t.Errorf("expected ColumnRef, got %T", q.SelectList[0].Expr)
					return
				}
				if colRef.Column != tt.wantColumn {
					t.Errorf("expected column %q, got %q", tt.wantColumn, colRef.Column)
				}
			}
		})
	}
}

func TestParser_SelectList_MultipleColumns(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantColumns []string
		wantErr     bool
	}{
		{
			name:        "two columns",
			query:       "select name, age from data.parquet",
			wantColumns: []string{"name", "age"},
			wantErr:     false,
		},
		{
			name:        "three columns",
			query:       "select name, age, active from data.parquet",
			wantColumns: []string{"name", "age", "active"},
			wantErr:     false,
		},
		{
			name:        "columns with underscores",
			query:       "select user_id, user_name, is_active from data.parquet",
			wantColumns: []string{"user_id", "user_name", "is_active"},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(q.SelectList) != len(tt.wantColumns) {
					t.Errorf("expected %d select items, got %d", len(tt.wantColumns), len(q.SelectList))
					return
				}
				for i, wantCol := range tt.wantColumns {
					colRef, ok := q.SelectList[i].Expr.(*ColumnRef)
					if !ok {
						t.Errorf("item %d: expected ColumnRef, got %T", i, q.SelectList[i].Expr)
						continue
					}
					if colRef.Column != wantCol {
						t.Errorf("item %d: expected column %q, got %q", i, wantCol, colRef.Column)
					}
				}
			}
		})
	}
}

func TestParser_SelectList_WithAlias(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantAlias string
		wantErr   bool
	}{
		{
			name:      "alias with AS",
			query:     "select name AS user_name from data.parquet",
			wantAlias: "user_name",
			wantErr:   false,
		},
		{
			name:      "alias with as (lowercase)",
			query:     "select name as user_name from data.parquet",
			wantAlias: "user_name",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(q.SelectList) != 1 {
					t.Errorf("expected 1 select item, got %d", len(q.SelectList))
					return
				}
				if q.SelectList[0].Alias != tt.wantAlias {
					t.Errorf("expected alias %q, got %q", tt.wantAlias, q.SelectList[0].Alias)
				}
			}
		})
	}
}

func TestParser_SelectList_WithFunctionCall(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		wantFuncName string
		wantArgCount int
		wantErr      bool
	}{
		{
			name:         "function with one arg",
			query:        "select UPPER(name) from data.parquet",
			wantFuncName: "UPPER",
			wantArgCount: 1,
			wantErr:      false,
		},
		{
			name:         "function with no args",
			query:        "select NOW() from data.parquet",
			wantFuncName: "NOW",
			wantArgCount: 0,
			wantErr:      false,
		},
		{
			name:         "function with multiple args",
			query:        "select CONCAT(first_name, last_name) from data.parquet",
			wantFuncName: "CONCAT",
			wantArgCount: 2,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(q.SelectList) != 1 {
					t.Errorf("expected 1 select item, got %d", len(q.SelectList))
					return
				}
				funcCall, ok := q.SelectList[0].Expr.(*FunctionCall)
				if !ok {
					t.Errorf("expected FunctionCall, got %T", q.SelectList[0].Expr)
					return
				}
				if funcCall.Name != tt.wantFuncName {
					t.Errorf("expected function name %q, got %q", tt.wantFuncName, funcCall.Name)
				}
				if len(funcCall.Args) != tt.wantArgCount {
					t.Errorf("expected %d args, got %d", tt.wantArgCount, len(funcCall.Args))
				}
			}
		})
	}
}

func TestParser_SelectList_MixedColumnsAndFunctions(t *testing.T) {
	query := "select name, UPPER(email), age AS user_age from data.parquet"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(q.SelectList) != 3 {
		t.Fatalf("expected 3 select items, got %d", len(q.SelectList))
	}

	// First item: name (column)
	if colRef, ok := q.SelectList[0].Expr.(*ColumnRef); !ok {
		t.Errorf("item 0: expected ColumnRef, got %T", q.SelectList[0].Expr)
	} else if colRef.Column != "name" {
		t.Errorf("item 0: expected column 'name', got %q", colRef.Column)
	}

	// Second item: UPPER(email) (function)
	if funcCall, ok := q.SelectList[1].Expr.(*FunctionCall); !ok {
		t.Errorf("item 1: expected FunctionCall, got %T", q.SelectList[1].Expr)
	} else if funcCall.Name != "UPPER" {
		t.Errorf("item 1: expected function 'UPPER', got %q", funcCall.Name)
	}

	// Third item: age AS user_age (column with alias)
	if colRef, ok := q.SelectList[2].Expr.(*ColumnRef); !ok {
		t.Errorf("item 2: expected ColumnRef, got %T", q.SelectList[2].Expr)
	} else if colRef.Column != "age" {
		t.Errorf("item 2: expected column 'age', got %q", colRef.Column)
	}
	if q.SelectList[2].Alias != "user_age" {
		t.Errorf("item 2: expected alias 'user_age', got %q", q.SelectList[2].Alias)
	}
}

func TestParser_SelectList_WithWhereClause(t *testing.T) {
	query := "select name, age from data.parquet where age > 30"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check SELECT list
	if len(q.SelectList) != 2 {
		t.Errorf("expected 2 select items, got %d", len(q.SelectList))
	}

	// Check WHERE clause exists
	if q.Filter == nil {
		t.Error("expected non-nil filter")
	}
}

func TestParser_SelectList_Errors(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "missing column after comma",
			query: "select name, from data.parquet",
		},
		{
			name:  "trailing comma",
			query: "select name, age, from data.parquet",
		},
		{
			name:  "missing closing paren in function",
			query: "select UPPER(name from data.parquet",
		},
		{
			name:  "empty select list",
			query: "select from data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.query)
			if err == nil {
				t.Errorf("Parse() expected error for query: %s", tt.query)
			}
		})
	}
}

func TestColumnRef_EvaluateSelect(t *testing.T) {
	row := map[string]interface{}{
		"name": "alice",
		"age":  int64(30),
	}

	tests := []struct {
		name      string
		column    string
		wantValue interface{}
		wantErr   bool
	}{
		{
			name:      "existing column",
			column:    "name",
			wantValue: "alice",
			wantErr:   false,
		},
		{
			name:      "star returns full row",
			column:    "*",
			wantValue: row,
			wantErr:   false,
		},
		{
			name:      "non-existent column",
			column:    "missing",
			wantValue: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colRef := &ColumnRef{Column: tt.column}
			value, err := colRef.EvaluateSelect(row)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvaluateSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// For * case, compare the entire row
				if tt.column == "*" {
					rowMap, ok := value.(map[string]interface{})
					if !ok {
						t.Errorf("expected map for *, got %T", value)
						return
					}
					if len(rowMap) != len(row) {
						t.Errorf("expected row with %d columns, got %d", len(row), len(rowMap))
					}
				} else if value != tt.wantValue {
					t.Errorf("expected value %v, got %v", tt.wantValue, value)
				}
			}
		})
	}
}
