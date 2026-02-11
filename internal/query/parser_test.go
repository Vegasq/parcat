package query

import (
	"testing"
)

func TestParser_SimpleQuery(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantTable string
		wantErr   bool
	}{
		{
			name:      "basic select",
			query:     "select * from data.parquet",
			wantTable: "data.parquet",
			wantErr:   false,
		},
		{
			name:      "with file path",
			query:     "select * from testdata/simple.parquet",
			wantTable: "testdata/simple.parquet",
			wantErr:   false,
		},
		{
			name:      "quoted table name",
			query:     `select * from "my file.parquet"`,
			wantTable: "my file.parquet",
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
			if !tt.wantErr && q.TableName != tt.wantTable {
				t.Errorf("Parse() table = %v, want %v", q.TableName, tt.wantTable)
			}
		})
	}
}

func TestParser_WhereClause(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple comparison",
			query:   "select * from data.parquet where age > 30",
			wantErr: false,
		},
		{
			name:    "string comparison",
			query:   "select * from data.parquet where name = 'alice'",
			wantErr: false,
		},
		{
			name:    "boolean comparison",
			query:   "select * from data.parquet where active = true",
			wantErr: false,
		},
		{
			name:    "AND expression",
			query:   "select * from data.parquet where age > 30 AND active = true",
			wantErr: false,
		},
		{
			name:    "OR expression",
			query:   "select * from data.parquet where age > 30 OR premium = true",
			wantErr: false,
		},
		{
			name:    "complex nested expression",
			query:   "select * from data.parquet where age > 30 AND active = true OR premium = true",
			wantErr: false,
		},
		{
			name:    "all comparison operators",
			query:   "select * from data.parquet where a = 1 AND b != 2 AND c < 3 AND d > 4 AND e <= 5 AND f >= 6",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q.Filter == nil {
				t.Error("Parse() filter is nil, expected non-nil")
			}
		})
	}
}

func TestParser_Errors(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "missing SELECT",
			query: "from data.parquet where age > 30",
		},
		{
			name:  "missing FROM",
			query: "select * where age > 30",
		},
		{
			name:  "missing table name",
			query: "select * from where age > 30",
		},
		{
			name:  "invalid SELECT target",
			query: "select age from data.parquet",
		},
		{
			name:  "missing comparison value",
			query: "select * from data.parquet where age >",
		},
		{
			name:  "missing column name",
			query: "select * from data.parquet where > 30",
		},
		{
			name:  "incomplete AND",
			query: "select * from data.parquet where age > 30 AND",
		},
		{
			name:  "incomplete OR",
			query: "select * from data.parquet where age > 30 OR",
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

func TestParser_OperatorPrecedence(t *testing.T) {
	// AND should bind tighter than OR
	// a OR b AND c should parse as: a OR (b AND c)
	query := "select * from data.parquet where a = 1 OR b = 2 AND c = 3"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check that the root is an OR expression
	binExpr, ok := q.Filter.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", q.Filter)
	}
	if binExpr.Operator != TokenOr {
		t.Errorf("expected root operator to be OR, got %v", binExpr.Operator)
	}

	// Check that the right side is an AND expression
	rightBin, ok := binExpr.Right.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected right side to be BinaryExpr, got %T", binExpr.Right)
	}
	if rightBin.Operator != TokenAnd {
		t.Errorf("expected right operator to be AND, got %v", rightBin.Operator)
	}
}

func TestComparisonExpr_String(t *testing.T) {
	query := "select * from data.parquet where name = 'alice'"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	comp, ok := q.Filter.(*ComparisonExpr)
	if !ok {
		t.Fatalf("expected ComparisonExpr, got %T", q.Filter)
	}

	if comp.Column != "name" {
		t.Errorf("expected column 'name', got %q", comp.Column)
	}
	if comp.Operator != TokenEqual {
		t.Errorf("expected operator TokenEqual, got %v", comp.Operator)
	}
	if comp.Value != "alice" {
		t.Errorf("expected value 'alice', got %v", comp.Value)
	}
}

func TestComparisonExpr_Number(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantValue interface{}
	}{
		{
			name:      "integer",
			query:     "select * from data.parquet where age = 30",
			wantValue: int64(30),
		},
		{
			name:      "float",
			query:     "select * from data.parquet where score = 95.5",
			wantValue: float64(95.5),
		},
		{
			name:      "negative integer",
			query:     "select * from data.parquet where temp = -10",
			wantValue: int64(-10),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			comp, ok := q.Filter.(*ComparisonExpr)
			if !ok {
				t.Fatalf("expected ComparisonExpr, got %T", q.Filter)
			}

			if comp.Value != tt.wantValue {
				t.Errorf("expected value %v (%T), got %v (%T)", tt.wantValue, tt.wantValue, comp.Value, comp.Value)
			}
		})
	}
}
