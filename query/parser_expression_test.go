package query

import (
	"strings"
	"testing"
)

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

func TestParser_InOperator(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "IN with strings",
			query:   "select * from data.parquet where status IN ('active', 'pending', 'complete')",
			wantErr: false,
		},
		{
			name:    "IN with numbers",
			query:   "select * from data.parquet where age IN (25, 30, 35)",
			wantErr: false,
		},
		{
			name:    "NOT IN",
			query:   "select * from data.parquet where status NOT IN ('deleted', 'archived')",
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
				t.Errorf("Expected filter to be set")
			}
		})
	}
}

func TestParser_LikeOperator(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "LIKE with prefix wildcard",
			query:   "select * from data.parquet where name LIKE 'alice%'",
			wantErr: false,
		},
		{
			name:    "LIKE with suffix wildcard",
			query:   "select * from data.parquet where name LIKE '%smith'",
			wantErr: false,
		},
		{
			name:    "LIKE with both wildcards",
			query:   "select * from data.parquet where email LIKE '%@example.com%'",
			wantErr: false,
		},
		{
			name:    "NOT LIKE",
			query:   "select * from data.parquet where name NOT LIKE 'test%'",
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
				t.Errorf("Expected filter to be set")
			}
		})
	}
}

func TestParser_BetweenOperator(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "BETWEEN with numbers",
			query:   "select * from data.parquet where age BETWEEN 25 AND 40",
			wantErr: false,
		},
		{
			name:    "BETWEEN with strings",
			query:   "select * from data.parquet where name BETWEEN 'A' AND 'M'",
			wantErr: false,
		},
		{
			name:    "NOT BETWEEN",
			query:   "select * from data.parquet where age NOT BETWEEN 18 AND 65",
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
				t.Errorf("Expected filter to be set")
			}
		})
	}
}

func TestParser_IsNullOperator(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "IS NULL",
			query:   "select * from data.parquet where email IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT NULL",
			query:   "select * from data.parquet where email IS NOT NULL",
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
				t.Errorf("Expected filter to be set")
			}
		})
	}
}

func TestWindowFrameParsing_Errors(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []Token
		wantErr string
	}{
		{
			name: "missing ROWS/RANGE",
			tokens: []Token{
				{Type: TokenBetween, Value: "BETWEEN"},
			},
			wantErr: "expected ROWS or RANGE",
		},
		{
			name: "UNBOUNDED without PRECEDING/FOLLOWING",
			tokens: []Token{
				{Type: TokenRows, Value: "ROWS"},
				{Type: TokenIdent, Value: "UNBOUNDED"},
				{Type: TokenEOF, Value: ""},
			},
			wantErr: "expected PRECEDING or FOLLOWING after UNBOUNDED",
		},
		{
			name: "CURRENT without ROW",
			tokens: []Token{
				{Type: TokenRows, Value: "ROWS"},
				{Type: TokenIdent, Value: "CURRENT"},
				{Type: TokenEOF, Value: ""},
			},
			wantErr: "expected ROW after CURRENT",
		},
		{
			name: "offset without PRECEDING/FOLLOWING",
			tokens: []Token{
				{Type: TokenRows, Value: "ROWS"},
				{Type: TokenNumber, Value: "5"},
				{Type: TokenEOF, Value: ""},
			},
			wantErr: "expected PRECEDING or FOLLOWING after offset",
		},
		{
			name: "BETWEEN without AND",
			tokens: []Token{
				{Type: TokenRows, Value: "ROWS"},
				{Type: TokenBetween, Value: "BETWEEN"},
				{Type: TokenIdent, Value: "UNBOUNDED"},
				{Type: TokenIdent, Value: "PRECEDING"},
				{Type: TokenEOF, Value: ""},
			},
			wantErr: "expected AND in BETWEEN frame clause",
		},
		{
			name: "invalid bound",
			tokens: []Token{
				{Type: TokenRows, Value: "ROWS"},
				{Type: TokenIdent, Value: "INVALID"},
			},
			wantErr: "invalid frame bound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.tokens)
			_, err := p.parseWindowFrame()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			// Check that error message contains the expected substring
			if tt.wantErr != "" {
				errStr := err.Error()
				if !containsSubstring(errStr, tt.wantErr) {
					t.Errorf("error = %v, want error containing %v", errStr, tt.wantErr)
				}
			}
		})
	}
}

func TestParseFrameBound_Direct(t *testing.T) {
	tests := []struct {
		name       string
		tokens     []Token
		wantType   BoundType
		wantOffset int64
		wantErr    bool
	}{
		{
			name: "UNBOUNDED PRECEDING",
			tokens: []Token{
				{Type: TokenIdent, Value: "UNBOUNDED"},
				{Type: TokenIdent, Value: "PRECEDING"},
				{Type: TokenEOF, Value: ""},
			},
			wantType: BoundUnboundedPreceding,
			wantErr:  false,
		},
		{
			name: "UNBOUNDED FOLLOWING",
			tokens: []Token{
				{Type: TokenIdent, Value: "UNBOUNDED"},
				{Type: TokenIdent, Value: "FOLLOWING"},
				{Type: TokenEOF, Value: ""},
			},
			wantType: BoundUnboundedFollowing,
			wantErr:  false,
		},
		{
			name: "CURRENT ROW",
			tokens: []Token{
				{Type: TokenIdent, Value: "CURRENT"},
				{Type: TokenIdent, Value: "ROW"},
				{Type: TokenEOF, Value: ""},
			},
			wantType: BoundCurrentRow,
			wantErr:  false,
		},
		{
			name: "5 PRECEDING",
			tokens: []Token{
				{Type: TokenNumber, Value: "5"},
				{Type: TokenIdent, Value: "PRECEDING"},
				{Type: TokenEOF, Value: ""},
			},
			wantType:   BoundOffsetPreceding,
			wantOffset: 5,
			wantErr:    false,
		},
		{
			name: "10 FOLLOWING",
			tokens: []Token{
				{Type: TokenNumber, Value: "10"},
				{Type: TokenIdent, Value: "FOLLOWING"},
				{Type: TokenEOF, Value: ""},
			},
			wantType:   BoundOffsetFollowing,
			wantOffset: 10,
			wantErr:    false,
		},
		{
			name: "lowercase unbounded preceding",
			tokens: []Token{
				{Type: TokenIdent, Value: "unbounded"},
				{Type: TokenIdent, Value: "preceding"},
				{Type: TokenEOF, Value: ""},
			},
			wantType: BoundUnboundedPreceding,
			wantErr:  false,
		},
		{
			name: "mixed case CURRENT row",
			tokens: []Token{
				{Type: TokenIdent, Value: "CuRrEnT"},
				{Type: TokenIdent, Value: "RoW"},
				{Type: TokenEOF, Value: ""},
			},
			wantType: BoundCurrentRow,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.tokens)
			bound, err := p.parseFrameBound()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFrameBound() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if bound.Type != tt.wantType {
					t.Errorf("bound type = %v, want %v", bound.Type, tt.wantType)
				}
				if tt.wantOffset != 0 && bound.Offset != tt.wantOffset {
					t.Errorf("bound offset = %v, want %v", bound.Offset, tt.wantOffset)
				}
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}
