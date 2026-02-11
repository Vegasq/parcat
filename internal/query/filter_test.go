package query

import (
	"testing"
)

func TestCompare_Numbers(t *testing.T) {
	tests := []struct {
		name     string
		left     interface{}
		operator TokenType
		right    interface{}
		want     bool
	}{
		// Integer comparisons
		{"int equal", int64(30), TokenEqual, int64(30), true},
		{"int not equal", int64(30), TokenNotEqual, int64(25), true},
		{"int less", int32(25), TokenLess, int64(30), true},
		{"int greater", int64(35), TokenGreater, int32(30), true},
		{"int less equal same", int64(30), TokenLessEqual, int64(30), true},
		{"int less equal less", int64(25), TokenLessEqual, int64(30), true},
		{"int greater equal same", int64(30), TokenGreaterEqual, int64(30), true},
		{"int greater equal greater", int64(35), TokenGreaterEqual, int64(30), true},

		// Float comparisons
		{"float equal", float64(3.14), TokenEqual, float64(3.14), true},
		{"float not equal", float64(3.14), TokenNotEqual, float64(2.71), true},
		{"float less", float64(2.5), TokenLess, float64(3.0), true},
		{"float greater", float64(3.5), TokenGreater, float64(3.0), true},

		// Mixed int/float comparisons
		{"int vs float equal", int64(30), TokenEqual, float64(30.0), true},
		{"float vs int equal", float64(30.0), TokenEqual, int64(30), true},
		{"int vs float less", int32(25), TokenLess, float64(30.5), true},
		{"float vs int greater", float64(35.5), TokenGreater, int64(30), true},

		// Negative results
		{"int not equal same", int64(30), TokenNotEqual, int64(30), false},
		{"int less wrong", int64(35), TokenLess, int64(30), false},
		{"int greater wrong", int64(25), TokenGreater, int64(30), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare(tt.left, tt.operator, tt.right)
			if err != nil {
				t.Errorf("compare() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("compare(%v, %v, %v) = %v, want %v", tt.left, tt.operator, tt.right, got, tt.want)
			}
		})
	}
}

func TestCompare_Strings(t *testing.T) {
	tests := []struct {
		name     string
		left     string
		operator TokenType
		right    string
		want     bool
	}{
		{"equal", "alice", TokenEqual, "alice", true},
		{"not equal", "alice", TokenNotEqual, "bob", true},
		{"less", "alice", TokenLess, "bob", true},
		{"greater", "bob", TokenGreater, "alice", true},
		{"less equal same", "alice", TokenLessEqual, "alice", true},
		{"less equal less", "alice", TokenLessEqual, "bob", true},
		{"greater equal same", "alice", TokenGreaterEqual, "alice", true},
		{"greater equal greater", "bob", TokenGreaterEqual, "alice", true},

		// Case sensitivity
		{"case sensitive not equal", "Alice", TokenEqual, "alice", false},
		{"case sensitive equal", "alice", TokenEqual, "alice", true},

		// Negative results
		{"not equal same", "alice", TokenNotEqual, "alice", false},
		{"less wrong", "bob", TokenLess, "alice", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare(tt.left, tt.operator, tt.right)
			if err != nil {
				t.Errorf("compare() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("compare(%q, %v, %q) = %v, want %v", tt.left, tt.operator, tt.right, got, tt.want)
			}
		})
	}
}

func TestCompare_Booleans(t *testing.T) {
	tests := []struct {
		name     string
		left     bool
		operator TokenType
		right    bool
		want     bool
	}{
		{"true equals true", true, TokenEqual, true, true},
		{"false equals false", false, TokenEqual, false, true},
		{"true not equals false", true, TokenNotEqual, false, true},
		{"false not equals true", false, TokenNotEqual, true, true},
		{"true not equals true", true, TokenNotEqual, true, false},
		{"false not equals false", false, TokenNotEqual, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare(tt.left, tt.operator, tt.right)
			if err != nil {
				t.Errorf("compare() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("compare(%v, %v, %v) = %v, want %v", tt.left, tt.operator, tt.right, got, tt.want)
			}
		})
	}
}

func TestCompare_Nil(t *testing.T) {
	tests := []struct {
		name     string
		left     interface{}
		operator TokenType
		right    interface{}
		want     bool
	}{
		{"nil equals nil", nil, TokenEqual, nil, true},
		{"nil not equals value", nil, TokenNotEqual, "alice", true},
		{"value not equals nil", "alice", TokenNotEqual, nil, true},
		{"nil not equals nil", nil, TokenNotEqual, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare(tt.left, tt.operator, tt.right)
			if err != nil {
				t.Errorf("compare() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("compare(%v, %v, %v) = %v, want %v", tt.left, tt.operator, tt.right, got, tt.want)
			}
		})
	}
}

func TestCompare_TypeMismatch(t *testing.T) {
	tests := []struct {
		name  string
		left  interface{}
		right interface{}
	}{
		{"string vs number", "alice", int64(30)},
		{"number vs string", int64(30), "alice"},
		{"boolean vs number", true, int64(30)},
		{"number vs boolean", int64(30), true},
		{"string vs boolean", "true", true},
		{"boolean vs string", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compare(tt.left, TokenEqual, tt.right)
			if err == nil {
				t.Errorf("compare(%v, =, %v) expected error for type mismatch", tt.left, tt.right)
			}
		})
	}
}

func TestComparisonExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name string
		expr ComparisonExpr
		row  map[string]interface{}
		want bool
	}{
		{
			name: "simple match",
			expr: ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
			row:  map[string]interface{}{"age": int32(35)},
			want: true,
		},
		{
			name: "simple no match",
			expr: ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
			row:  map[string]interface{}{"age": int32(25)},
			want: false,
		},
		{
			name: "string match",
			expr: ComparisonExpr{Column: "name", Operator: TokenEqual, Value: "alice"},
			row:  map[string]interface{}{"name": "alice"},
			want: true,
		},
		{
			name: "column not found",
			expr: ComparisonExpr{Column: "missing", Operator: TokenEqual, Value: "alice"},
			row:  map[string]interface{}{"name": "alice"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if err != nil {
				t.Errorf("Evaluate() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name string
		expr BinaryExpr
		row  map[string]interface{}
		want bool
	}{
		{
			name: "AND both true",
			expr: BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
				Operator: TokenAnd,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			row:  map[string]interface{}{"age": int32(35), "active": true},
			want: true,
		},
		{
			name: "AND one false",
			expr: BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
				Operator: TokenAnd,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			row:  map[string]interface{}{"age": int32(25), "active": true},
			want: false,
		},
		{
			name: "OR both true",
			expr: BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
				Operator: TokenOr,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			row:  map[string]interface{}{"age": int32(35), "active": true},
			want: true,
		},
		{
			name: "OR one true",
			expr: BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
				Operator: TokenOr,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			row:  map[string]interface{}{"age": int32(25), "active": true},
			want: true,
		},
		{
			name: "OR both false",
			expr: BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
				Operator: TokenOr,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			row:  map[string]interface{}{"age": int32(25), "active": false},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if err != nil {
				t.Errorf("Evaluate() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyFilter(t *testing.T) {
	rows := []map[string]interface{}{
		{"id": int64(1), "name": "alice", "age": int32(30), "active": true},
		{"id": int64(2), "name": "bob", "age": int32(25), "active": false},
		{"id": int64(3), "name": "charlie", "age": int32(35), "active": true},
	}

	tests := []struct {
		name      string
		filter    Expression
		wantCount int
	}{
		{
			name:      "nil filter returns all",
			filter:    nil,
			wantCount: 3,
		},
		{
			name:      "age > 30 returns 1",
			filter:    &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(30)},
			wantCount: 1,
		},
		{
			name:      "age >= 30 returns 2",
			filter:    &ComparisonExpr{Column: "age", Operator: TokenGreaterEqual, Value: int64(30)},
			wantCount: 2,
		},
		{
			name:      "active = true returns 2",
			filter:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			wantCount: 2,
		},
		{
			name: "age > 25 AND active = true returns 2",
			filter: &BinaryExpr{
				Left:     &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(25)},
				Operator: TokenAnd,
				Right:    &ComparisonExpr{Column: "active", Operator: TokenEqual, Value: true},
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplyFilter(rows, tt.filter)
			if err != nil {
				t.Errorf("ApplyFilter() error = %v", err)
				return
			}
			if len(got) != tt.wantCount {
				t.Errorf("ApplyFilter() returned %d rows, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestGetColumnNames(t *testing.T) {
	tests := []struct {
		name string
		rows []map[string]interface{}
		want int
	}{
		{
			name: "empty rows",
			rows: []map[string]interface{}{},
			want: 0,
		},
		{
			name: "single row",
			rows: []map[string]interface{}{
				{"id": 1, "name": "alice", "age": 30},
			},
			want: 3,
		},
		{
			name: "multiple rows same columns",
			rows: []map[string]interface{}{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			},
			want: 2,
		},
		{
			name: "multiple rows different columns",
			rows: []map[string]interface{}{
				{"id": 1, "name": "alice"},
				{"id": 2, "age": 30},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetColumnNames(tt.rows)
			if len(got) != tt.want {
				t.Errorf("GetColumnNames() returned %d columns, want %d", len(got), tt.want)
			}
		})
	}
}
