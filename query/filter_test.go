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
		name    string
		expr    ComparisonExpr
		row     map[string]interface{}
		want    bool
		wantErr bool
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
			name:    "column not found",
			expr:    ComparisonExpr{Column: "missing", Operator: TokenEqual, Value: "alice"},
			row:     map[string]interface{}{"name": "alice"},
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
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

func TestInExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name    string
		expr    *InExpr
		row     map[string]interface{}
		want    bool
		wantErr bool
	}{
		{
			name: "value in list - strings",
			expr: &InExpr{Column: "status", Values: []interface{}{"active", "pending", "complete"}},
			row:  map[string]interface{}{"status": "active"},
			want: true,
		},
		{
			name: "value not in list - strings",
			expr: &InExpr{Column: "status", Values: []interface{}{"active", "pending"}},
			row:  map[string]interface{}{"status": "deleted"},
			want: false,
		},
		{
			name: "value in list - numbers",
			expr: &InExpr{Column: "age", Values: []interface{}{int64(25), int64(30), int64(35)}},
			row:  map[string]interface{}{"age": int64(30)},
			want: true,
		},
		{
			name: "NOT IN - value in list",
			expr: &InExpr{Column: "status", Values: []interface{}{"deleted"}, Negate: true},
			row:  map[string]interface{}{"status": "deleted"},
			want: false,
		},
		{
			name: "NOT IN - value not in list",
			expr: &InExpr{Column: "status", Values: []interface{}{"deleted"}, Negate: true},
			row:  map[string]interface{}{"status": "active"},
			want: true,
		},
		{
			name:    "column missing",
			expr:    &InExpr{Column: "status", Values: []interface{}{"active"}},
			row:     map[string]interface{}{"name": "alice"},
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("InExpr.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("InExpr.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLikeExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name    string
		expr    *LikeExpr
		row     map[string]interface{}
		want    bool
		wantErr bool
	}{
		{
			name: "prefix match",
			expr: &LikeExpr{Column: "name", Pattern: "alice%"},
			row:  map[string]interface{}{"name": "alice smith"},
			want: true,
		},
		{
			name: "prefix no match",
			expr: &LikeExpr{Column: "name", Pattern: "alice%"},
			row:  map[string]interface{}{"name": "bob smith"},
			want: false,
		},
		{
			name: "suffix match",
			expr: &LikeExpr{Column: "email", Pattern: "%@example.com"},
			row:  map[string]interface{}{"email": "alice@example.com"},
			want: true,
		},
		{
			name: "contains match",
			expr: &LikeExpr{Column: "text", Pattern: "%test%"},
			row:  map[string]interface{}{"text": "this is a test string"},
			want: true,
		},
		{
			name: "exact match",
			expr: &LikeExpr{Column: "name", Pattern: "alice"},
			row:  map[string]interface{}{"name": "alice"},
			want: true,
		},
		{
			name: "single char wildcard",
			expr: &LikeExpr{Column: "code", Pattern: "A_C"},
			row:  map[string]interface{}{"code": "ABC"},
			want: true,
		},
		{
			name: "NOT LIKE match",
			expr: &LikeExpr{Column: "name", Pattern: "test%", Negate: true},
			row:  map[string]interface{}{"name": "alice"},
			want: true,
		},
		{
			name: "NOT LIKE no match",
			expr: &LikeExpr{Column: "name", Pattern: "test%", Negate: true},
			row:  map[string]interface{}{"name": "test user"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("LikeExpr.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LikeExpr.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBetweenExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name    string
		expr    *BetweenExpr
		row     map[string]interface{}
		want    bool
		wantErr bool
	}{
		{
			name: "between numbers - in range",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40)},
			row:  map[string]interface{}{"age": int64(30)},
			want: true,
		},
		{
			name: "between numbers - below range",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40)},
			row:  map[string]interface{}{"age": int64(20)},
			want: false,
		},
		{
			name: "between numbers - above range",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40)},
			row:  map[string]interface{}{"age": int64(50)},
			want: false,
		},
		{
			name: "between numbers - at lower bound",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40)},
			row:  map[string]interface{}{"age": int64(25)},
			want: true,
		},
		{
			name: "between numbers - at upper bound",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40)},
			row:  map[string]interface{}{"age": int64(40)},
			want: true,
		},
		{
			name: "NOT BETWEEN - in range",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40), Negate: true},
			row:  map[string]interface{}{"age": int64(30)},
			want: false,
		},
		{
			name: "NOT BETWEEN - outside range",
			expr: &BetweenExpr{Column: "age", Lower: int64(25), Upper: int64(40), Negate: true},
			row:  map[string]interface{}{"age": int64(50)},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("BetweenExpr.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BetweenExpr.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNullExpr_Evaluate(t *testing.T) {
	tests := []struct {
		name    string
		expr    *IsNullExpr
		row     map[string]interface{}
		want    bool
		wantErr bool
	}{
		{
			name: "IS NULL - column missing",
			expr: &IsNullExpr{Column: "email"},
			row:  map[string]interface{}{"name": "alice"},
			want: true,
		},
		{
			name: "IS NULL - value is nil",
			expr: &IsNullExpr{Column: "email"},
			row:  map[string]interface{}{"email": nil},
			want: true,
		},
		{
			name: "IS NULL - value exists",
			expr: &IsNullExpr{Column: "email"},
			row:  map[string]interface{}{"email": "alice@example.com"},
			want: false,
		},
		{
			name: "IS NOT NULL - column missing",
			expr: &IsNullExpr{Column: "email", Negate: true},
			row:  map[string]interface{}{"name": "alice"},
			want: false,
		},
		{
			name: "IS NOT NULL - value is nil",
			expr: &IsNullExpr{Column: "email", Negate: true},
			row:  map[string]interface{}{"email": nil},
			want: false,
		},
		{
			name: "IS NOT NULL - value exists",
			expr: &IsNullExpr{Column: "email", Negate: true},
			row:  map[string]interface{}{"email": "alice@example.com"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Evaluate(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsNullExpr.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsNullExpr.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyDistinct(t *testing.T) {
	tests := []struct {
		name string
		rows []map[string]interface{}
		want int // expected number of distinct rows
	}{
		{
			name: "no duplicates",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
				{"id": int64(3), "name": "charlie"},
			},
			want: 3,
		},
		{
			name: "some duplicates",
			rows: []map[string]interface{}{
				{"status": "active"},
				{"status": "pending"},
				{"status": "active"},
				{"status": "complete"},
				{"status": "pending"},
			},
			want: 3, // active, pending, complete
		},
		{
			name: "all duplicates",
			rows: []map[string]interface{}{
				{"status": "active"},
				{"status": "active"},
				{"status": "active"},
			},
			want: 1,
		},
		{
			name: "empty input",
			rows: []map[string]interface{}{},
			want: 0,
		},
		{
			name: "multiple columns",
			rows: []map[string]interface{}{
				{"status": "active", "dept": "sales"},
				{"status": "active", "dept": "eng"},
				{"status": "active", "dept": "sales"},
				{"status": "pending", "dept": "sales"},
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplyDistinct(tt.rows)
			if err != nil {
				t.Errorf("ApplyDistinct() error = %v", err)
				return
			}
			if len(got) != tt.want {
				t.Errorf("ApplyDistinct() returned %d rows, want %d", len(got), tt.want)
			}
		})
	}
}

func TestApplySelectListAfterWindows(t *testing.T) {
	tests := []struct {
		name       string
		rows       []map[string]interface{}
		selectList []SelectItem
		wantRows   []map[string]interface{}
		wantErr    bool
	}{
		{
			name: "empty rows returns empty",
			rows: []map[string]interface{}{},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "id"}},
			},
			wantRows: []map[string]interface{}{},
			wantErr:  false,
		},
		{
			name: "window expression projection",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "ROW_NUMBER": int64(1)},
				{"id": int64(2), "name": "bob", "ROW_NUMBER": int64(2)},
			},
			selectList: []SelectItem{
				{Expr: &WindowExpr{Function: "ROW_NUMBER"}},
			},
			wantRows: []map[string]interface{}{
				{"ROW_NUMBER": int64(1)},
				{"ROW_NUMBER": int64(2)},
			},
			wantErr: false,
		},
		{
			name: "window expression with alias",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "row_num": int64(1)},
				{"id": int64(2), "name": "bob", "row_num": int64(2)},
			},
			selectList: []SelectItem{
				{Expr: &WindowExpr{Function: "ROW_NUMBER"}, Alias: "row_num"},
			},
			wantRows: []map[string]interface{}{
				{"row_num": int64(1)},
				{"row_num": int64(2)},
			},
			wantErr: false,
		},
		{
			name: "mixed window and regular expressions",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "RANK": int64(1)},
				{"id": int64(2), "name": "bob", "RANK": int64(2)},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "id"}},
				{Expr: &WindowExpr{Function: "RANK"}},
				{Expr: &ColumnRef{Column: "name"}},
			},
			wantRows: []map[string]interface{}{
				{"id": int64(1), "RANK": int64(1), "name": "alice"},
				{"id": int64(2), "RANK": int64(2), "name": "bob"},
			},
			wantErr: false,
		},
		{
			name: "mixed window and regular with aliases",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "ranking": int64(1)},
				{"id": int64(2), "name": "bob", "ranking": int64(2)},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "id"}, Alias: "user_id"},
				{Expr: &WindowExpr{Function: "RANK"}, Alias: "ranking"},
			},
			wantRows: []map[string]interface{}{
				{"user_id": int64(1), "ranking": int64(1)},
				{"user_id": int64(2), "ranking": int64(2)},
			},
			wantErr: false,
		},
		{
			name: "error - missing window results",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
			},
			selectList: []SelectItem{
				{Expr: &WindowExpr{Function: "ROW_NUMBER"}},
			},
			wantRows: nil,
			wantErr:  true,
		},
		{
			name: "error - missing window results with alias",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
			},
			selectList: []SelectItem{
				{Expr: &WindowExpr{Function: "RANK"}, Alias: "ranking"},
			},
			wantRows: nil,
			wantErr:  true,
		},
		{
			name: "column reference without alias",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
			},
			wantRows: []map[string]interface{}{
				{"name": "alice"},
				{"name": "bob"},
			},
			wantErr: false,
		},
		{
			name: "column reference with alias",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
			},
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}, Alias: "user_name"},
			},
			wantRows: []map[string]interface{}{
				{"user_name": "alice"},
				{"user_name": "bob"},
			},
			wantErr: false,
		},
		{
			name: "function call without alias",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice"},
				{"id": int64(2), "name": "bob"},
			},
			selectList: []SelectItem{
				{Expr: &FunctionCall{Name: "UPPER", Args: []SelectExpression{&ColumnRef{Column: "name"}}}},
			},
			wantRows: []map[string]interface{}{
				{"UPPER": "ALICE"},
				{"UPPER": "BOB"},
			},
			wantErr: false,
		},
		{
			name: "multiple window functions",
			rows: []map[string]interface{}{
				{"id": int64(1), "name": "alice", "ROW_NUMBER": int64(1), "RANK": int64(1)},
				{"id": int64(2), "name": "bob", "ROW_NUMBER": int64(2), "RANK": int64(2)},
			},
			selectList: []SelectItem{
				{Expr: &WindowExpr{Function: "ROW_NUMBER"}},
				{Expr: &WindowExpr{Function: "RANK"}},
			},
			wantRows: []map[string]interface{}{
				{"ROW_NUMBER": int64(1), "RANK": int64(1)},
				{"ROW_NUMBER": int64(2), "RANK": int64(2)},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ApplySelectListAfterWindows(tt.rows, tt.selectList)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplySelectListAfterWindows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(got) != len(tt.wantRows) {
				t.Errorf("ApplySelectListAfterWindows() returned %d rows, want %d", len(got), len(tt.wantRows))
				return
			}

			for i, row := range got {
				wantRow := tt.wantRows[i]
				if len(row) != len(wantRow) {
					t.Errorf("Row %d: got %d columns, want %d", i, len(row), len(wantRow))
					continue
				}
				for key, wantVal := range wantRow {
					gotVal, exists := row[key]
					if !exists {
						t.Errorf("Row %d: missing column %q", i, key)
						continue
					}
					if gotVal != wantVal {
						t.Errorf("Row %d, column %q: got %v, want %v", i, key, gotVal, wantVal)
					}
				}
			}
		})
	}
}
