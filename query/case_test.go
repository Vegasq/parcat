package query

import (
	"testing"
)

func TestParser_CaseExpression(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple case",
			sql:     "select case when age > 18 then 'adult' else 'minor' end from users.parquet",
			wantErr: false,
		},
		{
			name:    "multiple when clauses",
			sql:     "select case when age < 13 then 'child' when age < 18 then 'teen' else 'adult' end from users.parquet",
			wantErr: false,
		},
		{
			name:    "no else clause",
			sql:     "select case when status = 'active' then 1 end from users.parquet",
			wantErr: false,
		},
		{
			name:    "with alias",
			sql:     "select case when age > 18 then 'adult' else 'minor' end as age_group from users.parquet",
			wantErr: false,
		},
		{
			name:    "missing when",
			sql:     "select case then 'result' end from users.parquet",
			wantErr: true,
		},
		{
			name:    "missing then",
			sql:     "select case when age > 18 'adult' end from users.parquet",
			wantErr: true,
		},
		{
			name:    "missing end",
			sql:     "select case when age > 18 then 'adult' from users.parquet",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && query == nil {
				t.Error("expected query to be non-nil")
			}
		})
	}
}

func TestCaseExpr_EvaluateSelect(t *testing.T) {
	tests := []struct {
		name     string
		caseExpr *CaseExpr
		row      map[string]interface{}
		want     interface{}
		wantErr  bool
	}{
		{
			name: "simple case - first condition matches",
			caseExpr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Condition: &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(18)},
						Result:    &LiteralExpr{Value: "adult"},
					},
				},
				ElseExpr: &LiteralExpr{Value: "minor"},
			},
			row:     map[string]interface{}{"age": int64(25)},
			want:    "adult",
			wantErr: false,
		},
		{
			name: "simple case - else clause",
			caseExpr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Condition: &ComparisonExpr{Column: "age", Operator: TokenGreater, Value: int64(18)},
						Result:    &LiteralExpr{Value: "adult"},
					},
				},
				ElseExpr: &LiteralExpr{Value: "minor"},
			},
			row:     map[string]interface{}{"age": int64(15)},
			want:    "minor",
			wantErr: false,
		},
		{
			name: "multiple conditions - second matches",
			caseExpr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Condition: &ComparisonExpr{Column: "age", Operator: TokenLess, Value: int64(13)},
						Result:    &LiteralExpr{Value: "child"},
					},
					{
						Condition: &ComparisonExpr{Column: "age", Operator: TokenLess, Value: int64(18)},
						Result:    &LiteralExpr{Value: "teen"},
					},
				},
				ElseExpr: &LiteralExpr{Value: "adult"},
			},
			row:     map[string]interface{}{"age": int64(15)},
			want:    "teen",
			wantErr: false,
		},
		{
			name: "no match and no else - returns nil",
			caseExpr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Condition: &ComparisonExpr{Column: "status", Operator: TokenEqual, Value: "active"},
						Result:    &LiteralExpr{Value: int64(1)},
					},
				},
				ElseExpr: nil,
			},
			row:     map[string]interface{}{"status": "inactive"},
			want:    nil,
			wantErr: false,
		},
		{
			name: "case with column reference in result",
			caseExpr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Condition: &ComparisonExpr{Column: "status", Operator: TokenEqual, Value: "vip"},
						Result:    &ColumnRef{Column: "vip_discount"},
					},
				},
				ElseExpr: &ColumnRef{Column: "regular_discount"},
			},
			row:     map[string]interface{}{"status": "vip", "vip_discount": 20, "regular_discount": 10},
			want:    20,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.caseExpr.EvaluateSelect(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("CaseExpr.EvaluateSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CaseExpr.EvaluateSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntegration_CaseExpression(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		rows    []map[string]interface{}
		want    []map[string]interface{}
		wantErr bool
	}{
		{
			name: "categorize age groups",
			sql:  "select name, case when age < 18 then 'minor' else 'adult' end as category from users.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(25)},
				{"name": "Bob", "age": int64(15)},
				{"name": "Charlie", "age": int64(30)},
			},
			want: []map[string]interface{}{
				{"name": "Alice", "category": "adult"},
				{"name": "Bob", "category": "minor"},
				{"name": "Charlie", "category": "adult"},
			},
			wantErr: false,
		},
		{
			name: "multiple conditions",
			sql:  "select name, case when age < 13 then 'child' when age < 18 then 'teen' when age < 65 then 'adult' else 'senior' end as age_category from users.parquet",
			rows: []map[string]interface{}{
				{"name": "Kid", "age": int64(10)},
				{"name": "Teen", "age": int64(15)},
				{"name": "Adult", "age": int64(40)},
				{"name": "Senior", "age": int64(70)},
			},
			want: []map[string]interface{}{
				{"name": "Kid", "age_category": "child"},
				{"name": "Teen", "age_category": "teen"},
				{"name": "Adult", "age_category": "adult"},
				{"name": "Senior", "age_category": "senior"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Project the rows
			var got []map[string]interface{}
			for _, row := range tt.rows {
				result := make(map[string]interface{})
				for _, item := range query.SelectList {
					value, err := item.Expr.EvaluateSelect(row)
					if err != nil {
						t.Fatalf("EvaluateSelect() error = %v", err)
					}

					// Use alias if present, otherwise use column name
					key := item.Alias
					if key == "" {
						if colRef, ok := item.Expr.(*ColumnRef); ok {
							key = colRef.Column
						} else {
							// For complex expressions without alias, skip (or handle differently)
							continue
						}
					}
					result[key] = value
				}
				got = append(got, result)
			}

			// Compare results
			if len(got) != len(tt.want) {
				t.Errorf("got %d rows, want %d rows", len(got), len(tt.want))
				return
			}

			for i := range got {
				for key := range tt.want[i] {
					if got[i][key] != tt.want[i][key] {
						t.Errorf("row %d: got[%s] = %v, want %v", i, key, got[i][key], tt.want[i][key])
					}
				}
			}
		})
	}
}
