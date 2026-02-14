package query

import (
	"strings"
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

func TestParser_OrderBy(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantErr   bool
		wantCount int
		wantFirst string
		wantDesc  bool
	}{
		{
			name:      "single column ASC",
			query:     "select * from data.parquet order by age",
			wantErr:   false,
			wantCount: 1,
			wantFirst: "age",
			wantDesc:  false,
		},
		{
			name:      "single column DESC",
			query:     "select * from data.parquet order by age desc",
			wantErr:   false,
			wantCount: 1,
			wantFirst: "age",
			wantDesc:  true,
		},
		{
			name:      "single column explicit ASC",
			query:     "select * from data.parquet order by age asc",
			wantErr:   false,
			wantCount: 1,
			wantFirst: "age",
			wantDesc:  false,
		},
		{
			name:      "multiple columns",
			query:     "select * from data.parquet order by department, age desc",
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:      "with WHERE and ORDER BY",
			query:     "select * from data.parquet where age > 30 order by name",
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "with GROUP BY and ORDER BY",
			query:     "select status, count(*) from data.parquet group by status order by status",
			wantErr:   false,
			wantCount: 1,
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
				if len(q.OrderBy) != tt.wantCount {
					t.Errorf("OrderBy count = %d, want %d", len(q.OrderBy), tt.wantCount)
				}
				if tt.wantCount > 0 && tt.wantFirst != "" {
					if q.OrderBy[0].Column != tt.wantFirst {
						t.Errorf("First OrderBy column = %s, want %s", q.OrderBy[0].Column, tt.wantFirst)
					}
					if q.OrderBy[0].Desc != tt.wantDesc {
						t.Errorf("First OrderBy Desc = %v, want %v", q.OrderBy[0].Desc, tt.wantDesc)
					}
				}
			}
		})
	}
}

func TestParser_Limit(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantErr   bool
		wantLimit *int64
	}{
		{
			name:      "basic LIMIT",
			query:     "select * from data.parquet limit 10",
			wantErr:   false,
			wantLimit: ptrInt64(10),
		},
		{
			name:      "LIMIT 0",
			query:     "select * from data.parquet limit 0",
			wantErr:   false,
			wantLimit: ptrInt64(0),
		},
		{
			name:      "with WHERE and LIMIT",
			query:     "select * from data.parquet where age > 30 limit 5",
			wantErr:   false,
			wantLimit: ptrInt64(5),
		},
		{
			name:      "with ORDER BY and LIMIT",
			query:     "select * from data.parquet order by age limit 20",
			wantErr:   false,
			wantLimit: ptrInt64(20),
		},
		{
			name:    "negative LIMIT",
			query:   "select * from data.parquet limit -1",
			wantErr: true,
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
				if tt.wantLimit == nil && q.Limit != nil {
					t.Errorf("Limit = %v, want nil", *q.Limit)
				} else if tt.wantLimit != nil && q.Limit == nil {
					t.Errorf("Limit = nil, want %v", *tt.wantLimit)
				} else if tt.wantLimit != nil && q.Limit != nil && *q.Limit != *tt.wantLimit {
					t.Errorf("Limit = %v, want %v", *q.Limit, *tt.wantLimit)
				}
			}
		})
	}
}

func TestParser_Offset(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantErr    bool
		wantOffset *int64
	}{
		{
			name:       "basic OFFSET",
			query:      "select * from data.parquet offset 10",
			wantErr:    false,
			wantOffset: ptrInt64(10),
		},
		{
			name:       "OFFSET 0",
			query:      "select * from data.parquet offset 0",
			wantErr:    false,
			wantOffset: ptrInt64(0),
		},
		{
			name:       "with LIMIT and OFFSET",
			query:      "select * from data.parquet limit 10 offset 20",
			wantErr:    false,
			wantOffset: ptrInt64(20),
		},
		{
			name:       "with ORDER BY, LIMIT and OFFSET",
			query:      "select * from data.parquet order by age limit 10 offset 5",
			wantErr:    false,
			wantOffset: ptrInt64(5),
		},
		{
			name:    "negative OFFSET",
			query:   "select * from data.parquet offset -1",
			wantErr: true,
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
				if tt.wantOffset == nil && q.Offset != nil {
					t.Errorf("Offset = %v, want nil", *q.Offset)
				} else if tt.wantOffset != nil && q.Offset == nil {
					t.Errorf("Offset = nil, want %v", *tt.wantOffset)
				} else if tt.wantOffset != nil && q.Offset != nil && *q.Offset != *tt.wantOffset {
					t.Errorf("Offset = %v, want %v", *q.Offset, *tt.wantOffset)
				}
			}
		})
	}
}

func TestParser_LimitOffset(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantErr    bool
		wantLimit  *int64
		wantOffset *int64
	}{
		{
			name:       "LIMIT then OFFSET",
			query:      "select * from data.parquet limit 10 offset 5",
			wantErr:    false,
			wantLimit:  ptrInt64(10),
			wantOffset: ptrInt64(5),
		},
		{
			name:       "with all clauses",
			query:      "select name from data.parquet where age > 30 order by age desc limit 10 offset 5",
			wantErr:    false,
			wantLimit:  ptrInt64(10),
			wantOffset: ptrInt64(5),
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
				if tt.wantLimit != nil && (q.Limit == nil || *q.Limit != *tt.wantLimit) {
					var got interface{} = nil
					if q.Limit != nil {
						got = *q.Limit
					}
					t.Errorf("Limit = %v, want %v", got, *tt.wantLimit)
				}
				if tt.wantOffset != nil && (q.Offset == nil || *q.Offset != *tt.wantOffset) {
					var got interface{} = nil
					if q.Offset != nil {
						got = *q.Offset
					}
					t.Errorf("Offset = %v, want %v", got, *tt.wantOffset)
				}
			}
		})
	}
}

// Helper function to create int64 pointer
func ptrInt64(v int64) *int64 {
	return &v
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

func TestParser_Distinct(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		wantErr      bool
		wantDistinct bool
	}{
		{
			name:         "SELECT DISTINCT",
			query:        "select DISTINCT status from data.parquet",
			wantErr:      false,
			wantDistinct: true,
		},
		{
			name:         "SELECT without DISTINCT",
			query:        "select status from data.parquet",
			wantErr:      false,
			wantDistinct: false,
		},
		{
			name:         "DISTINCT with multiple columns",
			query:        "select DISTINCT status, department from data.parquet",
			wantErr:      false,
			wantDistinct: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q.Distinct != tt.wantDistinct {
				t.Errorf("Distinct = %v, want %v", q.Distinct, tt.wantDistinct)
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestWindowFrameParsing(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantFrameType FrameType
		wantStartType BoundType
		wantEndType   BoundType
		wantStartOff  int64
		wantEndOff    int64
		wantErr       bool
	}{
		{
			name:          "ROWS UNBOUNDED PRECEDING",
			sql:           "SELECT ROW_NUMBER() OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundUnboundedPreceding,
			wantEndType:   BoundCurrentRow,
			wantErr:       false,
		},
		{
			name:          "ROWS CURRENT ROW",
			sql:           "SELECT ROW_NUMBER() OVER (ORDER BY id ROWS CURRENT ROW) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundCurrentRow,
			wantEndType:   BoundCurrentRow,
			wantErr:       false,
		},
		{
			name:          "ROWS 1 PRECEDING",
			sql:           "SELECT ROW_NUMBER() OVER (ORDER BY id ROWS 1 PRECEDING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundOffsetPreceding,
			wantEndType:   BoundCurrentRow,
			wantStartOff:  1,
			wantErr:       false,
		},
		{
			name:          "ROWS 5 FOLLOWING",
			sql:           "SELECT ROW_NUMBER() OVER (ORDER BY id ROWS 5 FOLLOWING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundOffsetFollowing,
			wantEndType:   BoundCurrentRow,
			wantStartOff:  5,
			wantErr:       false,
		},
		{
			name:          "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			sql:           "SELECT RANK() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundUnboundedPreceding,
			wantEndType:   BoundCurrentRow,
			wantErr:       false,
		},
		{
			name:          "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
			sql:           "SELECT FIRST_VALUE(price) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundOffsetPreceding,
			wantEndType:   BoundOffsetFollowing,
			wantStartOff:  1,
			wantEndOff:    1,
			wantErr:       false,
		},
		{
			name:          "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			sql:           "SELECT LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundCurrentRow,
			wantEndType:   BoundUnboundedFollowing,
			wantErr:       false,
		},
		{
			name:          "ROWS BETWEEN 2 PRECEDING AND 3 PRECEDING",
			sql:           "SELECT LAG(value) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 3 PRECEDING) FROM data.parquet",
			wantFrameType: FrameTypeRows,
			wantStartType: BoundOffsetPreceding,
			wantEndType:   BoundOffsetPreceding,
			wantStartOff:  2,
			wantEndOff:    3,
			wantErr:       false,
		},
		{
			name:          "RANGE UNBOUNDED PRECEDING",
			sql:           "SELECT DENSE_RANK() OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) FROM data.parquet",
			wantFrameType: FrameTypeRange,
			wantStartType: BoundUnboundedPreceding,
			wantEndType:   BoundCurrentRow,
			wantErr:       false,
		},
		{
			name:          "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			sql:           "SELECT LEAD(value) OVER (ORDER BY id RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM data.parquet",
			wantFrameType: FrameTypeRange,
			wantStartType: BoundCurrentRow,
			wantEndType:   BoundUnboundedFollowing,
			wantErr:       false,
		},
		{
			name:          "RANGE BETWEEN 10 PRECEDING AND 5 FOLLOWING",
			sql:           "SELECT NTILE(4) OVER (ORDER BY timestamp RANGE BETWEEN 10 PRECEDING AND 5 FOLLOWING) FROM data.parquet",
			wantFrameType: FrameTypeRange,
			wantStartType: BoundOffsetPreceding,
			wantEndType:   BoundOffsetFollowing,
			wantStartOff:  10,
			wantEndOff:    5,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			// Find the window function in the select list
			if len(q.SelectList) == 0 {
				t.Fatal("No select list items found")
			}

			selectItem, ok := q.SelectList[0].Expr.(*WindowExpr)
			if !ok {
				t.Fatalf("Expected WindowExpr, got %T", q.SelectList[0].Expr)
			}

			if selectItem.Window == nil {
				t.Fatal("Expected window spec, got nil")
			}

			if selectItem.Window.Frame == nil {
				t.Fatal("Expected window frame, got nil")
			}

			frame := selectItem.Window.Frame
			if frame.Type != tt.wantFrameType {
				t.Errorf("Frame type = %v, want %v", frame.Type, tt.wantFrameType)
			}

			if frame.Start.Type != tt.wantStartType {
				t.Errorf("Start bound type = %v, want %v", frame.Start.Type, tt.wantStartType)
			}

			if frame.End.Type != tt.wantEndType {
				t.Errorf("End bound type = %v, want %v", frame.End.Type, tt.wantEndType)
			}

			if tt.wantStartOff != 0 && frame.Start.Offset != tt.wantStartOff {
				t.Errorf("Start offset = %v, want %v", frame.Start.Offset, tt.wantStartOff)
			}

			if tt.wantEndOff != 0 && frame.End.Offset != tt.wantEndOff {
				t.Errorf("End offset = %v, want %v", frame.End.Offset, tt.wantEndOff)
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
