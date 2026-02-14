package query

import (
	"testing"
)

func TestROW_NUMBER(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "ROW_NUMBER",
			Args:     []SelectExpression{},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "age", Desc: false}}},
		}, Alias: "row_num"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify row_num column exists and has correct values
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	// After sorting by age: Bob(25)=1, Alice(30)=2, Charlie(35)=3
	// But we need to find which row is which
	for _, row := range result {
		name := row["name"].(string)
		rowNum := row["row_num"].(int64)

		switch name {
		case "Bob":
			if rowNum != 1 {
				t.Errorf("Expected Bob to have row_num=1, got %d", rowNum)
			}
		case "Alice":
			if rowNum != 2 {
				t.Errorf("Expected Alice to have row_num=2, got %d", rowNum)
			}
		case "Charlie":
			if rowNum != 3 {
				t.Errorf("Expected Charlie to have row_num=3, got %d", rowNum)
			}
		}
	}
}

func TestRANK(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "score": 90},
		{"name": "Bob", "score": 90},
		{"name": "Charlie", "score": 85},
		{"name": "David", "score": 95},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "RANK",
			Args:     []SelectExpression{},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "score", Desc: true}}},
		}, Alias: "rank"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify rank values
	for _, row := range result {
		name := row["name"].(string)
		rank := row["rank"].(int64)

		switch name {
		case "David":
			if rank != 1 {
				t.Errorf("Expected David to have rank=1, got %d", rank)
			}
		case "Alice", "Bob":
			if rank != 2 {
				t.Errorf("Expected %s to have rank=2 (tied), got %d", name, rank)
			}
		case "Charlie":
			if rank != 4 {
				t.Errorf("Expected Charlie to have rank=4 (skip 3), got %d", rank)
			}
		}
	}
}

func TestDENSE_RANK(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "score": 90},
		{"name": "Bob", "score": 90},
		{"name": "Charlie", "score": 85},
		{"name": "David", "score": 95},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "DENSE_RANK",
			Args:     []SelectExpression{},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "score", Desc: true}}},
		}, Alias: "dense_rank"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify dense_rank values
	for _, row := range result {
		name := row["name"].(string)
		denseRank := row["dense_rank"].(int64)

		switch name {
		case "David":
			if denseRank != 1 {
				t.Errorf("Expected David to have dense_rank=1, got %d", denseRank)
			}
		case "Alice", "Bob":
			if denseRank != 2 {
				t.Errorf("Expected %s to have dense_rank=2 (tied), got %d", name, denseRank)
			}
		case "Charlie":
			if denseRank != 3 {
				t.Errorf("Expected Charlie to have dense_rank=3 (no skip), got %d", denseRank)
			}
		}
	}
}

func TestNTILE(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "score": 90},
		{"name": "Bob", "score": 85},
		{"name": "Charlie", "score": 95},
		{"name": "David", "score": 80},
		{"name": "Eve", "score": 88},
		{"name": "Frank", "score": 92},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "NTILE",
			Args:     []SelectExpression{&LiteralExpr{Value: int64(3)}},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "score", Desc: false}}},
		}, Alias: "tile"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify tile values - 6 rows into 3 tiles = 2 rows per tile
	// After sorting by score: David(80), Bob(85), Eve(88), Alice(90), Frank(92), Charlie(95)
	for _, row := range result {
		name := row["name"].(string)
		tile := row["tile"].(int64)

		switch name {
		case "David", "Bob":
			if tile != 1 {
				t.Errorf("Expected %s to have tile=1, got %d", name, tile)
			}
		case "Eve", "Alice":
			if tile != 2 {
				t.Errorf("Expected %s to have tile=2, got %d", name, tile)
			}
		case "Frank", "Charlie":
			if tile != 3 {
				t.Errorf("Expected %s to have tile=3, got %d", name, tile)
			}
		}
	}
}

func TestFIRST_VALUE(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "salary": 50000},
		{"name": "Bob", "salary": 60000},
		{"name": "Charlie", "salary": 55000},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "FIRST_VALUE",
			Args:     []SelectExpression{&ColumnRef{Column: "salary"}},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "salary", Desc: false}}},
		}, Alias: "first_salary"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// All rows should have first_salary = 50000 (Alice's salary is lowest)
	for _, row := range result {
		firstSalary := row["first_salary"]
		if firstSalary != 50000 {
			t.Errorf("Expected first_salary=50000, got %v", firstSalary)
		}
	}
}

func TestLAST_VALUE(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "salary": 50000},
		{"name": "Bob", "salary": 60000},
		{"name": "Charlie", "salary": 55000},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "LAST_VALUE",
			Args:     []SelectExpression{&ColumnRef{Column: "salary"}},
			Window:   &WindowSpec{OrderBy: []OrderByItem{{Column: "salary", Desc: false}}},
		}, Alias: "last_salary"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// All rows should have last_salary = 60000 (Bob's salary is highest)
	for _, row := range result {
		lastSalary := row["last_salary"]
		if lastSalary != 60000 {
			t.Errorf("Expected last_salary=60000, got %v", lastSalary)
		}
	}
}

func TestNTH_VALUE(t *testing.T) {
	rows := []map[string]interface{}{
		{"name": "Alice", "salary": 50000},
		{"name": "Bob", "salary": 60000},
		{"name": "Charlie", "salary": 55000},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "NTH_VALUE",
			Args: []SelectExpression{
				&ColumnRef{Column: "salary"},
				&LiteralExpr{Value: int64(2)},
			},
			Window: &WindowSpec{OrderBy: []OrderByItem{{Column: "salary", Desc: false}}},
		}, Alias: "second_salary"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// All rows should have second_salary = 55000 (Charlie's salary is second when sorted)
	for _, row := range result {
		secondSalary := row["second_salary"]
		if secondSalary != 55000 {
			t.Errorf("Expected second_salary=55000, got %v", secondSalary)
		}
	}
}

func TestLAG(t *testing.T) {
	rows := []map[string]interface{}{
		{"date": "2024-01-01", "value": 100},
		{"date": "2024-01-02", "value": 110},
		{"date": "2024-01-03", "value": 105},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "date"}, Alias: ""},
		{Expr: &ColumnRef{Column: "value"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "LAG",
			Args: []SelectExpression{
				&ColumnRef{Column: "value"},
				&LiteralExpr{Value: int64(1)},
			},
			Window: &WindowSpec{OrderBy: []OrderByItem{{Column: "date", Desc: false}}},
		}, Alias: "prev_value"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify LAG values
	for _, row := range result {
		date := row["date"].(string)
		prevValue := row["prev_value"]

		switch date {
		case "2024-01-01":
			if prevValue != nil {
				t.Errorf("Expected prev_value=nil for first row, got %v", prevValue)
			}
		case "2024-01-02":
			if prevValue != 100 {
				t.Errorf("Expected prev_value=100, got %v", prevValue)
			}
		case "2024-01-03":
			if prevValue != 110 {
				t.Errorf("Expected prev_value=110, got %v", prevValue)
			}
		}
	}
}

func TestLEAD(t *testing.T) {
	rows := []map[string]interface{}{
		{"date": "2024-01-01", "value": 100},
		{"date": "2024-01-02", "value": 110},
		{"date": "2024-01-03", "value": 105},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "date"}, Alias: ""},
		{Expr: &ColumnRef{Column: "value"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "LEAD",
			Args: []SelectExpression{
				&ColumnRef{Column: "value"},
				&LiteralExpr{Value: int64(1)},
			},
			Window: &WindowSpec{OrderBy: []OrderByItem{{Column: "date", Desc: false}}},
		}, Alias: "next_value"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify LEAD values
	for _, row := range result {
		date := row["date"].(string)
		nextValue := row["next_value"]

		switch date {
		case "2024-01-01":
			if nextValue != 110 {
				t.Errorf("Expected next_value=110, got %v", nextValue)
			}
		case "2024-01-02":
			if nextValue != 105 {
				t.Errorf("Expected next_value=105, got %v", nextValue)
			}
		case "2024-01-03":
			if nextValue != nil {
				t.Errorf("Expected next_value=nil for last row, got %v", nextValue)
			}
		}
	}
}

func TestWindowWithPartition(t *testing.T) {
	rows := []map[string]interface{}{
		{"dept": "Sales", "name": "Alice", "salary": 50000},
		{"dept": "Sales", "name": "Bob", "salary": 60000},
		{"dept": "IT", "name": "Charlie", "salary": 55000},
		{"dept": "IT", "name": "David", "salary": 65000},
	}

	selectList := []SelectItem{
		{Expr: &ColumnRef{Column: "dept"}, Alias: ""},
		{Expr: &ColumnRef{Column: "name"}, Alias: ""},
		{Expr: &WindowExpr{
			Function: "ROW_NUMBER",
			Args:     []SelectExpression{},
			Window: &WindowSpec{
				PartitionBy: []string{"dept"},
				OrderBy:     []OrderByItem{{Column: "salary", Desc: false}},
			},
		}, Alias: "dept_rank"},
	}

	result, err := ApplyWindowFunctions(rows, selectList)
	if err != nil {
		t.Fatalf("ApplyWindowFunctions failed: %v", err)
	}

	// Verify partitioned row numbers
	for _, row := range result {
		name := row["name"].(string)
		deptRank := row["dept_rank"].(int64)

		switch name {
		case "Alice": // Sales, lowest salary in Sales
			if deptRank != 1 {
				t.Errorf("Expected Alice to have dept_rank=1, got %d", deptRank)
			}
		case "Bob": // Sales, highest salary in Sales
			if deptRank != 2 {
				t.Errorf("Expected Bob to have dept_rank=2, got %d", deptRank)
			}
		case "Charlie": // IT, lowest salary in IT
			if deptRank != 1 {
				t.Errorf("Expected Charlie to have dept_rank=1, got %d", deptRank)
			}
		case "David": // IT, highest salary in IT
			if deptRank != 2 {
				t.Errorf("Expected David to have dept_rank=2, got %d", deptRank)
			}
		}
	}
}

func TestParseWindowFunction(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "ROW_NUMBER with ORDER BY",
			query:   "SELECT name, ROW_NUMBER() OVER (ORDER BY age) as row_num FROM test.parquet",
			wantErr: false,
		},
		{
			name:    "RANK with PARTITION BY and ORDER BY",
			query:   "SELECT dept, name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank FROM test.parquet",
			wantErr: false,
		},
		{
			name:    "LAG with arguments",
			query:   "SELECT date, value, LAG(value, 1) OVER (ORDER BY date) as prev FROM test.parquet",
			wantErr: false,
		},
		{
			name:    "Window function without OVER clause should fail",
			query:   "SELECT name, ROW_NUMBER() FROM test.parquet",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHasSubqueryInWHERE(t *testing.T) {
	tests := []struct {
		name     string
		filter   Expression
		expected bool
	}{
		{
			name:     "nil filter",
			filter:   nil,
			expected: false,
		},
		{
			name: "InSubqueryExpr",
			filter: &InSubqueryExpr{
				Column:   "id",
				Subquery: &Query{},
			},
			expected: true,
		},
		{
			name:     "ExistsExpr",
			filter:   &ExistsExpr{Subquery: &Query{}},
			expected: true,
		},
		{
			name: "BinaryExpr with subquery in left",
			filter: &BinaryExpr{
				Operator: TokenAnd,
				Left: &InSubqueryExpr{
					Column:   "id",
					Subquery: &Query{},
				},
				Right: &ComparisonExpr{
					Column:   "status",
					Operator: TokenEqual,
					Value:    "active",
				},
			},
			expected: true,
		},
		{
			name: "BinaryExpr with subquery in right",
			filter: &BinaryExpr{
				Operator: TokenOr,
				Left: &ComparisonExpr{
					Column:   "status",
					Operator: TokenEqual,
					Value:    "active",
				},
				Right: &ExistsExpr{Subquery: &Query{}},
			},
			expected: true,
		},
		{
			name: "Simple ComparisonExpr without subquery",
			filter: &ComparisonExpr{
				Column:   "status",
				Operator: TokenEqual,
				Value:    "active",
			},
			expected: false,
		},
		{
			name:     "Simple IsNullExpr",
			filter:   &IsNullExpr{Column: "active"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasSubqueryInWHERE(tt.filter)
			if result != tt.expected {
				t.Errorf("HasSubqueryInWHERE() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasSubqueryInExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     Expression
		expected bool
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: false,
		},
		{
			name: "InSubqueryExpr",
			expr: &InSubqueryExpr{
				Column:   "id",
				Subquery: &Query{},
			},
			expected: true,
		},
		{
			name:     "ExistsExpr",
			expr:     &ExistsExpr{Subquery: &Query{}},
			expected: true,
		},
		{
			name: "Nested BinaryExpr with deep subquery",
			expr: &BinaryExpr{
				Operator: TokenAnd,
				Left: &ComparisonExpr{
					Column:   "a",
					Operator: TokenEqual,
					Value:    int64(1),
				},
				Right: &BinaryExpr{
					Operator: TokenOr,
					Left: &ComparisonExpr{
						Column:   "b",
						Operator: TokenGreater,
						Value:    int64(5),
					},
					Right: &InSubqueryExpr{
						Column:   "c",
						Subquery: &Query{},
					},
				},
			},
			expected: true,
		},
		{
			name: "BinaryExpr without subquery",
			expr: &BinaryExpr{
				Operator: TokenAnd,
				Left: &ComparisonExpr{
					Column:   "a",
					Operator: TokenEqual,
					Value:    int64(1),
				},
				Right: &ComparisonExpr{
					Column:   "b",
					Operator: TokenGreater,
					Value:    int64(5),
				},
			},
			expected: false,
		},
		{
			name:     "IsNullExpr",
			expr:     &IsNullExpr{Column: "name"},
			expected: false,
		},
		{
			name: "LikeExpr",
			expr: &LikeExpr{
				Column:  "name",
				Pattern: "%test%",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasSubqueryInExpression(tt.expr)
			if result != tt.expected {
				t.Errorf("hasSubqueryInExpression() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasSubqueryInSELECT(t *testing.T) {
	tests := []struct {
		name       string
		selectList []SelectItem
		expected   bool
	}{
		{
			name:       "empty select list",
			selectList: []SelectItem{},
			expected:   false,
		},
		{
			name: "SelectItem with ScalarSubqueryExpr",
			selectList: []SelectItem{
				{Expr: &ScalarSubqueryExpr{Query: &Query{}}},
			},
			expected: true,
		},
		{
			name: "SelectItem with FunctionCall containing ScalarSubquery",
			selectList: []SelectItem{
				{
					Expr: &FunctionCall{
						Name: "COALESCE",
						Args: []SelectExpression{
							&ScalarSubqueryExpr{Query: &Query{}},
							&LiteralExpr{Value: 0},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "SelectItem with CaseExpr with ScalarSubquery in ELSE",
			selectList: []SelectItem{
				{
					Expr: &CaseExpr{
						WhenClauses: []WhenClause{
							{
								Result: &LiteralExpr{Value: 1},
							},
						},
						ElseExpr: &ScalarSubqueryExpr{Query: &Query{}},
					},
				},
			},
			expected: true,
		},
		{
			name: "SelectItem with CaseExpr with ScalarSubquery in WHEN result",
			selectList: []SelectItem{
				{
					Expr: &CaseExpr{
						WhenClauses: []WhenClause{
							{
								Result: &ScalarSubqueryExpr{Query: &Query{}},
							},
						},
						ElseExpr: &LiteralExpr{Value: 0},
					},
				},
			},
			expected: true,
		},
		{
			name: "Multiple SelectItems with subquery in second",
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
				{Expr: &ScalarSubqueryExpr{Query: &Query{}}},
			},
			expected: true,
		},
		{
			name: "SelectItems without subqueries",
			selectList: []SelectItem{
				{Expr: &ColumnRef{Column: "name"}},
				{Expr: &LiteralExpr{Value: 42}},
				{
					Expr: &FunctionCall{
						Name: "UPPER",
						Args: []SelectExpression{&ColumnRef{Column: "name"}},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasSubqueryInSELECT(tt.selectList)
			if result != tt.expected {
				t.Errorf("HasSubqueryInSELECT() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasScalarSubquery(t *testing.T) {
	tests := []struct {
		name     string
		expr     SelectExpression
		expected bool
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: false,
		},
		{
			name:     "ScalarSubqueryExpr",
			expr:     &ScalarSubqueryExpr{Query: &Query{}},
			expected: true,
		},
		{
			name: "FunctionCall with ScalarSubquery argument",
			expr: &FunctionCall{
				Name: "COALESCE",
				Args: []SelectExpression{
					&ScalarSubqueryExpr{Query: &Query{}},
					&LiteralExpr{Value: 0},
				},
			},
			expected: true,
		},
		{
			name: "Nested FunctionCall with deep ScalarSubquery",
			expr: &FunctionCall{
				Name: "UPPER",
				Args: []SelectExpression{
					&FunctionCall{
						Name: "CONCAT",
						Args: []SelectExpression{
							&ColumnRef{Column: "name"},
							&ScalarSubqueryExpr{Query: &Query{}},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "FunctionCall without ScalarSubquery",
			expr: &FunctionCall{
				Name: "UPPER",
				Args: []SelectExpression{
					&ColumnRef{Column: "name"},
					&LiteralExpr{Value: "test"},
				},
			},
			expected: false,
		},
		{
			name: "CaseExpr with ScalarSubquery in ELSE",
			expr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Result: &LiteralExpr{Value: "one"},
					},
				},
				ElseExpr: &ScalarSubqueryExpr{Query: &Query{}},
			},
			expected: true,
		},
		{
			name: "CaseExpr with ScalarSubquery in WHEN result",
			expr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Result: &ScalarSubqueryExpr{Query: &Query{}},
					},
				},
				ElseExpr: &LiteralExpr{Value: "default"},
			},
			expected: true,
		},
		{
			name: "CaseExpr with ScalarSubquery in multiple WHEN results",
			expr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Result: &LiteralExpr{Value: "one"},
					},
					{
						Result: &ScalarSubqueryExpr{Query: &Query{}},
					},
				},
				ElseExpr: &LiteralExpr{Value: "default"},
			},
			expected: true,
		},
		{
			name: "CaseExpr without ScalarSubquery",
			expr: &CaseExpr{
				WhenClauses: []WhenClause{
					{
						Result: &LiteralExpr{Value: "one"},
					},
				},
				ElseExpr: &LiteralExpr{Value: "default"},
			},
			expected: false,
		},
		{
			name:     "ColumnRef",
			expr:     &ColumnRef{Column: "name"},
			expected: false,
		},
		{
			name:     "LiteralExpr",
			expr:     &LiteralExpr{Value: 42},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasScalarSubquery(tt.expr)
			if result != tt.expected {
				t.Errorf("hasScalarSubquery() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
