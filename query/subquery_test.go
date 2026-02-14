package query

import (
	"testing"
)

func TestParseSubqueryInFROM(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple FROM subquery",
			query:   "SELECT * FROM (SELECT * FROM data.parquet)",
			wantErr: false,
		},
		{
			name:    "FROM subquery with WHERE",
			query:   "SELECT * FROM (SELECT * FROM data.parquet WHERE age > 30)",
			wantErr: false,
		},
		{
			name:    "FROM subquery with alias",
			query:   "SELECT * FROM (SELECT name, age FROM data.parquet) WHERE age > 25",
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
			if err == nil && q.Subquery == nil {
				t.Errorf("Expected subquery to be parsed, but got nil")
			}
		})
	}
}

func TestParseINSubquery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "IN with subquery",
			query:   "SELECT * FROM users.parquet WHERE department IN (SELECT dept FROM large_depts.parquet)",
			wantErr: false,
		},
		{
			name:    "NOT IN with subquery",
			query:   "SELECT * FROM users.parquet WHERE status NOT IN (SELECT status FROM inactive.parquet)",
			wantErr: false,
		},
		{
			name:    "IN with value list (not subquery)",
			query:   "SELECT * FROM users.parquet WHERE status IN ('active', 'pending')",
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
			if err != nil {
				return
			}

			// Check that filter is not nil
			if q.Filter == nil {
				t.Errorf("Expected filter to be non-nil")
			}
		})
	}
}

func TestParseEXISTSSubquery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "EXISTS subquery",
			query:   "SELECT * FROM users.parquet WHERE EXISTS (SELECT 1 FROM orders.parquet WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "NOT EXISTS subquery",
			query:   "SELECT * FROM users.parquet WHERE NOT EXISTS (SELECT 1 FROM orders.parquet WHERE user_id = 456)",
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
			if err != nil {
				return
			}

			// Check that filter contains EXISTS expression
			if q.Filter == nil {
				t.Errorf("Expected filter to be non-nil")
			}

			// Check if it's an ExistsExpr
			switch expr := q.Filter.(type) {
			case *ExistsExpr:
				if expr.Subquery == nil {
					t.Errorf("Expected EXISTS to have subquery")
				}
			case *BinaryExpr:
				// Could be wrapped in AND/OR, that's ok
			default:
				t.Errorf("Expected ExistsExpr in filter, got %T", expr)
			}
		})
	}
}

func TestParseScalarSubquery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "scalar subquery in SELECT",
			query:   "SELECT name, (SELECT COUNT(*) FROM orders.parquet) as total_orders FROM users.parquet",
			wantErr: false,
		},
		{
			name:    "multiple scalar subqueries",
			query:   "SELECT name, (SELECT AVG(age) FROM users.parquet) as avg_age, (SELECT MAX(salary) FROM employees.parquet) as max_salary FROM data.parquet",
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
			if err != nil {
				return
			}

			// Check that SELECT list contains scalar subquery
			if len(q.SelectList) == 0 {
				t.Errorf("Expected SELECT list to be non-empty")
			}

			// Check if any item is a ScalarSubqueryExpr
			foundSubquery := false
			for _, item := range q.SelectList {
				if _, ok := item.Expr.(*ScalarSubqueryExpr); ok {
					foundSubquery = true
					break
				}
			}

			if !foundSubquery {
				t.Errorf("Expected to find ScalarSubqueryExpr in SELECT list")
			}
		})
	}
}

func TestINSubqueryStructure(t *testing.T) {
	query := "SELECT * FROM users.parquet WHERE department IN (SELECT dept FROM large_depts.parquet)"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check that filter is InSubqueryExpr
	inExpr, ok := q.Filter.(*InSubqueryExpr)
	if !ok {
		t.Fatalf("Expected filter to be InSubqueryExpr, got %T", q.Filter)
	}

	// Check column name
	if inExpr.Column != "department" {
		t.Errorf("Expected column 'department', got %q", inExpr.Column)
	}

	// Check subquery
	if inExpr.Subquery == nil {
		t.Errorf("Expected subquery to be non-nil")
	}

	// Check subquery table name
	if inExpr.Subquery.TableName != "large_depts.parquet" {
		t.Errorf("Expected subquery table 'large_depts.parquet', got %q", inExpr.Subquery.TableName)
	}

	// Check negation
	if inExpr.Negate {
		t.Errorf("Expected Negate to be false")
	}
}

func TestEXISTSSubqueryStructure(t *testing.T) {
	query := "SELECT * FROM users.parquet WHERE EXISTS (SELECT 1 FROM orders.parquet WHERE status = 'active')"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check that filter is ExistsExpr
	existsExpr, ok := q.Filter.(*ExistsExpr)
	if !ok {
		t.Fatalf("Expected filter to be ExistsExpr, got %T", q.Filter)
	}

	// Check subquery
	if existsExpr.Subquery == nil {
		t.Errorf("Expected subquery to be non-nil")
	}

	// Check subquery table name
	if existsExpr.Subquery.TableName != "orders.parquet" {
		t.Errorf("Expected subquery table 'orders.parquet', got %q", existsExpr.Subquery.TableName)
	}

	// Check negation
	if existsExpr.Negate {
		t.Errorf("Expected Negate to be false")
	}
}

func TestNOTEXISTSSubqueryStructure(t *testing.T) {
	query := "SELECT * FROM users.parquet WHERE NOT EXISTS (SELECT 1 FROM orders.parquet)"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check that filter is ExistsExpr
	existsExpr, ok := q.Filter.(*ExistsExpr)
	if !ok {
		t.Fatalf("Expected filter to be ExistsExpr, got %T", q.Filter)
	}

	// Check negation
	if !existsExpr.Negate {
		t.Errorf("Expected Negate to be true for NOT EXISTS")
	}
}
