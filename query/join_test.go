package query

import (
	"testing"
)

func TestParseJoin(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "INNER JOIN",
			query:   "SELECT * FROM users.parquet INNER JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "LEFT JOIN",
			query:   "SELECT * FROM users.parquet LEFT JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "LEFT OUTER JOIN",
			query:   "SELECT * FROM users.parquet LEFT OUTER JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "RIGHT JOIN",
			query:   "SELECT * FROM users.parquet RIGHT JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "FULL JOIN",
			query:   "SELECT * FROM users.parquet FULL JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "FULL OUTER JOIN",
			query:   "SELECT * FROM users.parquet FULL OUTER JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "CROSS JOIN",
			query:   "SELECT * FROM users.parquet CROSS JOIN orders.parquet",
			wantErr: false,
		},
		{
			name:    "Plain JOIN (defaults to INNER)",
			query:   "SELECT * FROM users.parquet JOIN orders.parquet ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "Multiple JOINs",
			query:   "SELECT * FROM users.parquet JOIN orders.parquet ON users.id = orders.user_id JOIN products.parquet ON orders.product_id = products.id",
			wantErr: false,
		},
		{
			name:    "JOIN with table aliases",
			query:   "SELECT * FROM users.parquet u JOIN orders.parquet o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "JOIN with WHERE clause",
			query:   "SELECT * FROM users.parquet u JOIN orders.parquet o ON u.id = o.user_id WHERE u.age > 30",
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

			if err == nil {
				// Verify JOIN was parsed
				if len(q.Joins) == 0 {
					t.Errorf("Parse() expected JOIN to be parsed, got no JOINs")
				}
			}
		})
	}
}

func TestJoinTypes(t *testing.T) {
	tests := []struct {
		query    string
		joinType JoinType
	}{
		{"SELECT * FROM a.parquet INNER JOIN b.parquet ON a.id = b.id", JoinInner},
		{"SELECT * FROM a.parquet LEFT JOIN b.parquet ON a.id = b.id", JoinLeft},
		{"SELECT * FROM a.parquet RIGHT JOIN b.parquet ON a.id = b.id", JoinRight},
		{"SELECT * FROM a.parquet FULL JOIN b.parquet ON a.id = b.id", JoinFull},
		{"SELECT * FROM a.parquet CROSS JOIN b.parquet", JoinCross},
		{"SELECT * FROM a.parquet JOIN b.parquet ON a.id = b.id", JoinInner}, // defaults to INNER
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(q.Joins) == 0 {
				t.Fatalf("Parse() expected JOIN to be parsed")
			}

			if q.Joins[0].Type != tt.joinType {
				t.Errorf("Parse() join type = %v, want %v", q.Joins[0].Type, tt.joinType)
			}
		})
	}
}

func TestJoinWithAliases(t *testing.T) {
	query := "SELECT u.name, o.amount FROM users.parquet u JOIN orders.parquet o ON u.id = o.user_id"

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check table alias
	if q.TableAlias != "u" {
		t.Errorf("Parse() table alias = %q, want %q", q.TableAlias, "u")
	}

	// Check join alias
	if len(q.Joins) == 0 {
		t.Fatalf("Parse() expected JOIN to be parsed")
	}

	if q.Joins[0].Alias != "o" {
		t.Errorf("Parse() join alias = %q, want %q", q.Joins[0].Alias, "o")
	}
}

func TestCrossJoinNoCondition(t *testing.T) {
	query := "SELECT * FROM a.parquet CROSS JOIN b.parquet"

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(q.Joins) == 0 {
		t.Fatalf("Parse() expected JOIN to be parsed")
	}

	// CROSS JOIN should not have a condition
	if q.Joins[0].Condition != nil {
		t.Errorf("Parse() CROSS JOIN should not have condition, got %v", q.Joins[0].Condition)
	}
}

func TestMultipleJoins(t *testing.T) {
	query := "SELECT * FROM users.parquet u JOIN orders.parquet o ON u.id = o.user_id JOIN products.parquet p ON o.product_id = p.id"

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(q.Joins) != 2 {
		t.Errorf("Parse() expected 2 JOINs, got %d", len(q.Joins))
	}

	// Check first join
	if q.Joins[0].TableName != "orders.parquet" {
		t.Errorf("Parse() first join table = %q, want %q", q.Joins[0].TableName, "orders.parquet")
	}

	// Check second join
	if q.Joins[1].TableName != "products.parquet" {
		t.Errorf("Parse() second join table = %q, want %q", q.Joins[1].TableName, "products.parquet")
	}
}

func TestJoinWithSubquery(t *testing.T) {
	query := "SELECT * FROM users.parquet u JOIN (SELECT * FROM orders.parquet WHERE amount > 100) o ON u.id = o.user_id"

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(q.Joins) == 0 {
		t.Fatalf("Parse() expected JOIN to be parsed")
	}

	// Check that join has subquery
	if q.Joins[0].Subquery == nil {
		t.Errorf("Parse() expected JOIN with subquery, got nil")
	}

	// Check subquery table name
	if q.Joins[0].Subquery.TableName != "orders.parquet" {
		t.Errorf("Parse() JOIN subquery table = %q, want %q", q.Joins[0].Subquery.TableName, "orders.parquet")
	}
}
