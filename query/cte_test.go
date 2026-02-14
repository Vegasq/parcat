package query

import (
	"testing"
)

func TestParseCTE(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple CTE",
			query:   "WITH cte AS (SELECT * FROM data.parquet) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "multiple CTEs",
			query:   "WITH cte1 AS (SELECT * FROM data.parquet), cte2 AS (SELECT * FROM other.parquet) SELECT * FROM cte1",
			wantErr: false,
		},
		{
			name:    "CTE with WHERE",
			query:   "WITH active_users AS (SELECT * FROM users.parquet WHERE active = true) SELECT * FROM active_users",
			wantErr: false,
		},
		{
			name:    "recursive CTE not supported",
			query:   "WITH RECURSIVE cte AS (SELECT * FROM data.parquet) SELECT * FROM cte",
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
			if err == nil {
				if len(q.CTEs) == 0 && !tt.wantErr {
					t.Errorf("Expected CTEs to be parsed, but got none")
				}
			}
		})
	}
}

func TestParseCTEStructure(t *testing.T) {
	query := "WITH active_users AS (SELECT name, age FROM users.parquet WHERE active = true) SELECT * FROM active_users"
	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check CTE count
	if len(q.CTEs) != 1 {
		t.Errorf("Expected 1 CTE, got %d", len(q.CTEs))
	}

	// Check CTE name
	if q.CTEs[0].Name != "active_users" {
		t.Errorf("Expected CTE name 'active_users', got %q", q.CTEs[0].Name)
	}

	// Check CTE query
	if q.CTEs[0].Query == nil {
		t.Errorf("Expected CTE query to be non-nil")
	}

	// Check CTE query has SELECT list
	if len(q.CTEs[0].Query.SelectList) != 2 {
		t.Errorf("Expected CTE query to have 2 select items, got %d", len(q.CTEs[0].Query.SelectList))
	}

	// Check main query references CTE
	if q.TableName != "active_users" {
		t.Errorf("Expected main query to reference CTE 'active_users', got %q", q.TableName)
	}
}

func TestParseMultipleCTEs(t *testing.T) {
	query := `WITH
		cte1 AS (SELECT * FROM data1.parquet WHERE status = 'active'),
		cte2 AS (SELECT * FROM data2.parquet WHERE type = 'premium')
	SELECT * FROM cte1`

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Check CTE count
	if len(q.CTEs) != 2 {
		t.Errorf("Expected 2 CTEs, got %d", len(q.CTEs))
	}

	// Check CTE names
	if q.CTEs[0].Name != "cte1" {
		t.Errorf("Expected first CTE name 'cte1', got %q", q.CTEs[0].Name)
	}
	if q.CTEs[1].Name != "cte2" {
		t.Errorf("Expected second CTE name 'cte2', got %q", q.CTEs[1].Name)
	}
}
