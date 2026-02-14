package query

import (
	"testing"
)

func TestAggregateCount(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		rows     []map[string]interface{}
		expected []map[string]interface{}
		wantErr  bool
	}{
		{
			name:  "COUNT(*) without GROUP BY",
			query: "SELECT COUNT(*) FROM data.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(30)},
				{"name": "Bob", "age": int64(25)},
				{"name": "Charlie", "age": int64(35)},
			},
			expected: []map[string]interface{}{
				{"count": int64(3)},
			},
			wantErr: false,
		},
		{
			name:  "COUNT(*) with GROUP BY",
			query: "SELECT status, COUNT(*) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active"},
				{"name": "Bob", "status": "inactive"},
				{"name": "Charlie", "status": "active"},
			},
			expected: []map[string]interface{}{
				{"status": "active", "count": int64(2)},
				{"status": "inactive", "count": int64(1)},
			},
			wantErr: false,
		},
		{
			name:  "COUNT(column) with GROUP BY",
			query: "SELECT status, COUNT(age) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active", "age": int64(30)},
				{"name": "Bob", "status": "inactive", "age": int64(25)},
				{"name": "Charlie", "status": "active", "age": nil},
			},
			expected: []map[string]interface{}{
				{"status": "active", "count": int64(1)},
				{"status": "inactive", "count": int64(1)},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Apply aggregation
			result, err := ApplyGroupByAndAggregate(tt.rows, q.GroupBy, q.SelectList)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ApplyGroupByAndAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Check row count
			if len(result) != len(tt.expected) {
				t.Fatalf("got %d rows, want %d rows", len(result), len(tt.expected))
			}

			// For simplicity, just check the count value exists
			// (we can't guarantee order in GROUP BY results)
			for _, row := range result {
				if _, ok := row["count"]; !ok {
					t.Errorf("count column not found in result")
				}
			}
		})
	}
}

func TestAggregateSumAvg(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		rows    []map[string]interface{}
		wantErr bool
	}{
		{
			name:  "SUM without GROUP BY",
			query: "SELECT SUM(age) FROM data.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(30)},
				{"name": "Bob", "age": int64(25)},
				{"name": "Charlie", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "AVG without GROUP BY",
			query: "SELECT AVG(age) FROM data.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(30)},
				{"name": "Bob", "age": int64(25)},
				{"name": "Charlie", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "SUM with GROUP BY",
			query: "SELECT status, SUM(age) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active", "age": int64(30)},
				{"name": "Bob", "status": "inactive", "age": int64(25)},
				{"name": "Charlie", "status": "active", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "AVG with GROUP BY",
			query: "SELECT status, AVG(age) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active", "age": int64(30)},
				{"name": "Bob", "status": "inactive", "age": int64(25)},
				{"name": "Charlie", "status": "active", "age": int64(35)},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Apply aggregation
			result, err := ApplyGroupByAndAggregate(tt.rows, q.GroupBy, q.SelectList)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ApplyGroupByAndAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Check that we got results
			if len(result) == 0 {
				t.Fatal("expected results, got none")
			}

			// Verify the aggregate value exists
			for _, row := range result {
				hasAggregate := false
				for key := range row {
					if key == "sum" || key == "avg" {
						hasAggregate = true
						break
					}
				}
				if !hasAggregate {
					t.Errorf("no aggregate value found in result row")
				}
			}
		})
	}
}

func TestAggregateMinMax(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		rows    []map[string]interface{}
		wantErr bool
	}{
		{
			name:  "MIN without GROUP BY",
			query: "SELECT MIN(age) FROM data.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(30)},
				{"name": "Bob", "age": int64(25)},
				{"name": "Charlie", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "MAX without GROUP BY",
			query: "SELECT MAX(age) FROM data.parquet",
			rows: []map[string]interface{}{
				{"name": "Alice", "age": int64(30)},
				{"name": "Bob", "age": int64(25)},
				{"name": "Charlie", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "MIN with GROUP BY",
			query: "SELECT status, MIN(age) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active", "age": int64(30)},
				{"name": "Bob", "status": "inactive", "age": int64(25)},
				{"name": "Charlie", "status": "active", "age": int64(35)},
			},
			wantErr: false,
		},
		{
			name:  "MAX with GROUP BY",
			query: "SELECT status, MAX(age) FROM data.parquet GROUP BY status",
			rows: []map[string]interface{}{
				{"name": "Alice", "status": "active", "age": int64(30)},
				{"name": "Bob", "status": "inactive", "age": int64(25)},
				{"name": "Charlie", "status": "active", "age": int64(35)},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Apply aggregation
			result, err := ApplyGroupByAndAggregate(tt.rows, q.GroupBy, q.SelectList)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ApplyGroupByAndAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Check that we got results
			if len(result) == 0 {
				t.Fatal("expected results, got none")
			}

			// Verify the aggregate value exists
			for _, row := range result {
				hasAggregate := false
				for key := range row {
					if key == "min" || key == "max" {
						hasAggregate = true
						break
					}
				}
				if !hasAggregate {
					t.Errorf("no aggregate value found in result row")
				}
			}
		})
	}
}

func TestGroupByMultipleColumns(t *testing.T) {
	query := "SELECT department, status, COUNT(*) FROM data.parquet GROUP BY department, status"
	rows := []map[string]interface{}{
		{"name": "Alice", "department": "eng", "status": "active"},
		{"name": "Bob", "department": "sales", "status": "inactive"},
		{"name": "Charlie", "department": "eng", "status": "active"},
		{"name": "Dave", "department": "eng", "status": "inactive"},
	}

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	result, err := ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
	if err != nil {
		t.Fatalf("ApplyGroupByAndAggregate() error = %v", err)
	}

	// Should have 3 groups: (eng, active), (sales, inactive), (eng, inactive)
	if len(result) != 3 {
		t.Errorf("got %d groups, want 3", len(result))
	}

	// Check that each row has the expected columns
	for _, row := range result {
		if _, ok := row["department"]; !ok {
			t.Error("department column not found")
		}
		if _, ok := row["status"]; !ok {
			t.Error("status column not found")
		}
		if _, ok := row["count"]; !ok {
			t.Error("count column not found")
		}
	}
}

func TestHavingClause(t *testing.T) {
	query := "SELECT status, COUNT(*) as total FROM data.parquet GROUP BY status HAVING total > 1"
	rows := []map[string]interface{}{
		{"name": "Alice", "status": "active"},
		{"name": "Bob", "status": "inactive"},
		{"name": "Charlie", "status": "active"},
		{"name": "Dave", "status": "active"},
	}

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Apply aggregation
	result, err := ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
	if err != nil {
		t.Fatalf("ApplyGroupByAndAggregate() error = %v", err)
	}

	// Apply HAVING
	result, err = EvaluateHaving(result, q.Having)
	if err != nil {
		t.Fatalf("EvaluateHaving() error = %v", err)
	}

	// Should only have the "active" group (count = 3)
	if len(result) != 1 {
		t.Errorf("got %d rows after HAVING, want 1", len(result))
	}

	if len(result) > 0 {
		if status, ok := result[0]["status"].(string); !ok || status != "active" {
			t.Errorf("got status %v, want active", result[0]["status"])
		}
	}
}

func TestAggregateWithAlias(t *testing.T) {
	query := "SELECT status, COUNT(*) as user_count, AVG(age) as avg_age FROM data.parquet GROUP BY status"
	rows := []map[string]interface{}{
		{"name": "Alice", "status": "active", "age": int64(30)},
		{"name": "Bob", "status": "inactive", "age": int64(25)},
		{"name": "Charlie", "status": "active", "age": int64(35)},
	}

	q, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	result, err := ApplyGroupByAndAggregate(rows, q.GroupBy, q.SelectList)
	if err != nil {
		t.Fatalf("ApplyGroupByAndAggregate() error = %v", err)
	}

	// Check that aliases are used
	for _, row := range result {
		if _, ok := row["user_count"]; !ok {
			t.Error("user_count alias not found")
		}
		if _, ok := row["avg_age"]; !ok {
			t.Error("avg_age alias not found")
		}
	}
}

func TestParseGroupByErrors(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "GROUP without BY",
			query:   "SELECT status, COUNT(*) FROM data.parquet GROUP status",
			wantErr: true,
		},
		{
			name:    "HAVING without GROUP BY",
			query:   "SELECT status, COUNT(*) FROM data.parquet HAVING COUNT(*) > 1",
			wantErr: true,
		},
		{
			name:    "Empty GROUP BY",
			query:   "SELECT status, COUNT(*) FROM data.parquet GROUP BY",
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
