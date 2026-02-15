package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/vegasq/parcat/reader"
)
func TestParquetGroupBy(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 30, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 25, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 35, Salary: 48000.0, Active: false, Score: 88.1},
		{ID: 6, Name: "Frank", Age: 30, Salary: 55000.0, Active: true, Score: 82.0},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "group by single column",
			queryTpl: "SELECT age FROM '%s' GROUP BY age",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				ages := make(map[int64]bool)
				for _, row := range rows {
					age := row["age"].(int64)
					if ages[age] {
						t.Errorf("Duplicate age %d in grouped results", age)
					}
					ages[age] = true
				}
			},
		},
		{
			name:     "group by with count",
			queryTpl: "SELECT age, COUNT(*) as count FROM '%s' GROUP BY age",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					age := row["age"].(int64)
					count := row["count"].(int64)
					if age == 30 && count != 3 {
						t.Errorf("Expected count 3 for age 30, got %d", count)
					}
					if age == 25 && count != 2 {
						t.Errorf("Expected count 2 for age 25, got %d", count)
					}
					if age == 35 && count != 1 {
						t.Errorf("Expected count 1 for age 35, got %d", count)
					}
				}
			},
		},
		{
			name:     "group by boolean column",
			queryTpl: "SELECT active, COUNT(*) as count FROM '%s' GROUP BY active",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					active := row["active"].(bool)
					count := row["count"].(int64)
					if active && count != 4 {
						t.Errorf("Expected count 4 for active=true, got %d", count)
					}
					if !active && count != 2 {
						t.Errorf("Expected count 2 for active=false, got %d", count)
					}
				}
			},
		},
		{
			name:     "group by with order",
			queryTpl: "SELECT age, COUNT(*) as count FROM '%s' GROUP BY age ORDER BY age ASC",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i := 1; i < len(rows); i++ {
					prev := rows[i-1]["age"].(int64)
					curr := rows[i]["age"].(int64)
					if prev > curr {
						t.Errorf("Ages not in ascending order")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile)
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

// TestParquetHaving tests HAVING clause for post-aggregation filtering
func TestParquetHaving(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 30, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 25, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 35, Salary: 48000.0, Active: false, Score: 88.1},
		{ID: 6, Name: "Frank", Age: 30, Salary: 55000.0, Active: true, Score: 82.0},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "having with count filter",
			queryTpl: "SELECT age, COUNT(*) as count FROM '%s' GROUP BY age HAVING count > 1",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					count := row["count"].(int64)
					if count <= 1 {
						t.Errorf("Expected count > 1, got %d", count)
					}
				}
			},
		},
		{
			name:     "having with aggregate comparison",
			queryTpl: "SELECT age, AVG(salary) as avg_salary FROM '%s' GROUP BY age HAVING avg_salary > 50000",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					avgSalary := row["avg_salary"].(float64)
					if avgSalary <= 50000 {
						t.Errorf("Expected avg_salary > 50000, got %f", avgSalary)
					}
				}
			},
		},
		{
			name:     "having with multiple conditions",
			queryTpl: "SELECT age, COUNT(*) as count, AVG(score) as avg_score FROM '%s' GROUP BY age HAVING count >= 2 AND avg_score > 80",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					count := row["count"].(int64)
					avgScore := row["avg_score"].(float64)
					if count < 2 {
						t.Errorf("Expected count >= 2, got %d", count)
					}
					if avgScore <= 80 {
						t.Errorf("Expected avg_score > 80, got %f", avgScore)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile)
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

// TestParquetAggregates tests standard aggregation functions
func TestParquetAggregates(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 28, Salary: 52000.0, Active: true, Score: 78.9},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "count all",
			queryTpl: "SELECT COUNT(*) as total FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if rows[0]["total"].(int64) != 4 {
					t.Errorf("Expected count 4, got %d", rows[0]["total"].(int64))
				}
			},
		},
		{
			name:     "count column",
			queryTpl: "SELECT COUNT(name) as name_count FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if rows[0]["name_count"].(int64) != 4 {
					t.Errorf("Expected count 4, got %d", rows[0]["name_count"].(int64))
				}
			},
		},
		{
			name:     "sum aggregate",
			queryTpl: "SELECT SUM(salary) as total_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := 207000.0
				if rows[0]["total_salary"].(float64) != expected {
					t.Errorf("Expected sum %f, got %f", expected, rows[0]["total_salary"].(float64))
				}
			},
		},
		{
			name:     "avg aggregate",
			queryTpl: "SELECT AVG(age) as avg_age FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := 29.5
				if rows[0]["avg_age"].(float64) != expected {
					t.Errorf("Expected avg %f, got %f", expected, rows[0]["avg_age"].(float64))
				}
			},
		},
		{
			name:     "min aggregate",
			queryTpl: "SELECT MIN(score) as min_score FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := 72.3
				if rows[0]["min_score"].(float64) != expected {
					t.Errorf("Expected min %f, got %f", expected, rows[0]["min_score"].(float64))
				}
			},
		},
		{
			name:     "max aggregate",
			queryTpl: "SELECT MAX(salary) as max_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := 60000.0
				if rows[0]["max_salary"].(float64) != expected {
					t.Errorf("Expected max %f, got %f", expected, rows[0]["max_salary"].(float64))
				}
			},
		},
		{
			name:     "multiple aggregates",
			queryTpl: "SELECT COUNT(*) as cnt, SUM(salary) as total, AVG(age) as avg_age, MIN(score) as min_score, MAX(score) as max_score FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				row := rows[0]
				if row["cnt"].(int64) != 4 {
					t.Errorf("Expected count 4, got %d", row["cnt"].(int64))
				}
				if row["total"].(float64) != 207000.0 {
					t.Errorf("Expected total 207000, got %f", row["total"].(float64))
				}
				if row["avg_age"].(float64) != 29.5 {
					t.Errorf("Expected avg_age 29.5, got %f", row["avg_age"].(float64))
				}
				if row["min_score"].(float64) != 72.3 {
					t.Errorf("Expected min_score 72.3, got %f", row["min_score"].(float64))
				}
				if row["max_score"].(float64) != 91.2 {
					t.Errorf("Expected max_score 91.2, got %f", row["max_score"].(float64))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile)
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

// TestParquetGroupByMultipleColumns tests grouping by multiple columns
func TestParquetGroupByMultipleColumns(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 30, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 25, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 30, Salary: 48000.0, Active: false, Score: 88.1},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "group by two columns",
			queryTpl: "SELECT age, active, COUNT(*) as count FROM '%s' GROUP BY age, active",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				seen := make(map[string]bool)
				for _, row := range rows {
					age := row["age"].(int64)
					active := row["active"].(bool)
					key := fmt.Sprintf("%d_%t", age, active)
					if seen[key] {
						t.Errorf("Duplicate group %s", key)
					}
					seen[key] = true
				}
			},
		},
		{
			name:     "group by two columns with aggregates",
			queryTpl: "SELECT age, active, COUNT(*) as count, AVG(salary) as avg_salary FROM '%s' GROUP BY age, active ORDER BY age, active",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					age := row["age"].(int64)
					active := row["active"].(bool)
					count := row["count"].(int64)

					if age == 30 && active {
						if count != 2 {
							t.Errorf("Expected count 2 for age=30, active=true, got %d", count)
						}
					}
				}
			},
		},
		{
			name:     "group by multiple columns with having",
			queryTpl: "SELECT age, active, COUNT(*) as count FROM '%s' GROUP BY age, active HAVING count > 1",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					count := row["count"].(int64)
					if count <= 1 {
						t.Errorf("Expected count > 1, got %d", count)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile)
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

// TestParquetAggregateWithNulls tests aggregation with null values
func TestParquetAggregateWithNulls(t *testing.T) {
	now := time.Now()
	testData := []ComplexDataRow{
		{ID: 1, Name: "Alice", Age: int64Ptr(30), Timestamp: now, Salary: float64Ptr(50000.0), Score: float64Ptr(85.5)},
		{ID: 2, Name: "Bob", Age: int64Ptr(25), Timestamp: now, Salary: nil, Score: float64Ptr(72.3)},
		{ID: 3, Name: "Charlie", Age: int64Ptr(35), Timestamp: now, Salary: float64Ptr(60000.0), Score: nil},
		{ID: 4, Name: "Diana", Age: int64Ptr(28), Timestamp: now, Salary: nil, Score: nil},
	}

	testFile := createComplexParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "count star includes nulls",
			queryTpl: "SELECT COUNT(*) as total FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if rows[0]["total"].(int64) != 4 {
					t.Errorf("Expected count 4, got %d", rows[0]["total"].(int64))
				}
			},
		},
		{
			name:     "count column excludes nulls",
			queryTpl: "SELECT COUNT(salary) as salary_count FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if rows[0]["salary_count"].(int64) != 2 {
					t.Errorf("Expected count 2 (non-null salaries), got %d", rows[0]["salary_count"].(int64))
				}
			},
		},
		{
			name:     "avg excludes nulls",
			queryTpl: "SELECT AVG(score) as avg_score FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := (85.5 + 72.3) / 2.0
				avgScore := rows[0]["avg_score"].(float64)
				if avgScore != expected {
					t.Errorf("Expected avg %f (excluding nulls), got %f", expected, avgScore)
				}
			},
		},
		{
			name:     "sum excludes nulls",
			queryTpl: "SELECT SUM(salary) as total_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				expected := 110000.0 // 50000 + 60000
				totalSalary := rows[0]["total_salary"].(float64)
				if totalSalary != expected {
					t.Errorf("Expected sum %f (excluding nulls), got %f", expected, totalSalary)
				}
			},
		},
		{
			name:     "count with group by and nulls",
			queryTpl: "SELECT age, COUNT(salary) as salary_count, COUNT(*) as total FROM '%s' GROUP BY age",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					total := row["total"].(int64)
					salaryCount := row["salary_count"].(int64)
					if total < salaryCount {
						t.Errorf("Total count should be >= salary count")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile)
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

