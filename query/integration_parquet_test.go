package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/vegasq/parcat/reader"
)

// TestParquetFilter tests WHERE clause variations with real parquet files
func TestParquetFilter(t *testing.T) {
	// Create test data with varied values for filtering
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 28, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 25, Salary: 48000.0, Active: false, Score: 88.1},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string // query template with %s for file path
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "filter with equals",
			queryTpl: "SELECT * FROM '%s' WHERE age = 25",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["age"].(int64) != 25 {
						t.Errorf("Expected age = 25, got %v", row["age"])
					}
				}
			},
		},
		{
			name:     "filter with not equals",
			queryTpl: "SELECT * FROM '%s' WHERE active != true",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["active"].(bool) != false {
						t.Errorf("Expected active = false, got %v", row["active"])
					}
				}
			},
		},
		{
			name:     "filter with greater than",
			queryTpl: "SELECT * FROM '%s' WHERE salary > 50000",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["salary"].(float64) <= 50000 {
						t.Errorf("Expected salary > 50000, got %v", row["salary"])
					}
				}
			},
		},
		{
			name:     "filter with less than",
			queryTpl: "SELECT * FROM '%s' WHERE score < 80",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["score"].(float64) >= 80 {
						t.Errorf("Expected score < 80, got %v", row["score"])
					}
				}
			},
		},
		{
			name:     "filter with greater than or equal",
			queryTpl: "SELECT * FROM '%s' WHERE age >= 30",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["age"].(int64) < 30 {
						t.Errorf("Expected age >= 30, got %v", row["age"])
					}
				}
			},
		},
		{
			name:     "filter with less than or equal",
			queryTpl: "SELECT * FROM '%s' WHERE salary <= 50000",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["salary"].(float64) > 50000 {
						t.Errorf("Expected salary <= 50000, got %v", row["salary"])
					}
				}
			},
		},
		{
			name:     "filter with AND",
			queryTpl: "SELECT * FROM '%s' WHERE age > 25 AND active = true",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["age"].(int64) <= 25 {
						t.Errorf("Expected age > 25, got %v", row["age"])
					}
					if row["active"].(bool) != true {
						t.Errorf("Expected active = true, got %v", row["active"])
					}
				}
			},
		},
		{
			name:     "filter with OR",
			queryTpl: "SELECT * FROM '%s' WHERE age < 26 OR age > 33",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					age := row["age"].(int64)
					if age >= 26 && age <= 33 {
						t.Errorf("Expected age < 26 OR age > 33, got %v", age)
					}
				}
			},
		},
		{
			name:     "complex filter with AND and OR",
			queryTpl: "SELECT * FROM '%s' WHERE age > 25 AND active = true OR salary > 55000.0",
			wantRows: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Format query with actual file path
			query := fmt.Sprintf(tt.queryTpl, testFile)

			// Parse query
			q, err := Parse(query)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			// Create reader
			r, err := reader.NewReader(testFile)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer r.Close()

			// Execute query
			results, err := ExecuteQuery(q, r)
			if err != nil {
				t.Fatalf("ExecuteQuery() error = %v", err)
			}

			// Verify row count
			if len(results) != tt.wantRows {
				t.Errorf("Expected %d rows, got %d", tt.wantRows, len(results))
			}

			// Run custom validation if provided
			if tt.validate != nil {
				tt.validate(t, results)
			}
		})
	}
}

// TestParquetProjection tests column selection and aliasing with real parquet files
func TestParquetProjection(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name         string
		queryTpl     string
		wantRows     int
		wantColumns  []string
		checkColumns bool
	}{
		{
			name:         "select all columns",
			queryTpl:     "SELECT * FROM '%s'",
			wantRows:     2,
			checkColumns: false,
		},
		{
			name:         "select single column",
			queryTpl:     "SELECT name FROM '%s'",
			wantRows:     2,
			wantColumns:  []string{"name"},
			checkColumns: true,
		},
		{
			name:         "select multiple columns",
			queryTpl:     "SELECT id, name, age FROM '%s'",
			wantRows:     2,
			wantColumns:  []string{"id", "name", "age"},
			checkColumns: true,
		},
		{
			name:         "select with alias single",
			queryTpl:     "SELECT name AS user_name FROM '%s'",
			wantRows:     2,
			wantColumns:  []string{"user_name"},
			checkColumns: true,
		},
		{
			name:         "select with alias multiple",
			queryTpl:     "SELECT id AS user_id, name AS user_name, age AS years FROM '%s'",
			wantRows:     2,
			wantColumns:  []string{"user_id", "user_name", "years"},
			checkColumns: true,
		},
		{
			name:         "select mixed with and without alias",
			queryTpl:     "SELECT id, name AS user_name, age FROM '%s'",
			wantRows:     2,
			wantColumns:  []string{"id", "user_name", "age"},
			checkColumns: true,
		},
		{
			name:         "select with projection and filter",
			queryTpl:     "SELECT name, salary FROM '%s' WHERE age > 25",
			wantRows:     1,
			wantColumns:  []string{"name", "salary"},
			checkColumns: true,
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

			if tt.checkColumns && len(results) > 0 {
				firstRow := results[0]
				if len(firstRow) != len(tt.wantColumns) {
					t.Errorf("Expected %d columns, got %d", len(tt.wantColumns), len(firstRow))
				}

				for _, col := range tt.wantColumns {
					if _, exists := firstRow[col]; !exists {
						t.Errorf("Expected column %q not found in results", col)
					}
				}
			}
		})
	}
}

// TestParquetDistinct tests DISTINCT keyword with real parquet files
func TestParquetDistinct(t *testing.T) {
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
	}{
		{
			name:     "distinct single column",
			queryTpl: "SELECT DISTINCT age FROM '%s'",
			wantRows: 2,
		},
		{
			name:     "distinct boolean column",
			queryTpl: "SELECT DISTINCT active FROM '%s'",
			wantRows: 2,
		},
		{
			name:     "distinct multiple columns",
			queryTpl: "SELECT DISTINCT age, active FROM '%s'",
			wantRows: 4,
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
				t.Errorf("Expected %d distinct rows, got %d", tt.wantRows, len(results))
			}
		})
	}
}

// TestParquetLimitOffset tests LIMIT and OFFSET for pagination with real parquet files
func TestParquetLimitOffset(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 28, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 25, Salary: 48000.0, Active: false, Score: 88.1},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
	}{
		{
			name:     "limit only",
			queryTpl: "SELECT * FROM '%s' LIMIT 3",
			wantRows: 3,
		},
		{
			name:     "limit 1",
			queryTpl: "SELECT * FROM '%s' LIMIT 1",
			wantRows: 1,
		},
		{
			name:     "limit larger than dataset",
			queryTpl: "SELECT * FROM '%s' LIMIT 100",
			wantRows: 5,
		},
		{
			name:     "offset only",
			queryTpl: "SELECT * FROM '%s' OFFSET 2",
			wantRows: 3,
		},
		{
			name:     "offset at end",
			queryTpl: "SELECT * FROM '%s' OFFSET 4",
			wantRows: 1,
		},
		{
			name:     "limit and offset",
			queryTpl: "SELECT * FROM '%s' LIMIT 2 OFFSET 1",
			wantRows: 2,
		},
		{
			name:     "limit and offset pagination",
			queryTpl: "SELECT * FROM '%s' LIMIT 2 OFFSET 2",
			wantRows: 2,
		},
		{
			name:     "limit and offset last page",
			queryTpl: "SELECT * FROM '%s' LIMIT 2 OFFSET 4",
			wantRows: 1,
		},
		{
			name:     "offset beyond data",
			queryTpl: "SELECT * FROM '%s' OFFSET 10",
			wantRows: 0,
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
		})
	}
}

// TestParquetOrderBy tests ORDER BY with ASC/DESC and multiple columns
func TestParquetOrderBy(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Charlie", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Alice", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Bob", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
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
			name:     "order by single column asc",
			queryTpl: "SELECT * FROM '%s' ORDER BY age ASC",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i := 1; i < len(rows); i++ {
					prev := rows[i-1]["age"].(int64)
					curr := rows[i]["age"].(int64)
					if prev > curr {
						t.Errorf("Row %d: age not in ascending order: %d > %d", i, prev, curr)
					}
				}
			},
		},
		{
			name:     "order by single column desc",
			queryTpl: "SELECT * FROM '%s' ORDER BY salary DESC",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i := 1; i < len(rows); i++ {
					prev := rows[i-1]["salary"].(float64)
					curr := rows[i]["salary"].(float64)
					if prev < curr {
						t.Errorf("Row %d: salary not in descending order: %f < %f", i, prev, curr)
					}
				}
			},
		},
		{
			name:     "order by string column",
			queryTpl: "SELECT * FROM '%s' ORDER BY name ASC",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i := 1; i < len(rows); i++ {
					prev := rows[i-1]["name"].(string)
					curr := rows[i]["name"].(string)
					if prev > curr {
						t.Errorf("Row %d: name not in ascending order: %s > %s", i, prev, curr)
					}
				}
			},
		},
		{
			name:     "order by multiple columns",
			queryTpl: "SELECT * FROM '%s' ORDER BY age ASC, salary DESC",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i := 1; i < len(rows); i++ {
					prevAge := rows[i-1]["age"].(int64)
					currAge := rows[i]["age"].(int64)
					if prevAge > currAge {
						t.Errorf("Row %d: age not in ascending order", i)
					}
					// If ages are equal, check salary descending
					if prevAge == currAge {
						prevSalary := rows[i-1]["salary"].(float64)
						currSalary := rows[i]["salary"].(float64)
						if prevSalary < currSalary {
							t.Errorf("Row %d: for same age, salary not in descending order", i)
						}
					}
				}
			},
		},
		{
			name:     "order by with limit",
			queryTpl: "SELECT * FROM '%s' ORDER BY score DESC LIMIT 3",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Check descending order
				for i := 1; i < len(rows); i++ {
					prev := rows[i-1]["score"].(float64)
					curr := rows[i]["score"].(float64)
					if prev < curr {
						t.Errorf("Row %d: score not in descending order", i)
					}
				}
				// Check that first row has highest score
				if rows[0]["score"].(float64) != 91.2 {
					t.Errorf("Expected highest score 91.2, got %f", rows[0]["score"].(float64))
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

// TestParquetGroupBy tests various GROUP BY scenarios with real parquet files
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

// TestParquetInnerJoin tests INNER JOIN with real parquet files
func TestParquetInnerJoin(t *testing.T) {
	tmpDir := t.TempDir()

	// Create users data
	usersData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
	}
	usersFile := createNamedBasicParquetFile(t, tmpDir, "users.parquet", usersData)

	// Create orders data (using ID as user_id, reusing BasicDataRow for simplicity)
	ordersData := []BasicDataRow{
		{ID: 101, Name: "Order-A", Age: 1, Salary: 250.0, Active: true, Score: 0},  // user_id=1 (Age field)
		{ID: 102, Name: "Order-B", Age: 1, Salary: 175.0, Active: true, Score: 0},  // user_id=1
		{ID: 103, Name: "Order-C", Age: 2, Salary: 300.0, Active: false, Score: 0}, // user_id=2
	}
	ordersFile := createNamedBasicParquetFile(t, tmpDir, "orders.parquet", ordersData)

	tests := []struct {
		name       string
		queryTpl   string
		wantRows   int
		validate   func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "basic inner join",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Alice should have 2 orders, Bob should have 1
				aliceCount := 0
				bobCount := 0
				for _, row := range rows {
					userName := row["u.name"].(string)
					if userName == "Alice" {
						aliceCount++
					} else if userName == "Bob" {
						bobCount++
					}
				}
				if aliceCount != 2 {
					t.Errorf("Expected 2 orders for Alice, got %d", aliceCount)
				}
				if bobCount != 1 {
					t.Errorf("Expected 1 order for Bob, got %d", bobCount)
				}
			},
		},
		{
			name:     "inner join with where clause",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age WHERE u.age > 25",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					userName := row["u.name"].(string)
					if userName != "Alice" {
						t.Errorf("Expected only Alice (age > 25), got %s", userName)
					}
				}
			},
		},
		{
			name:     "inner join with aggregation",
			queryTpl: "SELECT u.name, COUNT(*) as order_count FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age GROUP BY u.name",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					userName := row["u.name"].(string)
					count := row["order_count"].(int64)
					if userName == "Alice" && count != 2 {
						t.Errorf("Expected 2 orders for Alice, got %d", count)
					}
					if userName == "Bob" && count != 1 {
						t.Errorf("Expected 1 order for Bob, got %d", count)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			// Set actual file paths
			q.TableName = usersFile
			if len(q.Joins) > 0 {
				q.Joins[0].TableName = ordersFile
			}

			r, err := reader.NewReader(usersFile)
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

// TestParquetLeftJoin tests LEFT JOIN with real parquet files
func TestParquetLeftJoin(t *testing.T) {
	tmpDir := t.TempDir()

	// Create users data
	usersData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
	}
	usersFile := createNamedBasicParquetFile(t, tmpDir, "users.parquet", usersData)

	// Create orders data - only for users 1 and 2
	ordersData := []BasicDataRow{
		{ID: 101, Name: "Order-A", Age: 1, Salary: 250.0, Active: true, Score: 0},
		{ID: 102, Name: "Order-B", Age: 2, Salary: 175.0, Active: true, Score: 0},
	}
	ordersFile := createNamedBasicParquetFile(t, tmpDir, "orders.parquet", ordersData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "left join includes unmatched left rows",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u LEFT JOIN orders.parquet o ON u.id = o.age",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				foundCharlie := false
				for _, row := range rows {
					userName := row["u.name"].(string)
					if userName == "Charlie" {
						foundCharlie = true
						// Charlie should have null order name
						if row["o.name"] != nil {
							t.Errorf("Expected null order for Charlie, got %v", row["o.name"])
						}
					}
				}
				if !foundCharlie {
					t.Errorf("Expected to find Charlie in LEFT JOIN results")
				}
			},
		},
		{
			name:     "left join with where on left table",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u LEFT JOIN orders.parquet o ON u.id = o.age WHERE u.age >= 30",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					userName := row["u.name"].(string)
					if userName == "Bob" {
						t.Errorf("Expected only users with age >= 30, got Bob")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			q.TableName = usersFile
			if len(q.Joins) > 0 {
				q.Joins[0].TableName = ordersFile
			}

			r, err := reader.NewReader(usersFile)
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

// TestParquetRightJoin tests RIGHT JOIN with real parquet files
func TestParquetRightJoin(t *testing.T) {
	tmpDir := t.TempDir()

	// Create users data - only users 1 and 2
	usersData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
	}
	usersFile := createNamedBasicParquetFile(t, tmpDir, "users.parquet", usersData)

	// Create orders data - including user_id 3 which doesn't exist in users
	ordersData := []BasicDataRow{
		{ID: 101, Name: "Order-A", Age: 1, Salary: 250.0, Active: true, Score: 0},
		{ID: 102, Name: "Order-B", Age: 2, Salary: 175.0, Active: true, Score: 0},
		{ID: 103, Name: "Order-C", Age: 3, Salary: 300.0, Active: false, Score: 0},
	}
	ordersFile := createNamedBasicParquetFile(t, tmpDir, "orders.parquet", ordersData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "right join includes unmatched right rows",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u RIGHT JOIN orders.parquet o ON u.id = o.age",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				foundOrphanOrder := false
				for _, row := range rows {
					orderName := row["o.name"].(string)
					if orderName == "Order-C" {
						foundOrphanOrder = true
						// Order-C should have null user name
						if row["u.name"] != nil {
							t.Errorf("Expected null user for Order-C, got %v", row["u.name"])
						}
					}
				}
				if !foundOrphanOrder {
					t.Errorf("Expected to find Order-C in RIGHT JOIN results")
				}
			},
		},
		{
			name:     "right join with where on right table",
			queryTpl: "SELECT u.name, o.name FROM users.parquet u RIGHT JOIN orders.parquet o ON u.id = o.age WHERE o.salary > 200",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					orderName := row["o.name"].(string)
					if orderName == "Order-B" {
						t.Errorf("Expected only orders with salary > 200, got Order-B")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			q.TableName = usersFile
			if len(q.Joins) > 0 {
				q.Joins[0].TableName = ordersFile
			}

			r, err := reader.NewReader(usersFile)
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

// TestParquetFullJoin tests FULL JOIN with real parquet files
func TestParquetFullJoin(t *testing.T) {
	tmpDir := t.TempDir()

	// Create table1 with IDs 1, 2
	table1Data := []BasicDataRow{
		{ID: 1, Name: "A1", Age: 10, Salary: 1000.0, Active: true, Score: 50.0},
		{ID: 2, Name: "A2", Age: 20, Salary: 2000.0, Active: false, Score: 60.0},
	}
	table1File := createNamedBasicParquetFile(t, tmpDir, "table1.parquet", table1Data)

	// Create table2 with IDs 2, 3
	table2Data := []BasicDataRow{
		{ID: 2, Name: "B2", Age: 25, Salary: 2500.0, Active: true, Score: 70.0},
		{ID: 3, Name: "B3", Age: 30, Salary: 3000.0, Active: false, Score: 80.0},
	}
	table2File := createNamedBasicParquetFile(t, tmpDir, "table2.parquet", table2Data)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "full join includes all rows",
			queryTpl: "SELECT t1.name, t2.name FROM table1.parquet t1 FULL JOIN table2.parquet t2 ON t1.id = t2.id",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				foundA1 := false
				foundB3 := false
				foundMatched := false

				for _, row := range rows {
					t1Name := row["t1.name"]
					t2Name := row["t2.name"]

					if t1Name == "A1" && t2Name == nil {
						foundA1 = true // unmatched left
					}
					if t1Name == nil && t2Name == "B3" {
						foundB3 = true // unmatched right
					}
					if t1Name == "A2" && t2Name == "B2" {
						foundMatched = true // matched
					}
				}

				if !foundA1 {
					t.Errorf("Expected to find unmatched A1")
				}
				if !foundB3 {
					t.Errorf("Expected to find unmatched B3")
				}
				if !foundMatched {
					t.Errorf("Expected to find matched A2-B2")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			q.TableName = table1File
			if len(q.Joins) > 0 {
				q.Joins[0].TableName = table2File
			}

			r, err := reader.NewReader(table1File)
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

// TestParquetCrossJoin tests CROSS JOIN with real parquet files
func TestParquetCrossJoin(t *testing.T) {
	tmpDir := t.TempDir()

	// Create small table1
	table1Data := []BasicDataRow{
		{ID: 1, Name: "A", Age: 10, Salary: 1000.0, Active: true, Score: 50.0},
		{ID: 2, Name: "B", Age: 20, Salary: 2000.0, Active: false, Score: 60.0},
	}
	table1File := createNamedBasicParquetFile(t, tmpDir, "table1.parquet", table1Data)

	// Create small table2
	table2Data := []BasicDataRow{
		{ID: 3, Name: "X", Age: 30, Salary: 3000.0, Active: true, Score: 70.0},
		{ID: 4, Name: "Y", Age: 40, Salary: 4000.0, Active: false, Score: 80.0},
	}
	table2File := createNamedBasicParquetFile(t, tmpDir, "table2.parquet", table2Data)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "cross join produces cartesian product",
			queryTpl: "SELECT t1.name, t2.name FROM table1.parquet t1 CROSS JOIN table2.parquet t2",
			wantRows: 4, // 2 x 2
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Should have all combinations: A-X, A-Y, B-X, B-Y
				combinations := make(map[string]bool)
				for _, row := range rows {
					t1Name := row["t1.name"].(string)
					t2Name := row["t2.name"].(string)
					key := t1Name + "-" + t2Name
					combinations[key] = true
				}

				expected := []string{"A-X", "A-Y", "B-X", "B-Y"}
				for _, exp := range expected {
					if !combinations[exp] {
						t.Errorf("Expected combination %s not found", exp)
					}
				}
			},
		},
		{
			name:     "cross join with where clause",
			queryTpl: "SELECT t1.name, t2.name FROM table1.parquet t1 CROSS JOIN table2.parquet t2 WHERE t1.age < t2.age",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// All combinations should satisfy t1.age < t2.age
				if len(rows) != 4 {
					t.Errorf("Expected 4 rows, got %d", len(rows))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			q.TableName = table1File
			if len(q.Joins) > 0 {
				q.Joins[0].TableName = table2File
			}

			r, err := reader.NewReader(table1File)
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

// TestParquetMultipleJoins tests queries with 3+ table joins
func TestParquetMultipleJoins(t *testing.T) {
	tmpDir := t.TempDir()

	// Create users table
	usersData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
	}
	usersFile := createNamedBasicParquetFile(t, tmpDir, "users.parquet", usersData)

	// Create orders table (Age = user_id)
	ordersData := []BasicDataRow{
		{ID: 101, Name: "Order-A", Age: 1, Salary: 250.0, Active: true, Score: 0},
		{ID: 102, Name: "Order-B", Age: 2, Salary: 175.0, Active: true, Score: 0},
	}
	ordersFile := createNamedBasicParquetFile(t, tmpDir, "orders.parquet", ordersData)

	// Create products table (Age = order_id using first 3 digits)
	productsData := []BasicDataRow{
		{ID: 1001, Name: "Product-X", Age: 101, Salary: 100.0, Active: true, Score: 95.0},
		{ID: 1002, Name: "Product-Y", Age: 102, Salary: 75.0, Active: false, Score: 88.0},
	}
	productsFile := createNamedBasicParquetFile(t, tmpDir, "products.parquet", productsData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "three table inner join",
			queryTpl: "SELECT u.name, o.name, p.name FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age INNER JOIN products.parquet p ON o.id = p.age",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["u.name"] == nil || row["o.name"] == nil || row["p.name"] == nil {
						t.Errorf("Expected all names to be non-null in inner join")
					}
				}
			},
		},
		{
			name:     "three table join with aggregation",
			queryTpl: "SELECT u.name, COUNT(*) as total FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age INNER JOIN products.parquet p ON o.id = p.age GROUP BY u.name",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					count := row["total"].(int64)
					if count != 1 {
						t.Errorf("Expected count 1 for each user, got %d", count)
					}
				}
			},
		},
		{
			name:     "three table join with where",
			queryTpl: "SELECT u.name, o.name, p.name FROM users.parquet u INNER JOIN orders.parquet o ON u.id = o.age INNER JOIN products.parquet p ON o.id = p.age WHERE u.age > 25",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					userName := row["u.name"].(string)
					if userName != "Alice" {
						t.Errorf("Expected only Alice (age > 25), got %s", userName)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.queryTpl)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			q.TableName = usersFile
			if len(q.Joins) >= 1 {
				q.Joins[0].TableName = ordersFile
			}
			if len(q.Joins) >= 2 {
				q.Joins[1].TableName = productsFile
			}

			r, err := reader.NewReader(usersFile)
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
