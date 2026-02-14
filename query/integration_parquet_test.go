package query

import (
	"fmt"
	"testing"

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
