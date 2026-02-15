package query

import (
	"fmt"
	"testing"

	"github.com/vegasq/parcat/reader"
)

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
