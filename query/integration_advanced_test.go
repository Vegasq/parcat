package query

import (
	"fmt"
	"testing"
	"time"

	"github.com/vegasq/parcat/reader"
)
func TestParquetCTE(t *testing.T) {
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 28, Salary: 52000.0, Active: true, Score: 78.9},
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
			name:     "single CTE",
			queryTpl: "WITH high_earners AS (SELECT * FROM '%s' WHERE salary > 50000) SELECT name, salary FROM high_earners",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					salary := row["salary"].(float64)
					if salary <= 50000 {
						t.Errorf("Expected salary > 50000, got %f", salary)
					}
				}
			},
		},
		{
			name:     "CTE with aggregation",
			queryTpl: "WITH age_groups AS (SELECT age, COUNT(*) as count FROM '%s' GROUP BY age) SELECT age, count FROM age_groups WHERE count > 1",
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
		{
			name:     "multiple CTEs",
			queryTpl: "WITH active_users AS (SELECT * FROM '%s' WHERE active = true), high_scorers AS (SELECT * FROM active_users WHERE score > 80) SELECT name, score FROM high_scorers",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					score := row["score"].(float64)
					if score <= 80 {
						t.Errorf("Expected score > 80, got %f", score)
					}
				}
			},
		},
		{
			name:     "CTE with join",
			queryTpl: "WITH young_users AS (SELECT * FROM '%s' WHERE age < 30) SELECT y.name, y.age FROM young_users y",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					var age int64
					if val, ok := row["y.age"]; ok && val != nil {
						age = val.(int64)
					} else if val, ok := row["age"]; ok && val != nil {
						age = val.(int64)
					} else {
						t.Errorf("Age column not found or is nil")
						continue
					}
					if age >= 30 {
						t.Errorf("Expected age < 30, got %d", age)
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

// TestParquetSubquery tests subqueries in SELECT, FROM, and WHERE clauses
func TestParquetSubquery(t *testing.T) {
	t.Skip("Subqueries are not yet implemented in the query engine")
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
			name:     "subquery in WHERE with scalar result",
			queryTpl: "SELECT name, salary FROM '%s' WHERE salary > (SELECT AVG(salary) FROM '%s')",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				avgSalary := (50000.0 + 45000.0 + 60000.0 + 52000.0) / 4.0
				for _, row := range rows {
					salary := row["salary"].(float64)
					if salary <= avgSalary {
						t.Errorf("Expected salary > %f, got %f", avgSalary, salary)
					}
				}
			},
		},
		{
			name:     "subquery in FROM clause",
			queryTpl: "SELECT name, avg_score FROM (SELECT name, AVG(score) as avg_score FROM '%s' GROUP BY name) WHERE avg_score > 80",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					avgScore := row["avg_score"].(float64)
					if avgScore <= 80 {
						t.Errorf("Expected avg_score > 80, got %f", avgScore)
					}
				}
			},
		},
		{
			name:     "subquery with IN clause",
			queryTpl: "SELECT name FROM '%s' WHERE age IN (SELECT age FROM '%s' WHERE age >= 30)",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					if name == "Bob" || name == "Diana" {
						t.Errorf("Expected only users with age >= 30, got %s", name)
					}
				}
			},
		},
		{
			name:     "subquery in SELECT clause",
			queryTpl: "SELECT name, salary, (SELECT MAX(salary) FROM '%s') as max_salary FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					maxSalary := row["max_salary"].(float64)
					if maxSalary != 60000.0 {
						t.Errorf("Expected max_salary 60000, got %f", maxSalary)
					}
				}
			},
		},
		{
			name:     "nested subquery",
			queryTpl: "SELECT name FROM '%s' WHERE salary > (SELECT AVG(salary) FROM '%s' WHERE age > (SELECT MIN(age) FROM '%s'))",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					if name != "Charlie" && name != "Diana" {
						t.Errorf("Unexpected name in nested subquery result: %s", name)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(tt.queryTpl, testFile, testFile, testFile)
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

// TestParquetWindowFunctions tests window functions like ROW_NUMBER, RANK, LAG, LEAD, SUM OVER
func TestParquetWindowFunctions(t *testing.T) {
	t.Skip("Window functions are not yet fully implemented in the query engine")
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 30, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 25, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 35, Salary: 48000.0, Active: false, Score: 88.1},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "ROW_NUMBER window function",
			queryTpl: "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i, row := range rows {
					rank := row["rank"].(int64)
					if rank != int64(i+1) {
						t.Errorf("Row %d: expected rank %d, got %d", i, i+1, rank)
					}
				}
				// Check highest salary has rank 1
				if rows[0]["name"].(string) != "Charlie" {
					t.Errorf("Expected Charlie (highest salary) at rank 1")
				}
			},
		},
		{
			name:     "RANK window function with ties",
			queryTpl: "SELECT name, age, RANK() OVER (ORDER BY age DESC) as rank FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Eve (35) should be rank 1
				// Alice and Charlie (30) should both be rank 2
				// Bob and Diana (25) should both be rank 4
				rankCounts := make(map[int64]int)
				for _, row := range rows {
					rank := row["rank"].(int64)
					rankCounts[rank]++
				}
				if rankCounts[1] != 1 {
					t.Errorf("Expected 1 person at rank 1, got %d", rankCounts[1])
				}
				if rankCounts[2] != 2 {
					t.Errorf("Expected 2 people at rank 2, got %d", rankCounts[2])
				}
			},
		},
		{
			name:     "LAG window function",
			queryTpl: "SELECT name, salary, LAG(salary, 1) OVER (ORDER BY salary) as prev_salary FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// First row should have null prev_salary
				if rows[0]["prev_salary"] != nil {
					t.Errorf("Expected first row to have null prev_salary, got %v", rows[0]["prev_salary"])
				}
				// Check that prev_salary is less than current salary
				for i := 1; i < len(rows); i++ {
					currentSalary := rows[i]["salary"].(float64)
					prevSalary := rows[i]["prev_salary"].(float64)
					if prevSalary >= currentSalary {
						t.Errorf("Row %d: prev_salary should be less than current salary", i)
					}
				}
			},
		},
		{
			name:     "LEAD window function",
			queryTpl: "SELECT name, score, LEAD(score, 1) OVER (ORDER BY score) as next_score FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Last row should have null next_score
				if rows[len(rows)-1]["next_score"] != nil {
					t.Errorf("Expected last row to have null next_score")
				}
				// Check that next_score is greater than current score
				for i := 0; i < len(rows)-1; i++ {
					currentScore := rows[i]["score"].(float64)
					nextScore := rows[i]["next_score"].(float64)
					if nextScore <= currentScore {
						t.Errorf("Row %d: next_score should be greater than current score", i)
					}
				}
			},
		},
		{
			name:     "SUM OVER window function",
			queryTpl: "SELECT name, salary, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				var expectedTotal float64
				for i, row := range rows {
					salary := row["salary"].(float64)
					expectedTotal += salary
					runningTotal := row["running_total"].(float64)
					if runningTotal != expectedTotal {
						t.Errorf("Row %d: expected running total %f, got %f", i, expectedTotal, runningTotal)
					}
				}
			},
		},
		{
			name:     "window function with PARTITION BY",
			queryTpl: "SELECT name, age, salary, ROW_NUMBER() OVER (PARTITION BY age ORDER BY salary DESC) as rank_in_age FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				// Within each age group, verify ranking
				ageGroups := make(map[int64][]map[string]interface{})
				for _, row := range rows {
					age := row["age"].(int64)
					ageGroups[age] = append(ageGroups[age], row)
				}
				for age, group := range ageGroups {
					for i, row := range group {
						rank := row["rank_in_age"].(int64)
						if rank != int64(i+1) {
							t.Errorf("Age %d, row %d: expected rank %d, got %d", age, i, i+1, rank)
						}
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

// TestParquetCaseExpression tests CASE expressions for conditional logic
func TestParquetCaseExpression(t *testing.T) {
	t.Skip("CASE expressions are not yet implemented in the query engine")
	testData := []BasicDataRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 85.5},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 72.3},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 91.2},
		{ID: 4, Name: "Diana", Age: 28, Salary: 52000.0, Active: true, Score: 78.9},
		{ID: 5, Name: "Eve", Age: 22, Salary: 40000.0, Active: false, Score: 68.5},
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "simple CASE expression",
			queryTpl: "SELECT name, CASE WHEN age < 25 THEN 'Young' WHEN age < 30 THEN 'Mid' ELSE 'Senior' END as age_group FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					ageGroup := row["age_group"].(string)
					if name == "Eve" && ageGroup != "Young" {
						t.Errorf("Expected Eve (22) to be 'Young', got %s", ageGroup)
					}
					if name == "Bob" && ageGroup != "Mid" {
						t.Errorf("Expected Bob (25) to be 'Mid', got %s", ageGroup)
					}
					if name == "Charlie" && ageGroup != "Senior" {
						t.Errorf("Expected Charlie (35) to be 'Senior', got %s", ageGroup)
					}
				}
			},
		},
		{
			name:     "CASE with numeric result",
			queryTpl: "SELECT name, salary, CASE WHEN salary > 50000 THEN salary * 1.1 ELSE salary * 1.05 END as new_salary FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					salary := row["salary"].(float64)
					newSalary := row["new_salary"].(float64)
					var expected float64
					if salary > 50000 {
						expected = salary * 1.1
					} else {
						expected = salary * 1.05
					}
					if newSalary != expected {
						t.Errorf("Expected new_salary %f, got %f", expected, newSalary)
					}
				}
			},
		},
		{
			name:     "CASE with multiple conditions",
			queryTpl: "SELECT name, score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END as grade FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					score := row["score"].(float64)
					grade := row["grade"].(string)
					if score >= 90 && grade != "A" {
						t.Errorf("Score %f should be grade A, got %s", score, grade)
					}
					if score >= 80 && score < 90 && grade != "B" {
						t.Errorf("Score %f should be grade B, got %s", score, grade)
					}
					if score >= 70 && score < 80 && grade != "C" {
						t.Errorf("Score %f should be grade C, got %s", score, grade)
					}
					if score < 70 && grade != "F" {
						t.Errorf("Score %f should be grade F, got %s", score, grade)
					}
				}
			},
		},
		{
			name:     "CASE with boolean column",
			queryTpl: "SELECT name, active, CASE WHEN active THEN 'Active User' ELSE 'Inactive User' END as status FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					active := row["active"].(bool)
					status := row["status"].(string)
					if active && status != "Active User" {
						t.Errorf("Expected 'Active User', got %s", status)
					}
					if !active && status != "Inactive User" {
						t.Errorf("Expected 'Inactive User', got %s", status)
					}
				}
			},
		},
		{
			name:     "nested CASE expressions",
			queryTpl: "SELECT name, CASE WHEN active THEN CASE WHEN score > 80 THEN 'High Performer' ELSE 'Active' END ELSE 'Inactive' END as category FROM '%s'",
			wantRows: 5,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					category := row["category"].(string)
					if name == "Charlie" && category != "High Performer" {
						t.Errorf("Expected Charlie to be 'High Performer', got %s", category)
					}
					if name == "Bob" && category != "Inactive" {
						t.Errorf("Expected Bob to be 'Inactive', got %s", category)
					}
				}
			},
		},
		{
			name:     "CASE in WHERE clause",
			queryTpl: "SELECT name, age FROM '%s' WHERE CASE WHEN age < 26 THEN 'young' ELSE 'old' END = 'young'",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					age := row["age"].(int64)
					if age >= 26 {
						t.Errorf("Expected age < 26, got %d", age)
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

// TestParquetComplexExpressions tests nested functions and arithmetic operations
func TestParquetComplexExpressions(t *testing.T) {
	t.Skip("Some complex expressions may not yet be fully implemented in the query engine")
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
			name:     "arithmetic operations",
			queryTpl: "SELECT name, salary, salary * 1.1 as increased_salary, salary / 12 as monthly_salary FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					salary := row["salary"].(float64)
					increasedSalary := row["increased_salary"].(float64)
					monthlySalary := row["monthly_salary"].(float64)
					if increasedSalary != salary*1.1 {
						t.Errorf("Expected increased_salary %f, got %f", salary*1.1, increasedSalary)
					}
					if monthlySalary != salary/12 {
						t.Errorf("Expected monthly_salary %f, got %f", salary/12, monthlySalary)
					}
				}
			},
		},
		{
			name:     "complex arithmetic with multiple operations",
			queryTpl: "SELECT name, (salary * 0.8) + (score * 100) as combined_metric FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					combinedMetric := row["combined_metric"].(float64)
					// Verify calculation for Alice: (50000 * 0.8) + (85.5 * 100) = 40000 + 8550 = 48550
					if name == "Alice" && combinedMetric != 48550.0 {
						t.Errorf("Expected Alice's combined_metric 48550, got %f", combinedMetric)
					}
				}
			},
		},
		{
			name:     "nested string functions",
			queryTpl: "SELECT UPPER(SUBSTR(name, 1, 3)) as short_upper_name FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for i, row := range rows {
					shortUpperName := row["short_upper_name"].(string)
					expected := []string{"ALI", "BOB", "CHA", "DIA"}
					if shortUpperName != expected[i] {
						t.Errorf("Row %d: expected %s, got %s", i, expected[i], shortUpperName)
					}
				}
			},
		},
		{
			name:     "nested mathematical functions",
			queryTpl: "SELECT name, ROUND(SQRT(salary), 2) as sqrt_salary FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					sqrtSalary := row["sqrt_salary"].(float64)
					if name == "Alice" {
						// SQRT(50000) ≈ 223.61
						if sqrtSalary < 223.6 || sqrtSalary > 223.7 {
							t.Errorf("Expected sqrt_salary around 223.61, got %f", sqrtSalary)
						}
					}
				}
			},
		},
		{
			name:     "complex expressions in WHERE clause",
			queryTpl: "SELECT name FROM '%s' WHERE (salary / age) > 1500 AND (score * 0.9) > 70",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					if name == "Bob" {
						t.Errorf("Bob should not meet the criteria")
					}
				}
			},
		},
		{
			name:     "aggregation with complex expressions",
			queryTpl: "SELECT AVG(salary * 1.2) as avg_increased_salary, SUM(score + age) as total_combined FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				avgIncreasedSalary := rows[0]["avg_increased_salary"].(float64)
				totalCombined := rows[0]["total_combined"].(float64)
				// AVG((50000 + 45000 + 60000 + 52000) * 1.2 / 4) = 61800
				if avgIncreasedSalary != 61800.0 {
					t.Errorf("Expected avg_increased_salary 61800, got %f", avgIncreasedSalary)
				}
				// SUM((85.5+30) + (72.3+25) + (91.2+35) + (78.9+28)) = 445.9
				if totalCombined != 445.9 {
					t.Errorf("Expected total_combined 445.9, got %f", totalCombined)
				}
			},
		},
		{
			name:     "conditional expression with arithmetic",
			queryTpl: "SELECT name, CASE WHEN salary > 50000 THEN (salary - 50000) * 0.3 ELSE salary * 0.25 END as tax FROM '%s'",
			wantRows: 4,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					name := row["name"].(string)
					tax := row["tax"].(float64)
					if name == "Charlie" {
						// (60000 - 50000) * 0.3 = 3000
						if tax != 3000.0 {
							t.Errorf("Expected Charlie's tax 3000, got %f", tax)
						}
					}
					if name == "Bob" {
						// 45000 * 0.25 = 11250
						if tax != 11250.0 {
							t.Errorf("Expected Bob's tax 11250, got %f", tax)
						}
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
// TestParquetNullValues tests filtering and aggregating null values
func TestParquetNullValues(t *testing.T) {
	// Create test data with nullable fields
	testData := []ComplexDataRow{
		{
			ID:        1,
			Name:      "Alice",
			Age:       int64Ptr(30),
			Timestamp: time.Now(),
			Salary:    float64Ptr(50000.0),
			Active:    boolPtr(true),
			Tags:      []string{"engineer", "senior"},
			Score:     float64Ptr(85.5),
		},
		{
			ID:        2,
			Name:      "Bob",
			Age:       nil, // null age
			Timestamp: time.Now(),
			Salary:    float64Ptr(45000.0),
			Active:    boolPtr(false),
			Tags:      []string{"engineer"},
			Score:     nil, // null score
		},
		{
			ID:        3,
			Name:      "Charlie",
			Age:       int64Ptr(35),
			Timestamp: time.Now(),
			Salary:    nil, // null salary
			Active:    boolPtr(true),
			Tags:      []string{"manager"},
			Score:     float64Ptr(91.2),
		},
		{
			ID:        4,
			Name:      "Diana",
			Age:       int64Ptr(28),
			Timestamp: time.Now(),
			Salary:    float64Ptr(52000.0),
			Active:    nil, // null active
			Tags:      []string{},
			Score:     float64Ptr(78.9),
		},
	}

	testFile := createComplexParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "filter IS NULL",
			queryTpl: "SELECT * FROM '%s' WHERE age IS NULL",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Errorf("Expected 1 row with null age")
				}
			},
		},
		{
			name:     "filter IS NOT NULL",
			queryTpl: "SELECT * FROM '%s' WHERE salary IS NOT NULL",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					if row["salary"] == nil {
						t.Errorf("Expected non-null salary, got nil")
					}
				}
			},
		},
		{
			name:     "count with nulls",
			queryTpl: "SELECT COUNT(*) as total, COUNT(age) as age_count, COUNT(salary) as salary_count FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				total := rows[0]["total"].(int64)
				ageCount := rows[0]["age_count"].(int64)
				salaryCount := rows[0]["salary_count"].(int64)

				if total != 4 {
					t.Errorf("Expected total = 4, got %d", total)
				}
				if ageCount != 3 {
					t.Errorf("Expected age_count = 3, got %d", ageCount)
				}
				if salaryCount != 3 {
					t.Errorf("Expected salary_count = 3, got %d", salaryCount)
				}
			},
		},
		{
			name:     "aggregate with nulls - SUM ignores nulls",
			queryTpl: "SELECT SUM(salary) as total_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				// Sum should be 50000 + 45000 + 52000 = 147000 (ignoring null)
				total := rows[0]["total_salary"].(float64)
				expected := 147000.0
				if total != expected {
					t.Errorf("Expected total_salary = %.2f, got %.2f", expected, total)
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

// TestParquetEmptyFile tests queries on empty parquet files
func TestParquetEmptyFile(t *testing.T) {
	testFile := createEmptyParquetFile(t)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
	}{
		{
			name:     "select all from empty file",
			queryTpl: "SELECT * FROM '%s'",
			wantRows: 0,
		},
		{
			name:     "select with filter on empty file",
			queryTpl: "SELECT * FROM '%s' WHERE age > 25",
			wantRows: 0,
		},
		{
			name:     "count on empty file",
			queryTpl: "SELECT COUNT(*) as total FROM '%s'",
			wantRows: 1, // COUNT(*) returns 0, but there's still 1 result row
		},
		{
			name:     "aggregate on empty file",
			queryTpl: "SELECT SUM(salary) as total, AVG(age) as avg_age FROM '%s'",
			wantRows: 1, // Aggregates on empty set return 1 row with nulls/zeros
		},
		{
			name:     "group by on empty file",
			queryTpl: "SELECT age, COUNT(*) as cnt FROM '%s' GROUP BY age",
			wantRows: 0, // GROUP BY on empty set returns 0 rows
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

			// For COUNT(*) on empty file, verify the count is 0
			if tt.name == "count on empty file" && len(results) > 0 {
				if count, ok := results[0]["total"].(int64); ok {
					if count != 0 {
						t.Errorf("Expected count = 0, got %d", count)
					}
				}
			}
		})
	}
}

// TestParquetComplexSchema tests complex nested schemas
func TestParquetComplexSchema(t *testing.T) {
	// ComplexDataRow already has arrays and nullable fields
	testData := []ComplexDataRow{
		{
			ID:        1,
			Name:      "Alice",
			Age:       int64Ptr(30),
			Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			Salary:    float64Ptr(50000.0),
			Active:    boolPtr(true),
			Tags:      []string{"engineer", "senior", "golang"},
			Score:     float64Ptr(85.5),
		},
		{
			ID:        2,
			Name:      "Bob",
			Age:       int64Ptr(25),
			Timestamp: time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
			Salary:    float64Ptr(45000.0),
			Active:    boolPtr(false),
			Tags:      []string{"engineer"},
			Score:     float64Ptr(72.3),
		},
		{
			ID:        3,
			Name:      "Charlie",
			Age:       int64Ptr(35),
			Timestamp: time.Date(2024, 1, 3, 12, 0, 0, 0, time.UTC),
			Salary:    float64Ptr(60000.0),
			Active:    boolPtr(true),
			Tags:      []string{"manager", "senior"},
			Score:     float64Ptr(91.2),
		},
	}

	testFile := createComplexParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "select all from complex schema",
			queryTpl: "SELECT * FROM '%s'",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(rows))
				}
			},
		},
		{
			name:     "filter on nullable field",
			queryTpl: "SELECT * FROM '%s' WHERE age > 25",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					// Verify we got the right records (age 30 and 35)
					id := row["id"].(int64)
					if id != 1 && id != 3 {
						t.Errorf("Unexpected ID in filtered results: %d", id)
					}
				}
			},
		},
		{
			name:     "select specific columns from complex schema",
			queryTpl: "SELECT id, name, timestamp FROM '%s' ORDER BY id",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 3 {
					t.Fatalf("Expected 3 rows, got %d", len(rows))
				}
				// Verify column selection
				for _, row := range rows {
					if _, ok := row["id"]; !ok {
						t.Error("Missing id column")
					}
					if _, ok := row["name"]; !ok {
						t.Error("Missing name column")
					}
					if _, ok := row["timestamp"]; !ok {
						t.Error("Missing timestamp column")
					}
					// Should not have other columns
					if len(row) > 3 {
						t.Errorf("Expected 3 columns, got %d", len(row))
					}
				}
			},
		},
		{
			name:     "aggregate on complex schema",
			queryTpl: "SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				total := rows[0]["total"].(int64)
				avgSalary := rows[0]["avg_salary"].(float64)

				if total != 3 {
					t.Errorf("Expected total = 3, got %d", total)
				}
				expectedAvg := (50000.0 + 45000.0 + 60000.0) / 3.0
				if avgSalary != expectedAvg {
					t.Errorf("Expected avg_salary = %.2f, got %.2f", expectedAvg, avgSalary)
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

// TestParquetLargeDataset tests performance with 1000+ rows
func TestParquetLargeDataset(t *testing.T) {
	// Generate 1000 rows
	testData := make([]BasicDataRow, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = BasicDataRow{
			ID:     int64(i + 1),
			Name:   fmt.Sprintf("User_%d", i+1),
			Age:    int64(20 + (i % 50)),         // Ages 20-69
			Salary: float64(30000 + (i * 100)),   // Salaries 30000-129900
			Active: i%2 == 0,                     // Alternating true/false
			Score:  float64(50.0 + (i % 50)),     // Scores 50-99
		}
	}

	testFile := createBasicParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "select all from large dataset",
			queryTpl: "SELECT * FROM '%s'",
			wantRows: 1000,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1000 {
					t.Errorf("Expected 1000 rows, got %d", len(rows))
				}
			},
		},
		{
			name:     "filter on large dataset",
			queryTpl: "SELECT * FROM '%s' WHERE age >= 50",
			wantRows: 400, // Ages 50-69, 20 values, 1000/50 = 20 cycles, so 20*20 = 400
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					age := row["age"].(int64)
					if age < 50 {
						t.Errorf("Expected age >= 50, got %d", age)
					}
				}
			},
		},
		{
			name:     "aggregate on large dataset",
			queryTpl: "SELECT COUNT(*) as total, AVG(salary) as avg_salary, MAX(salary) as max_salary, MIN(salary) as min_salary FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				total := rows[0]["total"].(int64)
				if total != 1000 {
					t.Errorf("Expected total = 1000, got %d", total)
				}
				maxSalary := rows[0]["max_salary"].(float64)
				minSalary := rows[0]["min_salary"].(float64)
				if maxSalary != 129900.0 {
					t.Errorf("Expected max_salary = 129900.0, got %.2f", maxSalary)
				}
				if minSalary != 30000.0 {
					t.Errorf("Expected min_salary = 30000.0, got %.2f", minSalary)
				}
			},
		},
		{
			name:     "group by on large dataset",
			queryTpl: "SELECT age, COUNT(*) as cnt FROM '%s' GROUP BY age",
			wantRows: 50, // 50 unique ages (20-69)
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 50 {
					t.Errorf("Expected 50 groups, got %d", len(rows))
				}
				// Each age group should have 20 records (1000 rows / 50 ages)
				for _, row := range rows {
					cnt := row["cnt"].(int64)
					if cnt != 20 {
						t.Errorf("Expected count = 20 for each age group, got %d", cnt)
					}
				}
			},
		},
		{
			name:     "limit and offset on large dataset",
			queryTpl: "SELECT * FROM '%s' ORDER BY id LIMIT 50 OFFSET 100",
			wantRows: 50,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 50 {
					t.Errorf("Expected 50 rows, got %d", len(rows))
				}
				// First row should be ID 101 (offset 100 means skip first 100)
				if len(rows) > 0 {
					firstID := rows[0]["id"].(int64)
					if firstID != 101 {
						t.Errorf("Expected first ID = 101, got %d", firstID)
					}
					lastID := rows[len(rows)-1]["id"].(int64)
					if lastID != 150 {
						t.Errorf("Expected last ID = 150, got %d", lastID)
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

// TestParquetMixedTypes tests all supported data types in one file
func TestParquetMixedTypes(t *testing.T) {
	// Using ComplexDataRow which has multiple types: int64, string, *int64, time.Time, *float64, *bool, []string
	testData := []ComplexDataRow{
		{
			ID:        1,
			Name:      "Alpha",
			Age:       int64Ptr(30),
			Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			Salary:    float64Ptr(75000.0),
			Active:    boolPtr(true),
			Tags:      []string{"tag1", "tag2", "tag3"},
			Score:     float64Ptr(95.5),
		},
		{
			ID:        2,
			Name:      "Beta",
			Age:       int64Ptr(45),
			Timestamp: time.Date(2024, 2, 20, 14, 45, 0, 0, time.UTC),
			Salary:    float64Ptr(85000.0),
			Active:    boolPtr(false),
			Tags:      []string{"tag2", "tag4"},
			Score:     float64Ptr(88.2),
		},
		{
			ID:        3,
			Name:      "Gamma",
			Age:       int64Ptr(28),
			Timestamp: time.Date(2024, 3, 10, 9, 15, 0, 0, time.UTC),
			Salary:    float64Ptr(65000.0),
			Active:    boolPtr(true),
			Tags:      []string{"tag1"},
			Score:     float64Ptr(92.8),
		},
	}

	testFile := createComplexParquetFile(t, testData)

	tests := []struct {
		name     string
		queryTpl string
		wantRows int
		validate func(t *testing.T, rows []map[string]interface{})
	}{
		{
			name:     "select all types",
			queryTpl: "SELECT * FROM '%s'",
			wantRows: 3,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(rows))
				}
				// Verify all types are present
				for _, row := range rows {
					if _, ok := row["id"]; !ok {
						t.Error("Missing id field")
					}
					if _, ok := row["name"]; !ok {
						t.Error("Missing name field")
					}
					if _, ok := row["age"]; !ok {
						t.Error("Missing age field")
					}
					if _, ok := row["timestamp"]; !ok {
						t.Error("Missing timestamp field")
					}
					if _, ok := row["salary"]; !ok {
						t.Error("Missing salary field")
					}
					if _, ok := row["active"]; !ok {
						t.Error("Missing active field")
					}
					if _, ok := row["score"]; !ok {
						t.Error("Missing score field")
					}
				}
			},
		},
		{
			name:     "filter on int64",
			queryTpl: "SELECT * FROM '%s' WHERE id > 1",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					id := row["id"].(int64)
					if id <= 1 {
						t.Errorf("Expected id > 1, got %d", id)
					}
				}
			},
		},
		{
			name:     "filter on string",
			queryTpl: "SELECT * FROM '%s' WHERE name = 'Beta'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				name := rows[0]["name"].(string)
				if name != "Beta" {
					t.Errorf("Expected name = 'Beta', got %s", name)
				}
			},
		},
		{
			name:     "filter on float64",
			queryTpl: "SELECT * FROM '%s' WHERE salary >= 75000",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					salary := row["salary"].(float64)
					if salary < 75000 {
						t.Errorf("Expected salary >= 75000, got %.2f", salary)
					}
				}
			},
		},
		{
			name:     "filter on bool",
			queryTpl: "SELECT * FROM '%s' WHERE active = true",
			wantRows: 2,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				for _, row := range rows {
					active := row["active"].(bool)
					if !active {
						t.Error("Expected active = true")
					}
				}
			},
		},
		{
			name:     "aggregate mixed types",
			queryTpl: "SELECT COUNT(*) as cnt, AVG(age) as avg_age, SUM(salary) as total_salary, MAX(score) as max_score FROM '%s'",
			wantRows: 1,
			validate: func(t *testing.T, rows []map[string]interface{}) {
				if len(rows) != 1 {
					t.Fatalf("Expected 1 row, got %d", len(rows))
				}
				cnt := rows[0]["cnt"].(int64)
				avgAge := rows[0]["avg_age"].(float64)
				totalSalary := rows[0]["total_salary"].(float64)
				maxScore := rows[0]["max_score"].(float64)

				if cnt != 3 {
					t.Errorf("Expected cnt = 3, got %d", cnt)
				}
				expectedAvgAge := (30.0 + 45.0 + 28.0) / 3.0
				if avgAge != expectedAvgAge {
					t.Errorf("Expected avg_age = %.2f, got %.2f", expectedAvgAge, avgAge)
				}
				if totalSalary != 225000.0 {
					t.Errorf("Expected total_salary = 225000.0, got %.2f", totalSalary)
				}
				if maxScore != 95.5 {
					t.Errorf("Expected max_score = 95.5, got %.2f", maxScore)
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
