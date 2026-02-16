package query

import (
	"testing"

	"github.com/vegasq/parcat/reader"
)
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
					switch userName {
					case "Alice":
						aliceCount++
					case "Bob":
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
			defer func() { _ = r.Close() }()

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
			defer func() { _ = r.Close() }()

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
			defer func() { _ = r.Close() }()

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
			defer func() { _ = r.Close() }()

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
			defer func() { _ = r.Close() }()

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
			defer func() { _ = r.Close() }()

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

