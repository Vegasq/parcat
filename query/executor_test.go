package query

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vegasq/parcat/reader"
)

// createTestParquetFile creates a test parquet file with the given rows
func createTestParquetFile(t *testing.T, path string, rows []map[string]interface{}) {
	t.Helper()

	if len(rows) == 0 {
		t.Fatal("no rows provided to createTestParquetFile")
	}

	// Flexible row type that can handle various column names
	// We determine which columns are present from the first row
	firstRow := rows[0]

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer func() { _ = f.Close() }()

	// Check which columns are present and create appropriate struct type
	hasName := false
	hasAge := false
	hasID := false
	hasVal := false
	hasUserID := false

	for col := range firstRow {
		switch col {
		case "name":
			hasName = true
		case "age":
			hasAge = true
		case "id":
			hasID = true
		case "val":
			hasVal = true
		case "user_id":
			hasUserID = true
		}
	}

	// Convert maps to typed rows based on detected columns
	// This is ugly but necessary because parquet requires static types
	if hasName && hasAge && !hasID && !hasVal {
		// Original test case: name + age
		type Row struct {
			Name string `parquet:"name"`
			Age  int64  `parquet:"age"`
		}
		var typedRows []Row
		for _, row := range rows {
			r := Row{}
			if v, ok := row["name"].(string); ok {
				r.Name = v
			}
			if v, ok := row["age"].(int64); ok {
				r.Age = v
			} else if v, ok := row["age"].(int); ok {
				r.Age = int64(v)
			}
			typedRows = append(typedRows, r)
		}
		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(typedRows); err != nil {
			t.Fatalf("failed to write test data: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}
	} else if hasVal && !hasName && !hasAge && !hasID {
		// CTE test case: val only
		type Row struct {
			Val int64 `parquet:"val"`
		}
		var typedRows []Row
		for _, row := range rows {
			r := Row{}
			if v, ok := row["val"].(int64); ok {
				r.Val = v
			} else if v, ok := row["val"].(int); ok {
				r.Val = int64(v)
			}
			typedRows = append(typedRows, r)
		}
		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(typedRows); err != nil {
			t.Fatalf("failed to write test data: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}
	} else if hasID && hasName && !hasVal && !hasUserID {
		// CTE test case: id + name
		type Row struct {
			ID   int64  `parquet:"id"`
			Name string `parquet:"name"`
		}
		var typedRows []Row
		for _, row := range rows {
			r := Row{}
			if v, ok := row["id"].(int64); ok {
				r.ID = v
			} else if v, ok := row["id"].(int); ok {
				r.ID = int64(v)
			}
			if v, ok := row["name"].(string); ok {
				r.Name = v
			}
			typedRows = append(typedRows, r)
		}
		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(typedRows); err != nil {
			t.Fatalf("failed to write test data: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}
	} else if hasUserID && hasVal && !hasID && !hasName && !hasAge {
		// JOIN test case: user_id + val
		type Row struct {
			UserID int64 `parquet:"user_id"`
			Val    int64 `parquet:"val"`
		}
		var typedRows []Row
		for _, row := range rows {
			r := Row{}
			if v, ok := row["user_id"].(int64); ok {
				r.UserID = v
			} else if v, ok := row["user_id"].(int); ok {
				r.UserID = int64(v)
			}
			if v, ok := row["val"].(int64); ok {
				r.Val = v
			} else if v, ok := row["val"].(int); ok {
				r.Val = int64(v)
			}
			typedRows = append(typedRows, r)
		}
		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(typedRows); err != nil {
			t.Fatalf("failed to write test data: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}
	} else {
		// Fallback: support all possible columns
		type Row struct {
			Name   string `parquet:"name,optional"`
			Age    int64  `parquet:"age,optional"`
			ID     int64  `parquet:"id,optional"`
			Val    int64  `parquet:"val,optional"`
			UserID int64  `parquet:"user_id,optional"`
		}
		var typedRows []Row
		for _, row := range rows {
			r := Row{}
			if v, ok := row["name"].(string); ok {
				r.Name = v
			}
			if v, ok := row["age"].(int64); ok {
				r.Age = v
			} else if v, ok := row["age"].(int); ok {
				r.Age = int64(v)
			}
			if v, ok := row["id"].(int64); ok {
				r.ID = v
			} else if v, ok := row["id"].(int); ok {
				r.ID = int64(v)
			}
			if v, ok := row["val"].(int64); ok {
				r.Val = v
			} else if v, ok := row["val"].(int); ok {
				r.Val = int64(v)
			}
			if v, ok := row["user_id"].(int64); ok {
				r.UserID = v
			} else if v, ok := row["user_id"].(int); ok {
				r.UserID = int64(v)
			}
			typedRows = append(typedRows, r)
		}
		writer := parquet.NewGenericWriter[Row](f)
		if _, err := writer.Write(typedRows); err != nil {
			t.Fatalf("failed to write test data: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}
	}
}

func TestExecuteQuery_SimpleSelect(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data
	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(25)},
	}
	createTestParquetFile(t, testFile, rows)

	// Create reader
	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query
	q, err := Parse("SELECT name, age FROM test.parquet")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Execute query - this should use ExecuteQuery which handles file reading internally
	// However, ExecuteQuery expects to read from q.TableName
	// We need to set the table name to our test file
	q.TableName = testFile

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Verify results
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}

	// Check first row
	if results[0]["name"] != "alice" {
		t.Errorf("results[0][name] = %v, want alice", results[0]["name"])
	}
	if results[0]["age"] != int64(30) {
		t.Errorf("results[0][age] = %v, want 30", results[0]["age"])
	}
}

func TestExecuteQuery_WithFilter(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data
	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(25)},
		{"name": "charlie", "age": int64(35)},
	}
	createTestParquetFile(t, testFile, rows)

	// Create reader
	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with filter
	q, err := Parse("SELECT name FROM test.parquet WHERE age > 25")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	q.TableName = testFile

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Verify results - should only return alice and charlie
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}

	// Check names (order might vary)
	names := make(map[string]bool)
	for _, row := range results {
		if name, ok := row["name"].(string); ok {
			names[name] = true
		}
	}

	if !names["alice"] || !names["charlie"] {
		t.Errorf("results should contain alice and charlie, got %v", names)
	}
	if names["bob"] {
		t.Errorf("results should not contain bob")
	}
}

func TestNewExecutionContext(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"name": "test", "age": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)
	if ctx == nil {
		t.Fatal("NewExecutionContext() returned nil")
	}
	if ctx.CTEs == nil {
		t.Error("CTEs map is nil")
	}
	if ctx.Reader == nil {
		t.Error("Reader is nil")
	}
	if ctx.InProgress == nil {
		t.Error("InProgress map is nil")
	}
	if ctx.AllCTENames == nil {
		t.Error("AllCTENames map is nil")
	}
}

func TestNewChildContext(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"name": "test", "age": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	parent := NewExecutionContext(r)
	parent.CTEs["test_cte"] = []map[string]interface{}{
		{"col": "value"},
	}
	parent.AllCTENames["test_cte"] = true
	parent.AllCTENames["future_cte"] = true

	child := parent.NewChildContext()
	if child == nil {
		t.Fatal("NewChildContext() returned nil")
	}

	// Child should have parent's CTEs
	if len(child.CTEs) != 1 {
		t.Errorf("child.CTEs length = %d, want 1", len(child.CTEs))
	}

	// Child should inherit parent's AllCTENames for forward-reference detection
	if !child.AllCTENames["test_cte"] {
		t.Error("child should inherit parent's AllCTENames - test_cte not found")
	}
	if !child.AllCTENames["future_cte"] {
		t.Error("child should inherit parent's AllCTENames - future_cte not found")
	}

	// Child should have its own maps (not sharing)
	child.CTEs["new_cte"] = []map[string]interface{}{}
	if len(parent.CTEs) != 1 {
		t.Error("modifying child.CTEs should not affect parent")
	}

	// Modifying child AllCTENames should not affect parent
	child.AllCTENames["child_only"] = true
	if parent.AllCTENames["child_only"] {
		t.Error("modifying child.AllCTENames should not affect parent")
	}
}

func TestExecuteQuery_WithCTE(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data
	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(25)},
		{"name": "charlie", "age": int64(35)},
	}
	createTestParquetFile(t, testFile, rows)

	// Create reader
	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with CTE
	queryStr := `WITH adults AS (SELECT name, age FROM test.parquet WHERE age >= 30)
SELECT name FROM adults`

	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// The CTE references test.parquet, set it to our test file
	if len(q.CTEs) > 0 {
		q.CTEs[0].Query.TableName = testFile
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Verify results - should return alice and charlie
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}

	// Check names
	names := make(map[string]bool)
	for _, row := range results {
		if name, ok := row["name"].(string); ok {
			names[name] = true
		}
	}

	if !names["alice"] || !names["charlie"] {
		t.Errorf("results should contain alice and charlie, got %v", names)
	}
}

func TestExecuteQuery_WithInnerJoin(t *testing.T) {
	tmpDir := t.TempDir()
	usersFile := filepath.Join(tmpDir, "users.parquet")
	ordersFile := filepath.Join(tmpDir, "orders.parquet")

	// Create test users
	type UserRow struct {
		UserID int64  `parquet:"user_id"`
		Name   string `parquet:"name"`
	}
	usersData := []UserRow{
		{UserID: 1, Name: "alice"},
		{UserID: 2, Name: "bob"},
	}

	f, err := os.Create(usersFile)
	if err != nil {
		t.Fatalf("failed to create users file: %v", err)
	}
	writer := parquet.NewGenericWriter[UserRow](f)
	_, _ = writer.Write(usersData)
	_ = writer.Close()
	_ = f.Close()

	// Create test orders
	type OrderRow struct {
		OrderID int64 `parquet:"order_id"`
		UserID  int64 `parquet:"user_id"`
	}
	ordersData := []OrderRow{
		{OrderID: 100, UserID: 1},
		{OrderID: 101, UserID: 1},
	}

	f2, err := os.Create(ordersFile)
	if err != nil {
		t.Fatalf("failed to create orders file: %v", err)
	}
	writer2 := parquet.NewGenericWriter[OrderRow](f2)
	_, _ = writer2.Write(ordersData)
	_ = writer2.Close()
	_ = f2.Close()

	// Create reader (for users table)
	r, err := reader.NewReader(usersFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with JOIN
	queryStr := "SELECT u.name, o.order_id FROM users.parquet u INNER JOIN orders.parquet o ON u.user_id = o.user_id"
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Set table names
	q.TableName = usersFile
	if len(q.Joins) > 0 {
		q.Joins[0].TableName = ordersFile
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Verify results - should return 2 rows (alice's two orders)
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}

	// Both should be for alice
	for _, row := range results {
		if name, ok := row["u.name"].(string); ok {
			if name != "alice" {
				t.Errorf("expected name=alice, got %s", name)
			}
		}
	}
}

// TODO: Fix subquery table name resolution for EXISTS
// func TestExecuteQuery_WithSubquery(t *testing.T) {
// 	tmpDir := t.TempDir()
// 	testFile := filepath.Join(tmpDir, "test.parquet")
//
// 	// Create test data
// 	rows := []map[string]interface{}{
// 		{"name": "alice", "age": int64(30)},
// 		{"name": "bob", "age": int64(25)},
// 		{"name": "charlie", "age": int64(35)},
// 	}
// 	createTestParquetFile(t, testFile, rows)
//
// 	// Create reader
// 	r, err := reader.NewReader(testFile)
// 	if err != nil {
// 		t.Fatalf("NewReader() error = %v", err)
// 	}
// 	defer func() { _ = r.Close() }()
//
// 	// Parse query with EXISTS subquery
// 	queryStr := `SELECT name FROM test.parquet WHERE EXISTS (SELECT 1 FROM test.parquet WHERE age > 30)`
// 	q, err := Parse(queryStr)
// 	if err != nil {
// 		t.Fatalf("Parse() error = %v", err)
// 	}
// 	q.TableName = testFile
//
// 	// Execute query
// 	results, err := ExecuteQuery(q, r)
// 	if err != nil {
// 		t.Fatalf("ExecuteQuery() error = %v", err)
// 	}
//
// 	// If charlie (age 35) exists, all rows should be returned
// 	if len(results) != 3 {
// 		t.Errorf("ExecuteQuery() returned %d rows, want 3", len(results))
// 	}
// }

func TestExecuteQuery_WithScalarSubquery(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data
	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
		{"name": "bob", "age": int64(25)},
	}
	createTestParquetFile(t, testFile, rows)

	// Create reader
	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with scalar subquery in SELECT
	queryStr := `SELECT name, (SELECT COUNT(*) FROM test.parquet) as total FROM test.parquet`
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	q.TableName = testFile

	// Set subquery table name
	if len(q.SelectList) > 1 {
		if subq, ok := q.SelectList[1].Expr.(*ScalarSubqueryExpr); ok {
			subq.Query.TableName = testFile
		}
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Each row should have total=2
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}

	for i, row := range results {
		if total, ok := row["total"].(int64); ok {
			if total != 2 {
				t.Errorf("row %d: total = %d, want 2", i, total)
			}
		}
	}
}

// TODO: Fix subquery table name resolution for IN
// func TestExecuteQuery_WithINSubquery(t *testing.T) {
// 	tmpDir := t.TempDir()
// 	testFile := filepath.Join(tmpDir, "test.parquet")
//
// 	// Create test data
// 	rows := []map[string]interface{}{
// 		{"name": "alice", "age": int64(30)},
// 		{"name": "bob", "age": int64(25)},
// 		{"name": "charlie", "age": int64(35)},
// 	}
// 	createTestParquetFile(t, testFile, rows)
//
// 	// Create reader
// 	r, err := reader.NewReader(testFile)
// 	if err != nil {
// 		t.Fatalf("NewReader() error = %v", err)
// 	}
// 	defer func() { _ = r.Close() }()
//
// 	// Parse query with IN subquery
// 	queryStr := `SELECT name FROM test.parquet WHERE age IN (SELECT age FROM test.parquet WHERE age >= 30)`
// 	q, err := Parse(queryStr)
// 	if err != nil {
// 		t.Fatalf("Parse() error = %v", err)
// 	}
// 	q.TableName = testFile
//
// 	// Execute query
// 	results, err := ExecuteQuery(q, r)
// 	if err != nil {
// 		t.Fatalf("ExecuteQuery() error = %v", err)
// 	}
//
// 	// Should return alice and charlie
// 	if len(results) != 2 {
// 		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
// 	}
//
// 	names := make(map[string]bool)
// 	for _, row := range results {
// 		if name, ok := row["name"].(string); ok {
// 			names[name] = true
// 		}
// 	}
//
// 	if !names["alice"] || !names["charlie"] {
// 		t.Errorf("results should contain alice and charlie, got %v", names)
// 	}
// }

func TestExecuteQuery_WithLeftJoin(t *testing.T) {
	tmpDir := t.TempDir()
	usersFile := filepath.Join(tmpDir, "users.parquet")
	ordersFile := filepath.Join(tmpDir, "orders.parquet")

	// Create test users
	type UserRow struct {
		UserID int64  `parquet:"user_id"`
		Name   string `parquet:"name"`
	}
	usersData := []UserRow{
		{UserID: 1, Name: "alice"},
		{UserID: 2, Name: "bob"},
	}

	f, err := os.Create(usersFile)
	if err != nil {
		t.Fatalf("failed to create users file: %v", err)
	}
	writer := parquet.NewGenericWriter[UserRow](f)
	_, _ = writer.Write(usersData)
	_ = writer.Close()
	_ = f.Close()

	// Create test orders - only for user 1
	type OrderRow struct {
		OrderID int64 `parquet:"order_id"`
		UserID  int64 `parquet:"user_id"`
	}
	ordersData := []OrderRow{
		{OrderID: 100, UserID: 1},
	}

	f2, err := os.Create(ordersFile)
	if err != nil {
		t.Fatalf("failed to create orders file: %v", err)
	}
	writer2 := parquet.NewGenericWriter[OrderRow](f2)
	_, _ = writer2.Write(ordersData)
	_ = writer2.Close()
	_ = f2.Close()

	// Create reader
	r, err := reader.NewReader(usersFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with LEFT JOIN
	queryStr := "SELECT u.name FROM users.parquet u LEFT JOIN orders.parquet o ON u.user_id = o.user_id"
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = usersFile
	if len(q.Joins) > 0 {
		q.Joins[0].TableName = ordersFile
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Should return both users (bob with NULL order columns)
	if len(results) < 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want at least 2", len(results))
	}
}

func TestExecuteQuery_WithCrossJoin(t *testing.T) {
	tmpDir := t.TempDir()
	table1File := filepath.Join(tmpDir, "table1.parquet")
	table2File := filepath.Join(tmpDir, "table2.parquet")

	// Create first table
	type Row1 struct {
		A string `parquet:"a"`
	}
	data1 := []Row1{
		{A: "a1"},
		{A: "a2"},
	}

	f, err := os.Create(table1File)
	if err != nil {
		t.Fatalf("failed to create table1 file: %v", err)
	}
	writer := parquet.NewGenericWriter[Row1](f)
	_, _ = writer.Write(data1)
	_ = writer.Close()
	_ = f.Close()

	// Create second table
	type Row2 struct {
		B string `parquet:"b"`
	}
	data2 := []Row2{
		{B: "b1"},
		{B: "b2"},
	}

	f2, err := os.Create(table2File)
	if err != nil {
		t.Fatalf("failed to create table2 file: %v", err)
	}
	writer2 := parquet.NewGenericWriter[Row2](f2)
	_, _ = writer2.Write(data2)
	_ = writer2.Close()
	_ = f2.Close()

	// Create reader
	r, err := reader.NewReader(table1File)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with CROSS JOIN
	queryStr := "SELECT t1.a, t2.b FROM table1.parquet t1 CROSS JOIN table2.parquet t2"
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = table1File
	if len(q.Joins) > 0 {
		q.Joins[0].TableName = table2File
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// CROSS JOIN should return 2 * 2 = 4 rows
	if len(results) != 4 {
		t.Errorf("ExecuteQuery() returned %d rows, want 4", len(results))
	}
}

func TestExecuteQuery_WithRightJoin(t *testing.T) {
	tmpDir := t.TempDir()
	usersFile := filepath.Join(tmpDir, "users.parquet")
	ordersFile := filepath.Join(tmpDir, "orders.parquet")

	// Create test users - only user 1
	type UserRow struct {
		UserID int64  `parquet:"user_id"`
		Name   string `parquet:"name"`
	}
	usersData := []UserRow{
		{UserID: 1, Name: "alice"},
	}

	f, err := os.Create(usersFile)
	if err != nil {
		t.Fatalf("failed to create users file: %v", err)
	}
	writer := parquet.NewGenericWriter[UserRow](f)
	_, _ = writer.Write(usersData)
	_ = writer.Close()
	_ = f.Close()

	// Create test orders - orders for users 1 and 2 (user 2 doesn't exist in users table)
	type OrderRow struct {
		OrderID int64 `parquet:"order_id"`
		UserID  int64 `parquet:"user_id"`
	}
	ordersData := []OrderRow{
		{OrderID: 100, UserID: 1},
		{OrderID: 101, UserID: 2},
	}

	f2, err := os.Create(ordersFile)
	if err != nil {
		t.Fatalf("failed to create orders file: %v", err)
	}
	writer2 := parquet.NewGenericWriter[OrderRow](f2)
	_, _ = writer2.Write(ordersData)
	_ = writer2.Close()
	_ = f2.Close()

	// Create reader
	r, err := reader.NewReader(usersFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with RIGHT JOIN
	queryStr := "SELECT o.order_id FROM users.parquet u RIGHT JOIN orders.parquet o ON u.user_id = o.user_id"
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = usersFile
	if len(q.Joins) > 0 {
		q.Joins[0].TableName = ordersFile
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Should return both orders (order 101 with NULL user columns)
	if len(results) < 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want at least 2", len(results))
	}
}

func TestExecuteQuery_WithFullJoin(t *testing.T) {
	tmpDir := t.TempDir()
	table1File := filepath.Join(tmpDir, "table1.parquet")
	table2File := filepath.Join(tmpDir, "table2.parquet")

	// Create first table
	type Row1 struct {
		ID int64  `parquet:"id"`
		A  string `parquet:"a"`
	}
	data1 := []Row1{
		{ID: 1, A: "a1"},
		{ID: 2, A: "a2"},
	}

	f, err := os.Create(table1File)
	if err != nil {
		t.Fatalf("failed to create table1 file: %v", err)
	}
	writer := parquet.NewGenericWriter[Row1](f)
	_, _ = writer.Write(data1)
	_ = writer.Close()
	_ = f.Close()

	// Create second table
	type Row2 struct {
		ID int64  `parquet:"id"`
		B  string `parquet:"b"`
	}
	data2 := []Row2{
		{ID: 2, B: "b2"},
		{ID: 3, B: "b3"},
	}

	f2, err := os.Create(table2File)
	if err != nil {
		t.Fatalf("failed to create table2 file: %v", err)
	}
	writer2 := parquet.NewGenericWriter[Row2](f2)
	_, _ = writer2.Write(data2)
	_ = writer2.Close()
	_ = f2.Close()

	// Create reader
	r, err := reader.NewReader(table1File)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Parse query with FULL JOIN
	queryStr := "SELECT t1.id, t2.id FROM table1.parquet t1 FULL JOIN table2.parquet t2 ON t1.id = t2.id"
	q, err := Parse(queryStr)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = table1File
	if len(q.Joins) > 0 {
		q.Joins[0].TableName = table2File
	}

	// Execute query
	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// FULL JOIN should return all rows from both tables (id=1 from t1, id=2 matched, id=3 from t2)
	if len(results) < 3 {
		t.Errorf("ExecuteQuery() returned %d rows, want at least 3", len(results))
	}
}

// TestSubquery_EXISTS_WithCTE tests EXISTS subquery that contains CTEs
func TestSubquery_EXISTS_WithCTE(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")
	lookupFile := filepath.Join(tmpDir, "lookup.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	createTestParquetFile(t, testFile, rows)

	// Create a lookup table for the CTE
	lookupRows := []map[string]interface{}{
		{"val": int64(1)},
	}
	createTestParquetFile(t, lookupFile, lookupRows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Query: SELECT * FROM test WHERE EXISTS (WITH cte AS (SELECT val FROM lookup.parquet WHERE val = 1) SELECT * FROM cte)
	q, err := Parse("SELECT * FROM test WHERE EXISTS (WITH cte AS (SELECT val FROM lookup.parquet WHERE val = 1) SELECT * FROM cte)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = testFile
	// Update CTE table reference
	if q.Filter != nil {
		if exists, ok := q.Filter.(*ExistsExpr); ok && exists.Subquery != nil {
			if len(exists.Subquery.CTEs) > 0 {
				exists.Subquery.CTEs[0].Query.TableName = lookupFile
			}
		}
	}

	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// EXISTS should return true, so we should get all rows
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}
}

// TestSubquery_IN_WithCTE tests IN subquery that contains CTEs
func TestSubquery_IN_WithCTE(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")
	lookupFile := filepath.Join(tmpDir, "lookup.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	createTestParquetFile(t, testFile, rows)

	// Create a lookup table for the CTE
	lookupRows := []map[string]interface{}{
		{"val": int64(1)},
	}
	createTestParquetFile(t, lookupFile, lookupRows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Query: SELECT * FROM test WHERE id IN (WITH cte AS (SELECT val FROM lookup.parquet) SELECT val FROM cte)
	q, err := Parse("SELECT * FROM test WHERE id IN (WITH cte AS (SELECT val FROM lookup.parquet) SELECT val FROM cte)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = testFile
	// Update CTE table reference
	if q.Filter != nil {
		if inExpr, ok := q.Filter.(*InSubqueryExpr); ok && inExpr.Subquery != nil {
			if len(inExpr.Subquery.CTEs) > 0 {
				inExpr.Subquery.CTEs[0].Query.TableName = lookupFile
			}
		}
	}

	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Should match id=1 only
	if len(results) != 1 {
		t.Errorf("ExecuteQuery() returned %d rows, want 1", len(results))
	}
	if len(results) > 0 && results[0]["id"] != int64(1) {
		t.Errorf("Expected id=1, got %v", results[0]["id"])
	}
}

// TestSubquery_Scalar_WithCTE tests scalar subquery that contains CTEs
func TestSubquery_Scalar_WithCTE(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")
	constFile := filepath.Join(tmpDir, "const.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	createTestParquetFile(t, testFile, rows)

	// Create a constant table for the CTE
	constRows := []map[string]interface{}{
		{"val": int64(100)},
	}
	createTestParquetFile(t, constFile, constRows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Query: SELECT id, (WITH cte AS (SELECT val FROM const.parquet) SELECT val FROM cte) as constant FROM test
	q, err := Parse("SELECT id, (WITH cte AS (SELECT val FROM const.parquet) SELECT val FROM cte) as constant FROM test")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = testFile
	// Update CTE table reference in the scalar subquery
	for i := range q.SelectList {
		if scalarSubq, ok := q.SelectList[i].Expr.(*ScalarSubqueryExpr); ok {
			if len(scalarSubq.Query.CTEs) > 0 {
				scalarSubq.Query.CTEs[0].Query.TableName = constFile
			}
		}
	}

	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	// Should return both rows with constant=100
	if len(results) != 2 {
		t.Errorf("ExecuteQuery() returned %d rows, want 2", len(results))
	}
	for i, row := range results {
		if row["constant"] != int64(100) {
			t.Errorf("Row %d: expected constant=100, got %v", i, row["constant"])
		}
	}
}

// TestScalarSubqueryCache tests that scalar subqueries are cached across rows
func TestScalarSubqueryCache(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")
	constFile := filepath.Join(tmpDir, "const.parquet")

	// Create multiple rows to test caching
	rows := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	createTestParquetFile(t, testFile, rows)

	// Create const table
	constRows := []map[string]interface{}{
		{"val": int64(42)},
	}
	createTestParquetFile(t, constFile, constRows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Create a non-correlated scalar subquery
	q, err := Parse("SELECT id, (SELECT val FROM const.parquet) as const FROM test")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = testFile
	// Update scalar subquery table reference
	for i := range q.SelectList {
		if scalarSubq, ok := q.SelectList[i].Expr.(*ScalarSubqueryExpr); ok {
			scalarSubq.Query.TableName = constFile
		}
	}

	ctx := NewExecutionContext(r)
	results, err := ctx.executeSelect(q)
	if err != nil {
		t.Fatalf("executeSelect() error = %v", err)
	}

	// All rows should have const=42
	if len(results) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results))
	}

	for i, row := range results {
		if row["const"] != int64(42) {
			t.Errorf("Row %d: expected const=42, got %v", i, row["const"])
		}
	}

	// Verify cache was populated (cache should have exactly 1 entry)
	if len(ctx.ScalarSubqueryCache) != 1 {
		t.Errorf("Expected ScalarSubqueryCache to have 1 entry, got %d", len(ctx.ScalarSubqueryCache))
	}
}

// TestCTE_Shadowing tests that inner scopes can shadow outer CTE names
func TestCTE_Shadowing(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")
	val1File := filepath.Join(tmpDir, "val1.parquet")
	val2File := filepath.Join(tmpDir, "val2.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	val1Rows := []map[string]interface{}{
		{"val": int64(1)},
	}
	createTestParquetFile(t, val1File, val1Rows)

	val2Rows := []map[string]interface{}{
		{"val": int64(2)},
	}
	createTestParquetFile(t, val2File, val2Rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Query with CTE shadowing: outer CTE "cte" with val=1, inner subquery redefines "cte" with val=2
	q, err := Parse("WITH cte AS (SELECT val FROM val1.parquet) SELECT id, (WITH cte AS (SELECT val FROM val2.parquet) SELECT val FROM cte) as inner_val FROM test")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	q.TableName = testFile
	// Update outer CTE
	if len(q.CTEs) > 0 {
		q.CTEs[0].Query.TableName = val1File
	}
	// Update inner CTE in scalar subquery
	for i := range q.SelectList {
		if scalarSubq, ok := q.SelectList[i].Expr.(*ScalarSubqueryExpr); ok {
			if len(scalarSubq.Query.CTEs) > 0 {
				scalarSubq.Query.CTEs[0].Query.TableName = val2File
			}
		}
	}

	results, err := ExecuteQuery(q, r)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v (shadowing should be allowed)", err)
	}

	// Inner CTE should shadow outer, so inner_val should be 2
	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}
	if results[0]["inner_val"] != int64(2) {
		t.Errorf("Expected inner_val=2 (shadowed value), got %v", results[0]["inner_val"])
	}
}

// TestCTE_DuplicateInSameClause tests that duplicate CTEs in same WITH clause are rejected
func TestCTE_DuplicateInSameClause(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	// Query with duplicate CTE names in same WITH clause
	q, err := Parse("WITH cte AS (SELECT id FROM test.parquet), cte AS (SELECT id FROM test.parquet) SELECT * FROM cte")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	// Set both CTE references to testFile
	for i := range q.CTEs {
		q.CTEs[i].Query.TableName = testFile
	}

	_, err = ExecuteQuery(q, r)
	if err == nil {
		t.Error("Expected error for duplicate CTE names in same WITH clause, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate CTE name") {
		t.Errorf("Expected 'duplicate CTE name' error, got: %v", err)
	}
}

// TestEvaluateSelectExpression_FunctionCall tests function call evaluation with context
func TestEvaluateSelectExpression_FunctionCall(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"name": "alice", "age": int64(30)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	row := map[string]interface{}{"val": int64(5)}

	// Test UPPER function
	upperCall := &FunctionCall{
		Name: "UPPER",
		Args: []SelectExpression{&ColumnRef{Column: "name"}},
	}

	// Test with row containing the column
	testRow := map[string]interface{}{"name": "alice"}
	result, err := ctx.EvaluateSelectExpression(testRow, upperCall)
	if err != nil {
		t.Errorf("EvaluateSelectExpression(UPPER) error = %v", err)
	}
	if result != "ALICE" {
		t.Errorf("UPPER('alice') = %v, want ALICE", result)
	}

	// Test unknown function
	unknownCall := &FunctionCall{
		Name: "UNKNOWN_FUNC",
		Args: []SelectExpression{&LiteralExpr{Value: int64(1)}},
	}
	_, err = ctx.EvaluateSelectExpression(row, unknownCall)
	if err == nil {
		t.Error("Expected error for unknown function, got nil")
	}
	if !strings.Contains(err.Error(), "unknown function") {
		t.Errorf("Expected 'unknown function' error, got: %v", err)
	}

	// Test function with wrong arity (too few args)
	absCallWrong := &FunctionCall{
		Name: "ABS",
		Args: []SelectExpression{}, // ABS requires 1 argument
	}
	_, err = ctx.EvaluateSelectExpression(row, absCallWrong)
	if err == nil {
		t.Error("Expected arity error for ABS with no args, got nil")
	}

	// Test function with nested scalar subquery argument
	constFile := filepath.Join(tmpDir, "const.parquet")
	constRows := []map[string]interface{}{
		{"val": int64(42)},
	}
	createTestParquetFile(t, constFile, constRows)

	subqExpr := &ScalarSubqueryExpr{
		Query: &Query{
			TableName: constFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
		},
	}

	absCallWithSubq := &FunctionCall{
		Name: "ABS",
		Args: []SelectExpression{subqExpr},
	}

	result, err = ctx.EvaluateSelectExpression(row, absCallWithSubq)
	if err != nil {
		t.Errorf("EvaluateSelectExpression(ABS with subquery) error = %v", err)
	}
	// ABS returns the value as-is for positive numbers, type might be int64 or float64
	if result != int64(42) && result != float64(42) {
		t.Errorf("ABS((SELECT 42)) = %v (type %T), want 42", result, result)
	}
}

// TestEvaluateSelectExpression_CaseExpr tests CASE expression evaluation
func TestEvaluateSelectExpression_CaseExpr(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"val": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test CASE with matching WHEN clause
	caseExpr := &CaseExpr{
		WhenClauses: []WhenClause{
			{
				Condition: &ComparisonExpr{
					Column:   "val",
					Operator: TokenEqual,
					Value:    int64(1),
				},
				Result: &LiteralExpr{Value: "matched"},
			},
		},
		ElseExpr: &LiteralExpr{Value: "not_matched"},
	}

	row := map[string]interface{}{"val": int64(1)}
	result, err := ctx.EvaluateSelectExpression(row, caseExpr)
	if err != nil {
		t.Errorf("EvaluateSelectExpression(CASE) error = %v", err)
	}
	if result != "matched" {
		t.Errorf("CASE result = %v, want matched", result)
	}

	// Test CASE with no matching WHEN, falling to ELSE
	row2 := map[string]interface{}{"val": int64(99)}
	result, err = ctx.EvaluateSelectExpression(row2, caseExpr)
	if err != nil {
		t.Errorf("EvaluateSelectExpression(CASE ELSE) error = %v", err)
	}
	if result != "not_matched" {
		t.Errorf("CASE ELSE result = %v, want not_matched", result)
	}

	// Test CASE with no matching WHEN and no ELSE (returns NULL)
	caseExprNoElse := &CaseExpr{
		WhenClauses: []WhenClause{
			{
				Condition: &ComparisonExpr{
					Column:   "val",
					Operator: TokenEqual,
					Value:    int64(999),
				},
				Result: &LiteralExpr{Value: "never"},
			},
		},
		ElseExpr: nil,
	}

	result, err = ctx.EvaluateSelectExpression(row2, caseExprNoElse)
	if err != nil {
		t.Errorf("EvaluateSelectExpression(CASE no ELSE) error = %v", err)
	}
	if result != nil {
		t.Errorf("CASE with no match and no ELSE should return nil, got %v", result)
	}
}

// TestEvaluateScalarSubquery_EdgeCases tests scalar subquery edge cases
func TestEvaluateScalarSubquery_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test scalar subquery returning no rows (should return NULL)
	emptyFile := filepath.Join(tmpDir, "empty.parquet")
	emptyRows := []map[string]interface{}{
		{"val": int64(1)},
	}
	createTestParquetFile(t, emptyFile, emptyRows)

	subqEmpty := &ScalarSubqueryExpr{
		Query: &Query{
			TableName: emptyFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
			Filter: &ComparisonExpr{
				Column:   "val",
				Operator: TokenEqual,
				Value:    int64(999), // No match
			},
		},
	}

	row := map[string]interface{}{}
	result, err := ctx.EvaluateScalarSubquery(row, subqEmpty)
	if err != nil {
		t.Errorf("EvaluateScalarSubquery(empty result) error = %v", err)
	}
	if result != nil {
		t.Errorf("Empty scalar subquery should return nil, got %v", result)
	}

	// Test scalar subquery returning multiple rows (should error)
	multiFile := filepath.Join(tmpDir, "multi.parquet")
	multiRows := []map[string]interface{}{
		{"val": int64(1)},
		{"val": int64(2)},
	}
	createTestParquetFile(t, multiFile, multiRows)

	subqMulti := &ScalarSubqueryExpr{
		Query: &Query{
			TableName: multiFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
		},
	}

	_, err = ctx.EvaluateScalarSubquery(row, subqMulti)
	if err == nil {
		t.Error("Expected error for scalar subquery returning multiple rows, got nil")
	}
	if !strings.Contains(err.Error(), "more than one row") {
		t.Errorf("Expected 'more than one row' error, got: %v", err)
	}

	// Test scalar subquery returning multiple columns (should error)
	multiColFile := filepath.Join(tmpDir, "multicol.parquet")
	multiColRows := []map[string]interface{}{
		{"val": int64(1), "name": "test"},
	}
	createTestParquetFile(t, multiColFile, multiColRows)

	subqMultiCol := &ScalarSubqueryExpr{
		Query: &Query{
			TableName: multiColFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
				{Expr: &ColumnRef{Column: "name"}},
			},
		},
	}

	_, err = ctx.EvaluateScalarSubquery(row, subqMultiCol)
	if err == nil {
		t.Error("Expected error for scalar subquery returning multiple columns, got nil")
	}
	if !strings.Contains(err.Error(), "exactly one column") {
		t.Errorf("Expected 'exactly one column' error, got: %v", err)
	}
}

// TestExecuteSelect_EdgeCases tests executeSelect edge cases
func TestExecuteSelect_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test query with no data source (should error)
	qNoSource := &Query{
		SelectList: []SelectItem{
			{Expr: &LiteralExpr{Value: int64(1)}},
		},
	}

	_, err = ctx.executeSelect(qNoSource)
	if err == nil {
		t.Error("Expected error for query with no data source, got nil")
	}
	if !strings.Contains(err.Error(), "no data source") {
		t.Errorf("Expected 'no data source' error, got: %v", err)
	}

	// Test forward CTE reference (CTE defined but not yet materialized)
	ctx.AllCTENames["future_cte"] = true

	qForwardRef := &Query{
		TableName: "future_cte",
		SelectList: []SelectItem{
			{Expr: &ColumnRef{Column: "col"}},
		},
	}

	_, err = ctx.executeSelect(qForwardRef)
	if err == nil {
		t.Error("Expected error for forward CTE reference, got nil")
	}
	if !strings.Contains(err.Error(), "forward CTE reference") {
		t.Errorf("Expected 'forward CTE reference' error, got: %v", err)
	}

	// Test query with FROM subquery that has CTEs (should use child context)
	subqWithCTE := &Query{
		Subquery: &Query{
			CTEs: []CTE{
				{
					Name: "sub_cte",
					Query: &Query{
						TableName: testFile,
						SelectList: []SelectItem{
							{Expr: &ColumnRef{Column: "id"}},
						},
					},
				},
			},
			TableName: "sub_cte",
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "id"}},
			},
		},
		SelectList: []SelectItem{
			{Expr: &ColumnRef{Column: "id"}},
		},
	}

	results, err := ctx.executeSelect(subqWithCTE)
	if err != nil {
		t.Errorf("executeSelect(subquery with CTE) error = %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}
}

// TestEvaluateExpression_BinaryExpr tests binary expression evaluation with subqueries
func TestEvaluateExpression_BinaryExpr(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test AND operator
	andExpr := &BinaryExpr{
		Left: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
		Operator: TokenAnd,
		Right: &ComparisonExpr{
			Column:   "id",
			Operator: TokenGreater,
			Value:    int64(0),
		},
	}

	row := map[string]interface{}{"id": int64(1)}
	result, err := ctx.EvaluateExpression(row, andExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(AND) error = %v", err)
	}
	if !result {
		t.Error("AND expression should evaluate to true")
	}

	// Test OR operator
	orExpr := &BinaryExpr{
		Left: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(999),
		},
		Operator: TokenOr,
		Right: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	result, err = ctx.EvaluateExpression(row, orExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(OR) error = %v", err)
	}
	if !result {
		t.Error("OR expression should evaluate to true")
	}

	// Test unsupported operator (TokenEqual is not a boolean operator for BinaryExpr)
	unsupportedExpr := &BinaryExpr{
		Left: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
		Operator: TokenEqual, // Not TokenAnd or TokenOr - should fail
		Right: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	_, err = ctx.EvaluateExpression(row, unsupportedExpr)
	if err == nil {
		t.Error("Expected error for unsupported binary operator, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported binary operator") {
		t.Errorf("Expected 'unsupported binary operator' error, got: %v", err)
	}
}

// TestEvaluateExpression_EXISTS tests EXISTS expression evaluation
func TestEvaluateExpression_EXISTS(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test EXISTS with results
	existsExpr := &ExistsExpr{
		Subquery: &Query{
			TableName: testFile,
			SelectList: []SelectItem{
				{Expr: &LiteralExpr{Value: int64(1)}},
			},
		},
		Negate: false,
	}

	row := map[string]interface{}{}
	result, err := ctx.EvaluateExpression(row, existsExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(EXISTS) error = %v", err)
	}
	if !result {
		t.Error("EXISTS should return true when subquery has rows")
	}

	// Test NOT EXISTS with results
	notExistsExpr := &ExistsExpr{
		Subquery: &Query{
			TableName: testFile,
			SelectList: []SelectItem{
				{Expr: &LiteralExpr{Value: int64(1)}},
			},
		},
		Negate: true,
	}

	result, err = ctx.EvaluateExpression(row, notExistsExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(NOT EXISTS) error = %v", err)
	}
	if result {
		t.Error("NOT EXISTS should return false when subquery has rows")
	}

	// Test EXISTS with no results
	existsEmptyExpr := &ExistsExpr{
		Subquery: &Query{
			TableName: testFile,
			SelectList: []SelectItem{
				{Expr: &LiteralExpr{Value: int64(1)}},
			},
			Filter: &ComparisonExpr{
				Column:   "id",
				Operator: TokenEqual,
				Value:    int64(999),
			},
		},
		Negate: false,
	}

	result, err = ctx.EvaluateExpression(row, existsEmptyExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(EXISTS empty) error = %v", err)
	}
	if result {
		t.Error("EXISTS should return false when subquery has no rows")
	}
}

// TestEvaluateExpression_InSubquery tests IN subquery expression evaluation
func TestEvaluateExpression_InSubquery(t *testing.T) {
	tmpDir := t.TempDir()
	valuesFile := filepath.Join(tmpDir, "values.parquet")

	rows := []map[string]interface{}{
		{"val": int64(1)},
		{"val": int64(2)},
		{"val": int64(3)},
	}
	createTestParquetFile(t, valuesFile, rows)

	r, err := reader.NewReader(valuesFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Test IN with matching value
	inExpr := &InSubqueryExpr{
		Column: "id",
		Subquery: &Query{
			TableName: valuesFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
		},
		Negate: false,
	}

	row := map[string]interface{}{"id": int64(2)}
	result, err := ctx.EvaluateExpression(row, inExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(IN) error = %v", err)
	}
	if !result {
		t.Error("IN should return true when value matches")
	}

	// Test IN with non-matching value
	row2 := map[string]interface{}{"id": int64(999)}
	result, err = ctx.EvaluateExpression(row2, inExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(IN no match) error = %v", err)
	}
	if result {
		t.Error("IN should return false when value doesn't match")
	}

	// Test NOT IN
	notInExpr := &InSubqueryExpr{
		Column: "id",
		Subquery: &Query{
			TableName: valuesFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
		},
		Negate: true,
	}

	result, err = ctx.EvaluateExpression(row2, notInExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(NOT IN) error = %v", err)
	}
	if !result {
		t.Error("NOT IN should return true when value doesn't match")
	}

	// Test IN with missing column
	rowMissing := map[string]interface{}{"other": int64(1)}
	result, err = ctx.EvaluateExpression(rowMissing, inExpr)
	if err != nil {
		t.Errorf("EvaluateExpression(IN missing col) error = %v", err)
	}
	if result {
		t.Error("IN should return false when column is missing")
	}

	// Test IN with subquery returning multiple columns (should error)
	multiColFile := filepath.Join(tmpDir, "multicol.parquet")
	multiColRows := []map[string]interface{}{
		{"val": int64(1), "name": "test"},
	}
	createTestParquetFile(t, multiColFile, multiColRows)

	inMultiColExpr := &InSubqueryExpr{
		Column: "id",
		Subquery: &Query{
			TableName: multiColFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
				{Expr: &ColumnRef{Column: "name"}},
			},
		},
		Negate: false,
	}

	_, err = ctx.EvaluateExpression(row, inMultiColExpr)
	if err == nil {
		t.Error("Expected error for IN subquery with multiple columns, got nil")
	}
	if !strings.Contains(err.Error(), "exactly one column") {
		t.Errorf("Expected 'exactly one column' error, got: %v", err)
	}
}

// TestExecuteJoin_WithSubquery tests JOIN with subquery
func TestExecuteJoin_WithSubquery(t *testing.T) {
	tmpDir := t.TempDir()
	leftFile := filepath.Join(tmpDir, "left.parquet")
	rightFile := filepath.Join(tmpDir, "right.parquet")

	leftRows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice"},
	}
	createTestParquetFile(t, leftFile, leftRows)

	rightRows := []map[string]interface{}{
		{"val": int64(100)},
	}
	createTestParquetFile(t, rightFile, rightRows)

	r, err := reader.NewReader(leftFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	// Read left rows
	leftData, err := reader.ReadMultipleFiles(leftFile)
	if err != nil {
		t.Fatalf("ReadMultipleFiles(left) error = %v", err)
	}

	// JOIN with subquery - select only val to avoid column collision
	join := Join{
		Type: JoinInner,
		Subquery: &Query{
			TableName: rightFile,
			SelectList: []SelectItem{
				{Expr: &ColumnRef{Column: "val"}},
			},
		},
		// Simple condition that always matches (id = 1)
		Condition: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	results, err := ctx.executeJoin(leftData, "", join)
	if err != nil {
		t.Errorf("executeJoin(with subquery) error = %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}
	if len(results) > 0 {
		// Verify we have both name and val in the result
		if results[0]["name"] != "Alice" {
			t.Errorf("name = %v, want Alice", results[0]["name"])
		}
		if results[0]["val"] != int64(100) {
			t.Errorf("val = %v, want 100", results[0]["val"])
		}
	}
}

// TestMergeRows_ColumnCollision tests mergeRows with column name collision
func TestMergeRows_ColumnCollision(t *testing.T) {
	left := map[string]interface{}{
		"id":   int64(1),
		"name": "Alice",
	}

	right := map[string]interface{}{
		"id":  int64(1), // Collision with left
		"val": int64(100),
	}

	_, err := mergeRows(left, right)
	if err == nil {
		t.Error("Expected error for column name collision, got nil")
	}
	if !strings.Contains(err.Error(), "collision") {
		t.Errorf("Expected 'collision' error, got: %v", err)
	}
}

// TestMergeRows_FileColumn tests mergeRows with _file column handling
func TestMergeRows_FileColumn(t *testing.T) {
	left := map[string]interface{}{
		"id":    int64(1),
		"_file": "left.parquet",
	}

	right := map[string]interface{}{
		"val":   int64(100),
		"_file": "right.parquet",
	}

	merged, err := mergeRows(left, right)
	if err != nil {
		t.Errorf("mergeRows() error = %v", err)
	}

	// _file columns should be renamed to _file_left and _file_right
	if merged["_file_left"] != "left.parquet" {
		t.Errorf("_file_left = %v, want left.parquet", merged["_file_left"])
	}
	if merged["_file_right"] != "right.parquet" {
		t.Errorf("_file_right = %v, want right.parquet", merged["_file_right"])
	}
	if _, exists := merged["_file"]; exists {
		t.Error("_file should not exist in merged row")
	}
}

// TestCreateNullRow tests createNullRow helper
func TestCreateNullRow(t *testing.T) {
	// Test with non-empty rows
	rows := []map[string]interface{}{
		{"col1": "value1", "col2": int64(123)},
	}

	nullRow := createNullRow(rows)
	if nullRow == nil {
		t.Fatal("createNullRow returned nil")
	}

	// All columns should exist with nil values
	if nullRow["col1"] != nil {
		t.Errorf("col1 should be nil, got %v", nullRow["col1"])
	}
	if nullRow["col2"] != nil {
		t.Errorf("col2 should be nil, got %v", nullRow["col2"])
	}

	// Test with empty rows
	emptyNullRow := createNullRow([]map[string]interface{}{})
	if emptyNullRow == nil {
		t.Fatal("createNullRow with empty input returned nil")
	}
	if len(emptyNullRow) != 0 {
		t.Errorf("createNullRow with empty input should return empty map, got %v", emptyNullRow)
	}
}

// TestApplyTableAlias tests applyTableAlias helper
func TestApplyTableAlias(t *testing.T) {
	rows := []map[string]interface{}{
		{"col1": "value1", "col2": int64(123), "_file": "test.parquet"},
	}

	// Test with alias
	aliased := applyTableAlias(rows, "t")
	if len(aliased) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(aliased))
	}

	// Columns should be prefixed except _file
	if aliased[0]["t.col1"] != "value1" {
		t.Errorf("t.col1 = %v, want value1", aliased[0]["t.col1"])
	}
	if aliased[0]["t.col2"] != int64(123) {
		t.Errorf("t.col2 = %v, want 123", aliased[0]["t.col2"])
	}
	if aliased[0]["_file"] != "test.parquet" {
		t.Errorf("_file should not be aliased, got %v", aliased[0]["_file"])
	}

	// Original column names should not exist
	if _, exists := aliased[0]["col1"]; exists {
		t.Error("col1 should not exist after aliasing")
	}

	// Test with empty alias
	noAlias := applyTableAlias(rows, "")
	if len(noAlias) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(noAlias))
	}
	// Should return rows unchanged
	if noAlias[0]["col1"] != "value1" {
		t.Errorf("With empty alias, col1 = %v, want value1", noAlias[0]["col1"])
	}
}

// TestExecuteJoin_EmptySides tests join operations with empty sides
func TestExecuteJoin_EmptySides(t *testing.T) {
	// LEFT JOIN with empty right side
	leftRows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice"},
	}
	rightRowsEmpty := []map[string]interface{}{}

	condition := &ComparisonExpr{
		Column:   "id",
		Operator: TokenEqual,
		Value:    int64(1),
	}

	result, err := executeLeftJoin(leftRows, rightRowsEmpty, condition)
	if err != nil {
		t.Errorf("executeLeftJoin with empty right error = %v", err)
	}
	// Should return left rows unchanged
	if len(result) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result))
	}

	// RIGHT JOIN with empty left side
	result, err = executeRightJoin([]map[string]interface{}{}, rightRowsEmpty, condition)
	if err != nil {
		t.Errorf("executeRightJoin with empty left error = %v", err)
	}
	// Should return empty result
	if len(result) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result))
	}

	// FULL JOIN with empty left side
	rightRows := []map[string]interface{}{
		{"id": int64(1), "val": int64(100)},
	}
	result, err = executeFullJoin([]map[string]interface{}{}, rightRows, condition)
	if err != nil {
		t.Errorf("executeFullJoin with empty left error = %v", err)
	}
	// Should return right rows unchanged
	if len(result) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result))
	}

	// FULL JOIN with empty right side
	result, err = executeFullJoin(leftRows, []map[string]interface{}{}, condition)
	if err != nil {
		t.Errorf("executeFullJoin with empty right error = %v", err)
	}
	// Should return left rows unchanged
	if len(result) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result))
	}
}

// TestExecuteJoin_ErrorHandling tests join error conditions
func TestExecuteJoin_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	rows := []map[string]interface{}{
		{"id": int64(1)},
	}
	createTestParquetFile(t, testFile, rows)

	r, err := reader.NewReader(testFile)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx := NewExecutionContext(r)

	leftData := []map[string]interface{}{
		{"id": int64(1)},
	}

	// Test JOIN with no table name or subquery
	joinNoSource := Join{
		Type: JoinInner,
		Condition: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	_, err = ctx.executeJoin(leftData, "", joinNoSource)
	if err == nil {
		t.Error("Expected error for JOIN with no table name or subquery, got nil")
	}
	if !strings.Contains(err.Error(), "requires table name or subquery") {
		t.Errorf("Expected 'requires table name or subquery' error, got: %v", err)
	}

	// Test JOIN with forward CTE reference
	ctx.AllCTENames["future_cte"] = true

	joinForwardCTE := Join{
		Type:      JoinInner,
		TableName: "future_cte",
		Condition: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	_, err = ctx.executeJoin(leftData, "", joinForwardCTE)
	if err == nil {
		t.Error("Expected error for forward CTE reference in JOIN, got nil")
	}
	if !strings.Contains(err.Error(), "forward CTE reference") {
		t.Errorf("Expected 'forward CTE reference' error, got: %v", err)
	}

	// Test unsupported join type
	joinUnsupported := Join{
		Type:      JoinType(999), // Invalid join type
		TableName: testFile,
		Condition: &ComparisonExpr{
			Column:   "id",
			Operator: TokenEqual,
			Value:    int64(1),
		},
	}

	_, err = ctx.executeJoin(leftData, "", joinUnsupported)
	if err == nil {
		t.Error("Expected error for unsupported join type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported join type") {
		t.Errorf("Expected 'unsupported join type' error, got: %v", err)
	}
}
