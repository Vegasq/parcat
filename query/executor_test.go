package query

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/vegasq/parcat/reader"
)

// createTestParquetFile creates a test parquet file with the given rows
func createTestParquetFile(t *testing.T, path string, rows []map[string]interface{}) {
	t.Helper()

	// Convert to typed structs for parquet writer
	type Row struct {
		Name string `parquet:"name"`
		Age  int64  `parquet:"age"`
	}

	var typedRows []Row
	for _, row := range rows {
		typedRow := Row{}
		if v, ok := row["name"].(string); ok {
			typedRow.Name = v
		}
		if v, ok := row["age"].(int64); ok {
			typedRow.Age = v
		} else if v, ok := row["age"].(int); ok {
			typedRow.Age = int64(v)
		}
		typedRows = append(typedRows, typedRow)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer func() { _ = f.Close() }()

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(typedRows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
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
