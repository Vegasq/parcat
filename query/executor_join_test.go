package query

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vegasq/parcat/reader"
)

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
