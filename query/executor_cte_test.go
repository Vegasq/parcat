package query

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/vegasq/parcat/reader"
)

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
