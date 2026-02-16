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
