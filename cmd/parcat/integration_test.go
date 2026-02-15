package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/vegasq/parcat/query"
)

// TestRow defines a simple test data structure
type TestRow struct {
	ID     int64   `parquet:"id"`
	Name   string  `parquet:"name"`
	Age    int64   `parquet:"age"`
	Salary float64 `parquet:"salary"`
}

// createTestParquetFile creates a temporary parquet file with test data
func createTestParquetFile(t *testing.T, dir, filename string, rows []TestRow) string {
	t.Helper()
	testFile := filepath.Join(dir, filename)

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[TestRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	return testFile
}

func TestMain_BasicQuery(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "test.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0},
	})

	// Reset flags for test
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	queryFlag = flag.String("q", "", "SQL query")
	formatFlag = flag.String("f", "jsonl", "Output format")
	limitFlag = flag.Int("limit", 0, "Limit number of rows")
	schemaFlag = flag.Bool("schema", false, "Show schema")

	// Set up test flags
	os.Args = []string{"parcat", "-q", "select * from test.parquet", testFile}
	flag.Parse()

	// This would normally call main(), but since main() calls os.Exit(),
	// we can't test it directly. Instead, we'll test the components.
	// For now, this test validates that the file exists and is readable.
	if _, err := os.Stat(testFile); err != nil {
		t.Errorf("Test file not accessible: %v", err)
	}
}

func TestMain_SchemaMode(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "test.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
	})

	// Reset flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	queryFlag = flag.String("q", "", "SQL query")
	formatFlag = flag.String("f", "jsonl", "Output format")
	limitFlag = flag.Int("limit", 0, "Limit number of rows")
	schemaFlag = flag.Bool("schema", false, "Show schema")

	// Test schema mode
	os.Args = []string{"parcat", "--schema", testFile}

	// We can't directly test main() due to os.Exit(), but we can test handleSchemaMode
	t.Run("schema_jsonl", func(t *testing.T) {
		// Capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		handleSchemaMode(testFile, "jsonl")

		_ = w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		if _, err := buf.ReadFrom(r); err != nil {
			t.Fatalf("failed to read from pipe: %v", err)
		}
		output := buf.String()

		// Verify schema output contains expected fields
		if !strings.Contains(output, "name") {
			t.Errorf("Schema output missing 'name' field")
		}
		if !strings.Contains(output, "type") {
			t.Errorf("Schema output missing 'type' field")
		}
	})

	t.Run("schema_csv", func(t *testing.T) {
		// Capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		handleSchemaMode(testFile, "csv")

		_ = w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		if _, err := buf.ReadFrom(r); err != nil {
			t.Fatalf("failed to read from pipe: %v", err)
		}
		output := buf.String()

		// Verify CSV header
		if !strings.Contains(output, "name,type") && !strings.Contains(output, "type,name") {
			t.Errorf("CSV schema output missing expected headers")
		}
	})
}

func TestMain_JoinOperations(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create two test files for joining
	file1 := createTestParquetFile(t, tmpDir, "users.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
	})

	type DeptRow struct {
		UserID int64  `parquet:"user_id"`
		Dept   string `parquet:"dept"`
	}

	// Create second file
	file2Path := filepath.Join(tmpDir, "departments.parquet")
	f, err := os.Create(file2Path)
	if err != nil {
		t.Fatalf("failed to create dept file: %v", err)
	}
	defer func() {
		_ = f.Close()
	}()

	deptRows := []DeptRow{
		{UserID: 1, Dept: "Engineering"},
		{UserID: 2, Dept: "Sales"},
	}

	writer := parquet.NewGenericWriter[DeptRow](f)
	if _, err := writer.Write(deptRows); err != nil {
		t.Fatalf("failed to write dept data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Verify files exist
	if _, err := os.Stat(file1); err != nil {
		t.Errorf("Users file not accessible: %v", err)
	}
	if _, err := os.Stat(file2Path); err != nil {
		t.Errorf("Departments file not accessible: %v", err)
	}

	// Note: Full JOIN testing would require running main() which calls os.Exit()
	// Testing is limited to file setup and helper functions
}

func TestMain_CTEQueries(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "data.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0},
	})

	// Verify file exists for CTE testing
	if _, err := os.Stat(testFile); err != nil {
		t.Errorf("Test file not accessible: %v", err)
	}

	// Note: Full CTE testing requires running queries through the full pipeline
	// which is limited by main() calling os.Exit()
}

func TestApplyTableAliasHelper(t *testing.T) {
	tests := []struct {
		name  string
		rows  []map[string]interface{}
		alias string
		want  []map[string]interface{}
	}{
		{
			name: "basic alias",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
			},
			alias: "t",
			want: []map[string]interface{}{
				{"t.id": 1, "t.name": "Alice"},
			},
		},
		{
			name: "empty alias returns original",
			rows: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
			},
			alias: "",
			want: []map[string]interface{}{
				{"id": 1, "name": "Alice"},
			},
		},
		{
			name: "_file column not aliased",
			rows: []map[string]interface{}{
				{"id": 1, "_file": "test.parquet"},
			},
			alias: "t",
			want: []map[string]interface{}{
				{"t.id": 1, "_file": "test.parquet"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyTableAliasHelper(tt.rows, tt.alias)

			if len(got) != len(tt.want) {
				t.Errorf("applyTableAliasHelper() returned %d rows, want %d", len(got), len(tt.want))
				return
			}

			for i, wantRow := range tt.want {
				gotRow := got[i]
				if len(gotRow) != len(wantRow) {
					t.Errorf("row %d: got %d columns, want %d", i, len(gotRow), len(wantRow))
					continue
				}

				for key, wantVal := range wantRow {
					gotVal, exists := gotRow[key]
					if !exists {
						t.Errorf("row %d: missing key %q", i, key)
						continue
					}
					if gotVal != wantVal {
						t.Errorf("row %d, key %q: got %v, want %v", i, key, gotVal, wantVal)
					}
				}
			}
		})
	}
}

func TestMergeRowsHelper(t *testing.T) {
	tests := []struct {
		name    string
		left    map[string]interface{}
		right   map[string]interface{}
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name:  "basic merge",
			left:  map[string]interface{}{"a": 1, "b": 2},
			right: map[string]interface{}{"c": 3, "d": 4},
			want:  map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 4},
		},
		{
			name:    "collision error",
			left:    map[string]interface{}{"a": 1, "b": 2},
			right:   map[string]interface{}{"b": 3, "c": 4},
			wantErr: true,
		},
		{
			name:  "_file collision handled",
			left:  map[string]interface{}{"a": 1, "_file": "left.parquet"},
			right: map[string]interface{}{"b": 2, "_file": "right.parquet"},
			want:  map[string]interface{}{"a": 1, "b": 2, "_file_left": "left.parquet", "_file_right": "right.parquet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mergeRowsHelper(tt.left, tt.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("mergeRowsHelper() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("mergeRowsHelper() got %d keys, want %d", len(got), len(tt.want))
				return
			}

			for key, wantVal := range tt.want {
				gotVal, exists := got[key]
				if !exists {
					t.Errorf("missing key %q", key)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("key %q: got %v, want %v", key, gotVal, wantVal)
				}
			}
		})
	}
}

func TestCreateNullRowHelper(t *testing.T) {
	tests := []struct {
		name string
		rows []map[string]interface{}
		want map[string]interface{}
	}{
		{
			name: "creates null row from sample",
			rows: []map[string]interface{}{
				{"a": 1, "b": 2, "c": 3},
			},
			want: map[string]interface{}{"a": nil, "b": nil, "c": nil},
		},
		{
			name: "empty input returns empty map",
			rows: []map[string]interface{}{},
			want: map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createNullRowHelper(tt.rows)

			if len(got) != len(tt.want) {
				t.Errorf("createNullRowHelper() got %d keys, want %d", len(got), len(tt.want))
				return
			}

			for key, wantVal := range tt.want {
				gotVal, exists := got[key]
				if !exists {
					t.Errorf("missing key %q", key)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("key %q: got %v, want %v", key, gotVal, wantVal)
				}
			}
		})
	}
}

// Note: executeInnerJoinHelper is tested indirectly through integration tests
// Direct unit testing requires complex query.Expression setup

func TestExecuteCrossJoinHelper(t *testing.T) {
	tests := []struct {
		name      string
		leftRows  []map[string]interface{}
		rightRows []map[string]interface{}
		wantCount int
	}{
		{
			name: "2x2 cross join",
			leftRows: []map[string]interface{}{
				{"a": 1},
				{"a": 2},
			},
			rightRows: []map[string]interface{}{
				{"b": 3},
				{"b": 4},
			},
			wantCount: 4, // Cartesian product: 2 * 2
		},
		{
			name:      "empty left",
			leftRows:  []map[string]interface{}{},
			rightRows: []map[string]interface{}{{"b": 1}},
			wantCount: 0,
		},
		{
			name:      "empty right",
			leftRows:  []map[string]interface{}{{"a": 1}},
			rightRows: []map[string]interface{}{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := executeCrossJoinHelper(tt.leftRows, tt.rightRows)
			if err != nil {
				t.Errorf("executeCrossJoinHelper() error = %v", err)
				return
			}

			if len(got) != tt.wantCount {
				t.Errorf("executeCrossJoinHelper() got %d rows, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestHandleSchemaMode_GlobPattern(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create multiple test files
	createTestParquetFile(t, tmpDir, "file1.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
	})
	createTestParquetFile(t, tmpDir, "file2.parquet", []TestRow{
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
	})

	// Test with glob pattern
	pattern := filepath.Join(tmpDir, "*.parquet")

	// Capture output
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stdout = w
	os.Stderr = w

	handleSchemaMode(pattern, "jsonl")

	_ = w.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	// Verify schema output was produced
	if !strings.Contains(output, "name") && !strings.Contains(output, "type") {
		t.Errorf("Schema output missing expected fields")
	}
}

func TestMain_MultipleFiles(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create multiple test files
	createTestParquetFile(t, tmpDir, "data1.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
	})
	createTestParquetFile(t, tmpDir, "data2.parquet", []TestRow{
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
	})

	// Verify glob pattern matches files
	pattern := filepath.Join(tmpDir, "data*.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Errorf("Glob pattern failed: %v", err)
	}
	if len(matches) != 2 {
		t.Errorf("Expected 2 matching files, got %d", len(matches))
	}
}

func TestExecuteLeftJoinHelper(t *testing.T) {
	tests := []struct {
		name      string
		leftRows  []map[string]interface{}
		rightRows []map[string]interface{}
		wantCount int
		wantKeys  []string // Keys to verify in first result row
	}{
		{
			name: "basic left join with matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
				{"t1.id": int64(2), "t1.name": "Bob"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
			},
			wantCount: 2, // Both left rows (one matched, one with nulls)
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "left join with no matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
				{"t1.id": int64(2), "t1.name": "Bob"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(99), "t2.dept": "Engineering"},
			},
			wantCount: 2, // Both left rows with null right columns
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "left join with empty right side",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
				{"t1.id": int64(2), "t1.name": "Bob"},
			},
			rightRows: []map[string]interface{}{},
			wantCount: 2, // Returns left rows unchanged (special case)
			wantKeys:  []string{"t1.id", "t1.name"},
		},
		{
			name:      "left join with empty left side",
			leftRows:  []map[string]interface{}{},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create equality condition: t1.id == t2.id
			condition := &query.ColumnComparisonExpr{
				LeftColumn:  "t1.id",
				Operator:    query.TokenEqual,
				RightColumn: "t2.id",
			}

			got, err := executeLeftJoinHelper(tt.leftRows, tt.rightRows, condition)
			if err != nil {
				t.Errorf("executeLeftJoinHelper() error = %v", err)
				return
			}

			if len(got) != tt.wantCount {
				t.Errorf("executeLeftJoinHelper() got %d rows, want %d", len(got), tt.wantCount)
			}

			// Verify keys in first row if we have results
			if len(got) > 0 && len(tt.wantKeys) > 0 {
				for _, key := range tt.wantKeys {
					if _, exists := got[0][key]; !exists {
						t.Errorf("executeLeftJoinHelper() first row missing key %q", key)
					}
				}
			}
		})
	}
}

func TestExecuteRightJoinHelper(t *testing.T) {
	tests := []struct {
		name      string
		leftRows  []map[string]interface{}
		rightRows []map[string]interface{}
		wantCount int
		wantKeys  []string
	}{
		{
			name: "basic right join with matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
				{"t2.id": int64(2), "t2.dept": "Sales"},
			},
			wantCount: 2, // Both right rows (one matched, one with nulls)
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "right join with no matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(99), "t1.name": "Alice"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
				{"t2.id": int64(2), "t2.dept": "Sales"},
			},
			wantCount: 2, // Both right rows with null left columns
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "right join with empty left side",
			leftRows: []map[string]interface{}{},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
				{"t2.id": int64(2), "t2.dept": "Sales"},
			},
			wantCount: 2, // Returns right rows unchanged (special case)
			wantKeys:  []string{"t2.id", "t2.dept"},
		},
		{
			name: "right join with empty right side",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
			},
			rightRows: []map[string]interface{}{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create equality condition: t1.id == t2.id
			condition := &query.ColumnComparisonExpr{
				LeftColumn:  "t1.id",
				Operator:    query.TokenEqual,
				RightColumn: "t2.id",
			}

			got, err := executeRightJoinHelper(tt.leftRows, tt.rightRows, condition)
			if err != nil {
				t.Errorf("executeRightJoinHelper() error = %v", err)
				return
			}

			if len(got) != tt.wantCount {
				t.Errorf("executeRightJoinHelper() got %d rows, want %d", len(got), tt.wantCount)
			}

			// Verify keys in first row if we have results
			if len(got) > 0 && len(tt.wantKeys) > 0 {
				for _, key := range tt.wantKeys {
					if _, exists := got[0][key]; !exists {
						t.Errorf("executeRightJoinHelper() first row missing key %q", key)
					}
				}
			}
		})
	}
}

func TestExecuteFullJoinHelper(t *testing.T) {
	tests := []struct {
		name      string
		leftRows  []map[string]interface{}
		rightRows []map[string]interface{}
		wantCount int
		wantKeys  []string
	}{
		{
			name: "basic full join with partial matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
				{"t1.id": int64(2), "t1.name": "Bob"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
				{"t2.id": int64(3), "t2.dept": "Sales"},
			},
			wantCount: 3, // 1 matched + 1 unmatched left + 1 unmatched right
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "full join with no matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(2), "t2.dept": "Engineering"},
			},
			wantCount: 2, // 1 left with nulls + 1 right with nulls
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
		{
			name: "full join with empty left side",
			leftRows: []map[string]interface{}{},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
			},
			wantCount: 1, // Returns right rows unchanged (special case)
			wantKeys:  []string{"t2.id", "t2.dept"},
		},
		{
			name: "full join with empty right side",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
			},
			rightRows: []map[string]interface{}{},
			wantCount: 1, // Returns left rows unchanged (special case)
			wantKeys:  []string{"t1.id", "t1.name"},
		},
		{
			name: "full join with all matches",
			leftRows: []map[string]interface{}{
				{"t1.id": int64(1), "t1.name": "Alice"},
				{"t1.id": int64(2), "t1.name": "Bob"},
			},
			rightRows: []map[string]interface{}{
				{"t2.id": int64(1), "t2.dept": "Engineering"},
				{"t2.id": int64(2), "t2.dept": "Sales"},
			},
			wantCount: 2, // All matched, no unmatched rows
			wantKeys:  []string{"t1.id", "t1.name", "t2.id", "t2.dept"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create equality condition: t1.id == t2.id
			condition := &query.ColumnComparisonExpr{
				LeftColumn:  "t1.id",
				Operator:    query.TokenEqual,
				RightColumn: "t2.id",
			}

			got, err := executeFullJoinHelper(tt.leftRows, tt.rightRows, condition)
			if err != nil {
				t.Errorf("executeFullJoinHelper() error = %v", err)
				return
			}

			if len(got) != tt.wantCount {
				t.Errorf("executeFullJoinHelper() got %d rows, want %d", len(got), tt.wantCount)
			}

			// Verify keys in first row if we have results
			if len(got) > 0 && len(tt.wantKeys) > 0 {
				for _, key := range tt.wantKeys {
					if _, exists := got[0][key]; !exists {
						t.Errorf("executeFullJoinHelper() first row missing key %q", key)
					}
				}
			}
		})
	}
}

func TestExecuteCTEQuery_NestedCTEs(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "data.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
		{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0},
	})

	tests := []struct {
		name      string
		setupFunc func() (*query.Query, *query.ExecutionContext)
		wantCount int
		wantErr   bool
	}{
		{
			name: "single CTE",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				// WITH temp AS (SELECT * FROM data.parquet) SELECT * FROM temp
				ctx := query.NewExecutionContext(nil)
				cte := &query.CTE{
					Name: "temp",
					Query: &query.Query{
						TableName:  testFile,
						SelectList: []query.SelectItem{},
					},
				}
				q := &query.Query{
					CTEs:       []query.CTE{*cte},
					TableName:  "temp",
					SelectList: []query.SelectItem{},
				}
				return q, ctx
			},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name: "nested CTEs",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				// WITH cte1 AS (...), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2
				ctx := query.NewExecutionContext(nil)
				cte1 := &query.CTE{
					Name: "cte1",
					Query: &query.Query{
						TableName:  testFile,
						SelectList: []query.SelectItem{},
					},
				}
				cte2 := &query.CTE{
					Name: "cte2",
					Query: &query.Query{
						TableName:  "cte1",
						SelectList: []query.SelectItem{},
					},
				}
				q := &query.Query{
					CTEs:       []query.CTE{*cte1, *cte2},
					TableName:  "cte2",
					SelectList: []query.SelectItem{},
				}
				return q, ctx
			},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name: "circular dependency error",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				// WITH cte1 AS (SELECT * FROM cte1) SELECT * FROM cte1
				ctx := query.NewExecutionContext(nil)
				cte1 := &query.CTE{
					Name: "cte1",
					Query: &query.Query{
						TableName:  "cte1", // References itself
						SelectList: []query.SelectItem{},
					},
				}
				q := &query.Query{
					CTEs:       []query.CTE{*cte1},
					TableName:  "cte1",
					SelectList: []query.SelectItem{},
				}
				return q, ctx
			},
			wantErr: true,
		},
		{
			name: "forward CTE reference error",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				// WITH cte1 AS (SELECT * FROM cte2), cte2 AS (...) SELECT * FROM cte1
				// This should fail because cte1 references cte2 before it's defined
				ctx := query.NewExecutionContext(nil)
				cte1 := &query.CTE{
					Name: "cte1",
					Query: &query.Query{
						TableName:  "cte2", // Forward reference
						SelectList: []query.SelectItem{},
					},
				}
				cte2 := &query.CTE{
					Name: "cte2",
					Query: &query.Query{
						TableName:  testFile,
						SelectList: []query.SelectItem{},
					},
				}
				q := &query.Query{
					CTEs:       []query.CTE{*cte1, *cte2},
					TableName:  "cte1",
					SelectList: []query.SelectItem{},
				}
				return q, ctx
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, ctx := tt.setupFunc()
			got, err := executeCTEQuery(q, ctx)

			if (err != nil) != tt.wantErr {
				t.Errorf("executeCTEQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(got) != tt.wantCount {
				t.Errorf("executeCTEQuery() got %d rows, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestExecuteCTEQuery_WithSubqueries(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "data.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
		{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0},
	})

	tests := []struct {
		name      string
		setupFunc func() (*query.Query, *query.ExecutionContext)
		wantCount int
		wantErr   bool
	}{
		{
			name: "subquery in FROM clause",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				ctx := query.NewExecutionContext(nil)
				subquery := &query.Query{
					TableName:  testFile,
					SelectList: []query.SelectItem{},
				}
				q := &query.Query{
					Subquery:   subquery,
					SelectList: []query.SelectItem{},
				}
				return q, ctx
			},
			wantCount: 2,
			wantErr:   false,
		},
		{
			name: "no data source error",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				ctx := query.NewExecutionContext(nil)
				q := &query.Query{
					SelectList: []query.SelectItem{},
					// No TableName, Subquery, or CTEs
				}
				return q, ctx
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, ctx := tt.setupFunc()
			got, err := executeCTEQuery(q, ctx)

			if (err != nil) != tt.wantErr {
				t.Errorf("executeCTEQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(got) != tt.wantCount {
				t.Errorf("executeCTEQuery() got %d rows, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestExecuteCTEQuery_WithJoinErrors(t *testing.T) {
	// Create temporary directory and test file
	tmpDir := t.TempDir()
	testFile := createTestParquetFile(t, tmpDir, "data.parquet", []TestRow{
		{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0},
	})

	tests := []struct {
		name      string
		setupFunc func() (*query.Query, *query.ExecutionContext)
		wantErr   bool
		errMsg    string
	}{
		{
			name: "circular CTE dependency in JOIN",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				ctx := query.NewExecutionContext(nil)
				// Simulate in-progress CTE
				ctx.InProgress = map[string]bool{"cte1": true}

				q := &query.Query{
					TableName:  testFile,
					SelectList: []query.SelectItem{},
					Joins: []query.Join{
						{
							TableName: "cte1", // References CTE being materialized
							Type:      query.JoinInner,
							Condition: &query.ColumnComparisonExpr{
								LeftColumn:  "id",
								Operator:    query.TokenEqual,
								RightColumn: "id",
							},
						},
					},
				}
				return q, ctx
			},
			wantErr: true,
			errMsg:  "circular CTE dependency",
		},
		{
			name: "forward CTE reference in JOIN",
			setupFunc: func() (*query.Query, *query.ExecutionContext) {
				ctx := query.NewExecutionContext(nil)
				ctx.AllCTENames = map[string]bool{"future_cte": true}

				q := &query.Query{
					TableName:  testFile,
					SelectList: []query.SelectItem{},
					Joins: []query.Join{
						{
							TableName: "future_cte", // Forward reference
							Type:      query.JoinInner,
							Condition: &query.ColumnComparisonExpr{
								LeftColumn:  "id",
								Operator:    query.TokenEqual,
								RightColumn: "id",
							},
						},
					},
				}
				return q, ctx
			},
			wantErr: true,
			errMsg:  "forward CTE reference",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, ctx := tt.setupFunc()
			_, err := executeCTEQuery(q, ctx)

			if (err != nil) != tt.wantErr {
				t.Errorf("executeCTEQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("executeCTEQuery() error = %v, want error containing %q", err, tt.errMsg)
			}
		})
	}
}
