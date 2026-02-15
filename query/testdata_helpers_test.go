package query

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestCreateBasicParquetFile(t *testing.T) {
	tests := []struct {
		name string
		rows []BasicDataRow
	}{
		{
			name: "single row",
			rows: []BasicDataRow{
				{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 95.5},
			},
		},
		{
			name: "multiple rows",
			rows: []BasicDataRow{
				{ID: 1, Name: "Alice", Age: 30, Salary: 50000.0, Active: true, Score: 95.5},
				{ID: 2, Name: "Bob", Age: 25, Salary: 45000.0, Active: false, Score: 88.0},
				{ID: 3, Name: "Charlie", Age: 35, Salary: 60000.0, Active: true, Score: 92.3},
			},
		},
		{
			name: "empty rows",
			rows: []BasicDataRow{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := createBasicParquetFile(t, tt.rows)

			// Verify file exists
			if _, err := os.Stat(filePath); err != nil {
				t.Fatalf("created file does not exist: %v", err)
			}

			// Open and verify we can read the file
			f, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("failed to open created file: %v", err)
			}
			defer func() { _ = f.Close() }()

			stat, err := f.Stat()
			if err != nil {
				t.Fatalf("failed to stat file: %v", err)
			}

			pf, err := parquet.OpenFile(f, stat.Size())
			if err != nil {
				t.Fatalf("failed to open parquet file: %v", err)
			}

			// Verify row count
			if pf.NumRows() != int64(len(tt.rows)) {
				t.Errorf("expected %d rows, got %d", len(tt.rows), pf.NumRows())
			}

			// Verify we can read the rows back
			reader := parquet.NewGenericReader[BasicDataRow](f)
			defer func() { _ = reader.Close() }()

			readRows := make([]BasicDataRow, len(tt.rows))
			n, err := reader.Read(readRows)
			if err != nil && n != len(tt.rows) {
				t.Fatalf("failed to read rows: %v", err)
			}

			// Verify row contents
			for i := 0; i < n; i++ {
				if readRows[i] != tt.rows[i] {
					t.Errorf("row %d mismatch: got %+v, want %+v", i, readRows[i], tt.rows[i])
				}
			}
		})
	}
}

func TestCreateComplexParquetFile(t *testing.T) {
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		rows []ComplexDataRow
	}{
		{
			name: "with nullable fields",
			rows: []ComplexDataRow{
				{
					ID:        1,
					Name:      "Alice",
					Age:       int64Ptr(30),
					Timestamp: ts,
					Salary:    float64Ptr(50000.0),
					Active:    boolPtr(true),
					Tags:      []string{"engineer", "senior"},
					Score:     float64Ptr(95.5),
				},
			},
		},
		{
			name: "with null values",
			rows: []ComplexDataRow{
				{
					ID:        1,
					Name:      "Bob",
					Age:       nil,
					Timestamp: ts,
					Salary:    nil,
					Active:    nil,
					Tags:      []string{},
					Score:     nil,
				},
			},
		},
		{
			name: "mixed null and non-null",
			rows: []ComplexDataRow{
				{
					ID:        1,
					Name:      "Alice",
					Age:       int64Ptr(30),
					Timestamp: ts,
					Salary:    float64Ptr(50000.0),
					Active:    boolPtr(true),
					Tags:      []string{"engineer"},
					Score:     float64Ptr(95.5),
				},
				{
					ID:        2,
					Name:      "Bob",
					Age:       nil,
					Timestamp: ts,
					Salary:    nil,
					Active:    boolPtr(false),
					Tags:      []string{},
					Score:     nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := createComplexParquetFile(t, tt.rows)

			// Verify file exists
			if _, err := os.Stat(filePath); err != nil {
				t.Fatalf("created file does not exist: %v", err)
			}

			// Open and verify we can read the file
			f, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("failed to open created file: %v", err)
			}
			defer func() { _ = f.Close() }()

			stat, err := f.Stat()
			if err != nil {
				t.Fatalf("failed to stat file: %v", err)
			}

			pf, err := parquet.OpenFile(f, stat.Size())
			if err != nil {
				t.Fatalf("failed to open parquet file: %v", err)
			}

			// Verify row count
			if pf.NumRows() != int64(len(tt.rows)) {
				t.Errorf("expected %d rows, got %d", len(tt.rows), pf.NumRows())
			}
		})
	}
}

func TestCreateEmptyParquetFile(t *testing.T) {
	filePath := createEmptyParquetFile(t)

	// Verify file exists
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("created file does not exist: %v", err)
	}

	// Open and verify it's a valid empty parquet file
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open created file: %v", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	// Verify row count is 0
	if pf.NumRows() != 0 {
		t.Errorf("expected 0 rows, got %d", pf.NumRows())
	}
}

func TestCreateNamedBasicParquetFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := "custom_name.parquet"
	rows := []BasicDataRow{
		{ID: 1, Name: "Test", Age: 25, Salary: 40000.0, Active: true, Score: 80.0},
	}

	filePath := createNamedBasicParquetFile(t, tmpDir, filename, rows)

	// Verify the file has the correct path
	expectedPath := filepath.Join(tmpDir, filename)
	if filePath != expectedPath {
		t.Errorf("expected path %s, got %s", expectedPath, filePath)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("created file does not exist: %v", err)
	}

	// Verify we can read the file
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open created file: %v", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	if pf.NumRows() != int64(len(rows)) {
		t.Errorf("expected %d rows, got %d", len(rows), pf.NumRows())
	}
}

func TestCreateNamedComplexParquetFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := "complex_custom.parquet"
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	rows := []ComplexDataRow{
		{
			ID:        1,
			Name:      "Test",
			Age:       int64Ptr(25),
			Timestamp: ts,
			Salary:    float64Ptr(40000.0),
			Active:    boolPtr(true),
			Tags:      []string{"test"},
			Score:     float64Ptr(80.0),
		},
	}

	filePath := createNamedComplexParquetFile(t, tmpDir, filename, rows)

	// Verify the file has the correct path
	expectedPath := filepath.Join(tmpDir, filename)
	if filePath != expectedPath {
		t.Errorf("expected path %s, got %s", expectedPath, filePath)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("created file does not exist: %v", err)
	}

	// Verify we can read the file
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open created file: %v", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	if pf.NumRows() != int64(len(rows)) {
		t.Errorf("expected %d rows, got %d", len(rows), pf.NumRows())
	}
}

func TestPointerHelpers(t *testing.T) {
	t.Run("int64Ptr", func(t *testing.T) {
		val := int64(42)
		ptr := int64Ptr(val)
		if ptr == nil {
			t.Fatal("expected non-nil pointer")
		}
		if *ptr != val {
			t.Errorf("expected %d, got %d", val, *ptr)
		}
	})

	t.Run("float64Ptr", func(t *testing.T) {
		val := 3.14
		ptr := float64Ptr(val)
		if ptr == nil {
			t.Fatal("expected non-nil pointer")
		}
		if *ptr != val {
			t.Errorf("expected %f, got %f", val, *ptr)
		}
	})

	t.Run("boolPtr", func(t *testing.T) {
		val := true
		ptr := boolPtr(val)
		if ptr == nil {
			t.Fatal("expected non-nil pointer")
		}
		if *ptr != val {
			t.Errorf("expected %v, got %v", val, *ptr)
		}
	})
}
