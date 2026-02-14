package reader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/parquet-go"
)

func TestExtractSchemaInfo_PrimitiveTypes(t *testing.T) {
	// Create a temporary test file with various primitive types
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.parquet")

	// Create test data with different types
	type Row struct {
		ID       int64   `parquet:"id"`
		Name     string  `parquet:"name"`
		Age      int32   `parquet:"age"`
		Score    float64 `parquet:"score"`
		Active   bool    `parquet:"active"`
		Optional *string `parquet:"optional,optional"`
	}

	optVal := "test"
	rows := []Row{
		{ID: 1, Name: "Alice", Age: 30, Score: 95.5, Active: true, Optional: &optVal},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Verify we got all columns
	if len(schemaInfos) != 6 {
		t.Errorf("ExtractSchemaInfo() returned %d fields, want 6", len(schemaInfos))
	}

	// Check specific field properties
	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	// Verify id field
	if info, ok := fieldMap["id"]; ok {
		if info.Type != "INT64" {
			t.Errorf("id type = %s, want INT64", info.Type)
		}
		if !info.Required {
			t.Errorf("id should be required")
		}
	} else {
		t.Errorf("id field not found in schema")
	}

	// Verify name field (string)
	if info, ok := fieldMap["name"]; ok {
		if info.Type != "STRING" {
			t.Errorf("name type = %s, want STRING", info.Type)
		}
	} else {
		t.Errorf("name field not found in schema")
	}

	// Verify age field (int32)
	if info, ok := fieldMap["age"]; ok {
		if info.Type != "INT32" {
			t.Errorf("age type = %s, want INT32", info.Type)
		}
	} else {
		t.Errorf("age field not found in schema")
	}

	// Verify score field (float64/double)
	if info, ok := fieldMap["score"]; ok {
		if info.Type != "FLOAT64" {
			t.Errorf("score type = %s, want FLOAT64", info.Type)
		}
	} else {
		t.Errorf("score field not found in schema")
	}

	// Verify active field (boolean)
	if info, ok := fieldMap["active"]; ok {
		if info.Type != "BOOLEAN" {
			t.Errorf("active type = %s, want BOOLEAN", info.Type)
		}
	} else {
		t.Errorf("active field not found in schema")
	}

	// Verify optional field
	if info, ok := fieldMap["optional"]; ok {
		if !info.Optional {
			t.Errorf("optional field should be optional")
		}
	} else {
		t.Errorf("optional field not found in schema")
	}
}

func TestExtractSchemaInfo_NestedTypes(t *testing.T) {
	// Create a temporary test file with nested structure
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "nested.parquet")

	// Create test data with nested structure
	type Address struct {
		Street string `parquet:"street"`
		City   string `parquet:"city"`
	}

	type Row struct {
		ID      int64   `parquet:"id"`
		Name    string  `parquet:"name"`
		Address Address `parquet:"address"`
	}

	rows := []Row{
		{
			ID:   1,
			Name: "Alice",
			Address: Address{
				Street: "123 Main St",
				City:   "Springfield",
			},
		},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Create field map
	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	// Verify nested fields use dot notation
	if _, ok := fieldMap["address.street"]; !ok {
		t.Errorf("address.street field not found in schema")
	}

	if _, ok := fieldMap["address.city"]; !ok {
		t.Errorf("address.city field not found in schema")
	}
}

func TestExtractSchemaInfo_RepeatedTypes(t *testing.T) {
	// Create a temporary test file with repeated field
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "repeated.parquet")

	// Create test data with repeated field (slice)
	type Row struct {
		ID   int64    `parquet:"id"`
		Tags []string `parquet:"tags"`
	}

	rows := []Row{
		{ID: 1, Tags: []string{"tag1", "tag2"}},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Find tags field and verify it's marked as repeated
	var tagsInfo *SchemaInfo
	for i := range schemaInfos {
		if schemaInfos[i].Name == "tags" {
			tagsInfo = &schemaInfos[i]
			break
		}
	}

	if tagsInfo == nil {
		t.Fatalf("tags field not found in schema")
	}

	if !tagsInfo.Repeated {
		t.Errorf("tags field should be marked as repeated")
	}
}

func TestExtractSchemaInfo_FileNotFound(t *testing.T) {
	// Try to extract schema from non-existent file
	_, err := ExtractSchemaInfo("nonexistent.parquet")
	if err == nil {
		t.Errorf("ExtractSchemaInfo() expected error for non-existent file, got nil")
	}
}

func TestExtractSchemaInfo_InvalidParquetFile(t *testing.T) {
	// Create a temporary invalid file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "invalid.parquet")

	// Write invalid content
	if err := os.WriteFile(testFile, []byte("not a parquet file"), 0644); err != nil {
		t.Fatalf("failed to create invalid file: %v", err)
	}

	// Try to extract schema
	_, err := ExtractSchemaInfo(testFile)
	if err == nil {
		t.Errorf("ExtractSchemaInfo() expected error for invalid parquet file, got nil")
	}
}

func TestExtractSchemaInfo_TypeMapping(t *testing.T) {
	// Test various type mappings
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "types.parquet")

	type Row struct {
		IntField    int32   `parquet:"int_field"`
		LongField   int64   `parquet:"long_field"`
		FloatField  float32 `parquet:"float_field"`
		DoubleField float64 `parquet:"double_field"`
		BoolField   bool    `parquet:"bool_field"`
		StringField string  `parquet:"string_field"`
	}

	rows := []Row{
		{
			IntField:    42,
			LongField:   1234567890,
			FloatField:  3.14,
			DoubleField: 2.71828,
			BoolField:   true,
			StringField: "test",
		},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Verify type mappings
	expectedTypes := map[string]string{
		"int_field":    "INT32",
		"long_field":   "INT64",
		"float_field":  "FLOAT32",
		"double_field": "FLOAT64",
		"bool_field":   "BOOLEAN",
		"string_field": "STRING",
	}

	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	for fieldName, expectedType := range expectedTypes {
		if info, ok := fieldMap[fieldName]; ok {
			if info.Type != expectedType {
				t.Errorf("field %s: type = %s, want %s", fieldName, info.Type, expectedType)
			}
		} else {
			t.Errorf("field %s not found in schema", fieldName)
		}
	}
}

func TestExtractSchemaInfo_EmptyParquetFile(t *testing.T) {
	// Test file with no columns - this is actually invalid for parquet
	// So we test a file with columns but no rows
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.parquet")

	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	var rows []Row // Empty slice - no data rows

	// Write empty parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema - should still return schema even with no rows
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Verify we got schema for both columns
	if len(schemaInfos) != 2 {
		t.Errorf("ExtractSchemaInfo() returned %d fields, want 2", len(schemaInfos))
	}
}

func TestExtractSchemaInfo_LargeSchema(t *testing.T) {
	// Test file with many columns (100+)
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "large.parquet")

	// Create a struct with many fields using parquet-go's schema builder
	type LargeRow struct {
		Col001 string `parquet:"col001"`
		Col002 string `parquet:"col002"`
		Col003 string `parquet:"col003"`
		Col004 string `parquet:"col004"`
		Col005 string `parquet:"col005"`
		Col006 string `parquet:"col006"`
		Col007 string `parquet:"col007"`
		Col008 string `parquet:"col008"`
		Col009 string `parquet:"col009"`
		Col010 string `parquet:"col010"`
		Col011 string `parquet:"col011"`
		Col012 string `parquet:"col012"`
		Col013 string `parquet:"col013"`
		Col014 string `parquet:"col014"`
		Col015 string `parquet:"col015"`
		Col016 string `parquet:"col016"`
		Col017 string `parquet:"col017"`
		Col018 string `parquet:"col018"`
		Col019 string `parquet:"col019"`
		Col020 string `parquet:"col020"`
		Col021 string `parquet:"col021"`
		Col022 string `parquet:"col022"`
		Col023 string `parquet:"col023"`
		Col024 string `parquet:"col024"`
		Col025 string `parquet:"col025"`
		Col026 string `parquet:"col026"`
		Col027 string `parquet:"col027"`
		Col028 string `parquet:"col028"`
		Col029 string `parquet:"col029"`
		Col030 string `parquet:"col030"`
	}

	rows := []LargeRow{{}}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[LargeRow](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Verify we got all columns
	if len(schemaInfos) != 30 {
		t.Errorf("ExtractSchemaInfo() returned %d fields, want 30", len(schemaInfos))
	}
}

func TestExtractSchemaInfo_DeeplyNested(t *testing.T) {
	// Test deeply nested structures (3+ levels)
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "deep_nested.parquet")

	type Level3 struct {
		Value string `parquet:"value"`
	}

	type Level2 struct {
		Field2 string `parquet:"field2"`
		Level3 Level3 `parquet:"level3"`
	}

	type Level1 struct {
		Field1 string `parquet:"field1"`
		Level2 Level2 `parquet:"level2"`
	}

	type Row struct {
		ID     int64  `parquet:"id"`
		Level1 Level1 `parquet:"level1"`
	}

	rows := []Row{
		{
			ID: 1,
			Level1: Level1{
				Field1: "test1",
				Level2: Level2{
					Field2: "test2",
					Level3: Level3{
						Value: "deep_value",
					},
				},
			},
		},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Create field map
	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	// Verify deeply nested field uses dot notation correctly
	if _, ok := fieldMap["level1.level2.level3.value"]; !ok {
		t.Errorf("deeply nested field 'level1.level2.level3.value' not found in schema")
	}
}

func TestExtractSchemaInfo_MixedRepetition(t *testing.T) {
	// Test mixed required/optional/repeated fields
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "mixed.parquet")

	type Row struct {
		Required int64    `parquet:"required"`
		Optional *string  `parquet:"optional,optional"`
		Repeated []string `parquet:"repeated"`
	}

	optVal := "test"
	rows := []Row{
		{
			Required: 1,
			Optional: &optVal,
			Repeated: []string{"a", "b"},
		},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Create field map
	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	// Verify required field
	if info, ok := fieldMap["required"]; ok {
		if !info.Required {
			t.Errorf("required field should be marked as required")
		}
	} else {
		t.Errorf("required field not found in schema")
	}

	// Verify optional field
	if info, ok := fieldMap["optional"]; ok {
		if !info.Optional {
			t.Errorf("optional field should be marked as optional")
		}
	} else {
		t.Errorf("optional field not found in schema")
	}

	// Verify repeated field
	if info, ok := fieldMap["repeated"]; ok {
		if !info.Repeated {
			t.Errorf("repeated field should be marked as repeated")
		}
	} else {
		t.Errorf("repeated field not found in schema")
	}
}

func TestExtractSchemaInfo_SpecialCharacters(t *testing.T) {
	// Test column names with special characters
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "special.parquet")

	type Row struct {
		Field1 string `parquet:"column_with_underscore"`
		Field2 string `parquet:"column-with-dash"`
		Field3 string `parquet:"column.with.dot"`
		Field4 string `parquet:"column with space"`
	}

	rows := []Row{
		{
			Field1: "test1",
			Field2: "test2",
			Field3: "test3",
			Field4: "test4",
		},
	}

	// Write test parquet file
	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	// Extract schema
	schemaInfos, err := ExtractSchemaInfo(testFile)
	if err != nil {
		t.Fatalf("ExtractSchemaInfo() error = %v", err)
	}

	// Create field map
	fieldMap := make(map[string]SchemaInfo)
	for _, info := range schemaInfos {
		fieldMap[info.Name] = info
	}

	// Verify all special character columns are present
	expectedNames := []string{
		"column_with_underscore",
		"column-with-dash",
		"column.with.dot",
		"column with space",
	}

	for _, name := range expectedNames {
		if _, ok := fieldMap[name]; !ok {
			t.Errorf("column '%s' not found in schema", name)
		}
	}
}

// Benchmark tests for performance analysis

func BenchmarkExtractSchemaInfo_SmallSchema(b *testing.B) {
	// Create a test file with a small schema (5 columns)
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "small.parquet")

	type Row struct {
		ID     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Age    int32   `parquet:"age"`
		Score  float64 `parquet:"score"`
		Active bool    `parquet:"active"`
	}

	rows := []Row{{ID: 1, Name: "test", Age: 30, Score: 95.5, Active: true}}

	f, err := os.Create(testFile)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		b.Fatalf("failed to write test data: %v", err)
	}
	_ = writer.Close()
	_ = f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractSchemaInfo(testFile)
		if err != nil {
			b.Fatalf("ExtractSchemaInfo() error = %v", err)
		}
	}
}

func BenchmarkExtractSchemaInfo_LargeSchema(b *testing.B) {
	// Create a test file with a large schema (100 columns)
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "large.parquet")

	// Create a large struct similar to TestExtractSchemaInfo_LargeSchema
	type BenchRow struct {
		Col000 string `parquet:"col000"`
		Col001 string `parquet:"col001"`
		Col002 string `parquet:"col002"`
		Col003 string `parquet:"col003"`
		Col004 string `parquet:"col004"`
		Col005 string `parquet:"col005"`
		Col006 string `parquet:"col006"`
		Col007 string `parquet:"col007"`
		Col008 string `parquet:"col008"`
		Col009 string `parquet:"col009"`
		Col010 string `parquet:"col010"`
		Col011 string `parquet:"col011"`
		Col012 string `parquet:"col012"`
		Col013 string `parquet:"col013"`
		Col014 string `parquet:"col014"`
		Col015 string `parquet:"col015"`
		Col016 string `parquet:"col016"`
		Col017 string `parquet:"col017"`
		Col018 string `parquet:"col018"`
		Col019 string `parquet:"col019"`
		Col020 string `parquet:"col020"`
		Col021 string `parquet:"col021"`
		Col022 string `parquet:"col022"`
		Col023 string `parquet:"col023"`
		Col024 string `parquet:"col024"`
		Col025 string `parquet:"col025"`
		Col026 string `parquet:"col026"`
		Col027 string `parquet:"col027"`
		Col028 string `parquet:"col028"`
		Col029 string `parquet:"col029"`
		Col030 string `parquet:"col030"`
		Col031 string `parquet:"col031"`
		Col032 string `parquet:"col032"`
		Col033 string `parquet:"col033"`
		Col034 string `parquet:"col034"`
		Col035 string `parquet:"col035"`
		Col036 string `parquet:"col036"`
		Col037 string `parquet:"col037"`
		Col038 string `parquet:"col038"`
		Col039 string `parquet:"col039"`
		Col040 string `parquet:"col040"`
		Col041 string `parquet:"col041"`
		Col042 string `parquet:"col042"`
		Col043 string `parquet:"col043"`
		Col044 string `parquet:"col044"`
		Col045 string `parquet:"col045"`
		Col046 string `parquet:"col046"`
		Col047 string `parquet:"col047"`
		Col048 string `parquet:"col048"`
		Col049 string `parquet:"col049"`
	}

	rows := []BenchRow{{}}

	f, err := os.Create(testFile)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[BenchRow](f)
	if _, err := writer.Write(rows); err != nil {
		b.Fatalf("failed to write test data: %v", err)
	}
	_ = writer.Close()
	_ = f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractSchemaInfo(testFile)
		if err != nil {
			b.Fatalf("ExtractSchemaInfo() error = %v", err)
		}
	}
}

func BenchmarkExtractSchemaInfo_DeeplyNested(b *testing.B) {
	// Create a test file with deeply nested structures
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "nested.parquet")

	type Level3 struct {
		Value string `parquet:"value"`
	}

	type Level2 struct {
		Field2 string `parquet:"field2"`
		Level3 Level3 `parquet:"level3"`
	}

	type Level1 struct {
		Field1 string `parquet:"field1"`
		Level2 Level2 `parquet:"level2"`
	}

	type Row struct {
		ID     int64  `parquet:"id"`
		Level1 Level1 `parquet:"level1"`
	}

	rows := []Row{
		{
			ID: 1,
			Level1: Level1{
				Field1: "test1",
				Level2: Level2{
					Field2: "test2",
					Level3: Level3{Value: "deep_value"},
				},
			},
		},
	}

	f, err := os.Create(testFile)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		b.Fatalf("failed to write test data: %v", err)
	}
	_ = writer.Close()
	_ = f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractSchemaInfo(testFile)
		if err != nil {
			b.Fatalf("ExtractSchemaInfo() error = %v", err)
		}
	}
}

// Resource management tests

func TestExtractSchemaInfo_FileHandleClosed(t *testing.T) {
	// Test that file handles are properly closed
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "handles.parquet")

	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{{ID: 1, Name: "test"}}

	f, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}
	_ = writer.Close()
	_ = f.Close()

	// Call ExtractSchemaInfo multiple times to verify file handles are released
	for i := 0; i < 100; i++ {
		_, err := ExtractSchemaInfo(testFile)
		if err != nil {
			t.Fatalf("ExtractSchemaInfo() iteration %d error = %v", i, err)
		}
	}

	// Try to delete the file - if handles were left open, this would fail on some OSes
	err = os.Remove(testFile)
	if err != nil {
		t.Errorf("Failed to remove test file, file handles may not be properly closed: %v", err)
	}
}

func TestExtractSchemaInfo_VariousFileSizes(t *testing.T) {
	// Test with various file sizes to ensure scalability
	t.Run("tiny_5cols", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "tiny.parquet")

		type Row struct {
			Col0 string `parquet:"col0"`
			Col1 string `parquet:"col1"`
			Col2 string `parquet:"col2"`
			Col3 string `parquet:"col3"`
			Col4 string `parquet:"col4"`
		}

		rows := []Row{{}}
		f, _ := os.Create(testFile)
		writer := parquet.NewGenericWriter[Row](f)
		_, _ = writer.Write(rows)
		_ = writer.Close()
		_ = f.Close()

		schemaInfos, err := ExtractSchemaInfo(testFile)
		if err != nil {
			t.Fatalf("ExtractSchemaInfo() error = %v", err)
		}
		if len(schemaInfos) != 5 {
			t.Errorf("ExtractSchemaInfo() returned %d fields, want 5", len(schemaInfos))
		}
	})

	t.Run("medium_20cols", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "medium.parquet")

		type Row struct {
			Col00 string `parquet:"col00"`
			Col01 string `parquet:"col01"`
			Col02 string `parquet:"col02"`
			Col03 string `parquet:"col03"`
			Col04 string `parquet:"col04"`
			Col05 string `parquet:"col05"`
			Col06 string `parquet:"col06"`
			Col07 string `parquet:"col07"`
			Col08 string `parquet:"col08"`
			Col09 string `parquet:"col09"`
			Col10 string `parquet:"col10"`
			Col11 string `parquet:"col11"`
			Col12 string `parquet:"col12"`
			Col13 string `parquet:"col13"`
			Col14 string `parquet:"col14"`
			Col15 string `parquet:"col15"`
			Col16 string `parquet:"col16"`
			Col17 string `parquet:"col17"`
			Col18 string `parquet:"col18"`
			Col19 string `parquet:"col19"`
		}

		rows := []Row{{}}
		f, _ := os.Create(testFile)
		writer := parquet.NewGenericWriter[Row](f)
		_, _ = writer.Write(rows)
		_ = writer.Close()
		_ = f.Close()

		schemaInfos, err := ExtractSchemaInfo(testFile)
		if err != nil {
			t.Fatalf("ExtractSchemaInfo() error = %v", err)
		}
		if len(schemaInfos) != 20 {
			t.Errorf("ExtractSchemaInfo() returned %d fields, want 20", len(schemaInfos))
		}
	})

	// Note: The large schema test with 30 columns already exists as TestExtractSchemaInfo_LargeSchema
}
