// Package reader provides functionality for reading Apache Parquet files.
//
// This package offers a simple, high-level API for reading parquet files
// and returning rows as maps for flexible data access. It supports both
// single-file and multi-file (glob pattern) operations.
//
// # Basic Usage
//
// Reading a single parquet file:
//
//	reader, err := reader.NewReader("data.parquet")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
//
//	rows, err := reader.ReadAll()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for _, row := range rows {
//	    fmt.Printf("%v\n", row)
//	}
//
// # Multi-file Operations
//
// Reading multiple files using glob patterns:
//
//	rows, err := reader.ReadMultipleFiles("data/*.parquet")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Each row includes a "_file" column with the source file path
//	for _, row := range rows {
//	    fmt.Printf("From %s: %v\n", row["_file"], row)
//	}
//
// # Schema Introspection
//
// Accessing parquet file schema:
//
//	reader, err := reader.NewReader("data.parquet")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
//
//	schema := reader.Schema()
//	for i := 0; i < schema.NumFields(); i++ {
//	    field := schema.Field(i)
//	    fmt.Printf("%s: %s\n", field.Name(), field.Type())
//	}
//
// # Resource Management
//
// Always call Close() when done reading to release file handles:
//
//	reader, err := reader.NewReader("data.parquet")
//	if err != nil {
//	    return err
//	}
//	defer reader.Close()
//
// The package uses github.com/segmentio/parquet-go for the underlying
// parquet file operations.
package reader
