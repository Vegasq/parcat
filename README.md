# parcat

A GNU-inspired CLI tool to read and query Apache Parquet files, similar to how `cat` works for text files.

## Features

- 📖 Read and output all rows from Parquet files
- 🔍 Simple SQL-like query support with WHERE clauses
- 📊 Multiple output formats (JSON Lines, CSV)
- ⚡ Pure Go implementation with zero external dependencies (except parquet library)
- 🎯 Fast and efficient

## Installation

### From Source

```bash
go install github.com/vegasq/parcat@latest
```

### Build Locally

```bash
git clone https://github.com/vegasq/parcat.git
cd parcat
go build -o parcat .
```

## Usage

### Basic Usage

Output all rows from a Parquet file (default: JSON Lines format):

```bash
parcat data.parquet
```

### Output Formats

**JSON Lines (default):**
```bash
parcat data.parquet
parcat -f jsonl data.parquet
```

**CSV:**
```bash
parcat -f csv data.parquet
```

### Query with WHERE Clause

Filter rows using SQL-like WHERE clauses:

```bash
# Simple comparison
parcat -q "select * from data.parquet where age > 30"

# String matching
parcat -q "select * from data.parquet where name = 'alice'"

# AND condition
parcat -q "select * from data.parquet where age > 25 AND status = 'active'"

# OR condition
parcat -q "select * from data.parquet where age > 50 OR premium = true"

# Combine with CSV output
parcat -q "select * from data.parquet where age > 30" -f csv
```

### Limit Output

Limit the number of rows returned:

```bash
parcat -limit 10 data.parquet
```

## Query Syntax

### Supported Operators

- `=` - Equal
- `!=` - Not equal
- `<` - Less than
- `>` - Greater than
- `<=` - Less than or equal
- `>=` - Greater than or equal

### Logical Operators

- `AND` - Both conditions must be true
- `OR` - At least one condition must be true

### Value Types

- **Strings**: Use single or double quotes (`'alice'` or `"alice"`)
- **Numbers**: Integers or floats (`30`, `3.14`, `-5`)
- **Booleans**: `true` or `false`

### Query Format

```sql
select * from <filename> where <condition>
```

**Examples:**

```sql
select * from users.parquet where age > 30
select * from users.parquet where name = 'alice' AND active = true
select * from users.parquet where score >= 90 OR premium = true
select * from orders.parquet where amount > 100 AND status != 'cancelled'
```

## Command Line Options

```
Usage: parcat [options] <file.parquet>

Options:
  -q string
        SQL query (e.g., "select * from file.parquet where age > 30")
  -f string
        Output format: json, jsonl, csv (default "jsonl")
  -limit int
        Limit number of rows (0 = unlimited)

Examples:
  parcat data.parquet
  parcat -f csv data.parquet
  parcat -q "select * from data.parquet where age > 30" data.parquet
```

## Type Handling

### Parquet to Output Type Mapping

- **INT32/INT64** → Integer
- **FLOAT/DOUBLE** → Float
- **BYTE_ARRAY** → String
- **BOOLEAN** → Boolean
- **Complex/Nested** → Preserved in JSON, flattened in CSV

### Comparison Type Coercion

- **String comparisons**: Case-sensitive
- **Numeric comparisons**: Automatic conversion to float64
- **Boolean comparisons**: Direct equality
- **Type mismatch**: Returns false with warning

## Examples

### Read entire file as JSON Lines
```bash
parcat users.parquet
```

**Output:**
```json
{"id":1,"name":"alice","age":30,"active":true}
{"id":2,"name":"bob","age":25,"active":false}
{"id":3,"name":"charlie","age":35,"active":true}
```

### Read as CSV
```bash
parcat -f csv users.parquet
```

**Output:**
```csv
active,age,id,name
true,30,1,alice
false,25,2,bob
true,35,3,charlie
```

### Filter by age
```bash
parcat -q "select * from users.parquet where age > 30" users.parquet
```

**Output:**
```json
{"id":3,"name":"charlie","age":35,"active":true}
```

### Complex query with AND
```bash
parcat -q "select * from users.parquet where age > 25 AND active = true" users.parquet
```

**Output:**
```json
{"id":1,"name":"alice","age":30,"active":true}
{"id":3,"name":"charlie","age":35,"active":true}
```

### Limit results
```bash
parcat -limit 2 users.parquet
```

## Error Handling

### File Not Found
```bash
$ parcat nonexistent.parquet
Error: failed to open file: open nonexistent.parquet: no such file or directory
```

### Invalid Parquet File
```bash
$ parcat invalid.txt
Error: failed to open parquet file: ...
```

### Query Syntax Error
```bash
$ parcat -q "invalid query" data.parquet
Error parsing query: query must start with SELECT: ...
```

### Column Not Found
When a column doesn't exist, the filter returns false for that row (no error, just filters it out).

## Architecture

```
parcat/
├── main.go                         # CLI entry point
├── internal/
│   ├── reader/parquet.go           # Parquet file reading
│   ├── query/
│   │   ├── lexer.go                # Query tokenization
│   │   ├── parser.go               # Query parsing
│   │   ├── filter.go               # Filter evaluation
│   │   └── types.go                # AST types
│   └── output/
│       ├── formatter.go            # Formatter interface
│       ├── json.go                 # JSON Lines output
│       └── csv.go                  # CSV output
└── testdata/                       # Test files
```

## Dependencies

- [github.com/segmentio/parquet-go](https://github.com/segmentio/parquet-go) - Pure Go Parquet library

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License

## Future Enhancements

Potential features for future versions:
- SELECT specific columns (column projection)
- Aggregate functions (COUNT, SUM, AVG)
- Schema inspection command
- Statistics command
- Pretty table output format
- Streaming for very large files
- Multiple file support (glob patterns)
- IN operator for queries
- LIKE operator for pattern matching

## Author

Created by vegasq

## See Also

- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [parquet-tools](https://github.com/apache/parquet-mr/tree/master/parquet-tools) - Official Java-based Parquet tools
