# Suggested Commands

## Build
```bash
go build -o parcat .
```

## Test
```bash
go test ./...
go test -v ./internal/query/
```

## Run
```bash
./parcat data.parquet
./parcat -q "SELECT * FROM data.parquet WHERE age > 30"
```

## macOS Commands (Darwin)
Standard Unix commands available: git, ls, cd, grep, find, etc.
