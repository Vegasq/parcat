package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vegasq/parcat/internal/output"
	"github.com/vegasq/parcat/internal/query"
	"github.com/vegasq/parcat/internal/reader"
)

var (
	queryFlag  = flag.String("q", "", "SQL query (e.g., \"select * from file.parquet where age > 30\")")
	formatFlag = flag.String("f", "jsonl", "Output format: json, jsonl, csv")
	limitFlag  = flag.Int("limit", 0, "Limit number of rows (0 = unlimited)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <file.parquet>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "A tool to read and query Parquet files.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f csv data.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -q \"select * from data.parquet where age > 30\" data.parquet\n", os.Args[0])
	}

	flag.Parse()

	// Get filename from positional args
	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Error: missing parquet file argument\n\n")
		flag.Usage()
		os.Exit(1)
	}
	filename := flag.Arg(0)

	// Open parquet file
	r, err := reader.NewReader(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: file '%s' not found\n", filename)
			fmt.Fprintf(os.Stderr, "Please check the file path and try again.\n")
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}
	defer r.Close()

	// Read all rows
	rows, err := r.ReadAll()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading parquet file: %v\n", err)
		os.Exit(1)
	}

	// Parse and apply query filter if specified
	if *queryFlag != "" {
		q, err := query.Parse(*queryFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing query: %v\n\n", err)
			fmt.Fprintf(os.Stderr, "Query format: select * from file.parquet where <condition>\n")
			fmt.Fprintf(os.Stderr, "Example: select * from data.parquet where age > 30\n")
			os.Exit(1)
		}

		if q.Filter != nil {
			rows, err = query.ApplyFilter(rows, q.Filter)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error applying filter: %v\n", err)
				// List available columns to help user
				if len(rows) > 0 {
					columns := query.GetColumnNames(rows)
					fmt.Fprintf(os.Stderr, "\nAvailable columns: ")
					for i, col := range columns {
						if i > 0 {
							fmt.Fprintf(os.Stderr, ", ")
						}
						fmt.Fprintf(os.Stderr, "%s", col)
					}
					fmt.Fprintf(os.Stderr, "\n")
				}
				os.Exit(1)
			}
		}
	}

	// Apply limit if specified
	if *limitFlag > 0 && len(rows) > *limitFlag {
		rows = rows[:*limitFlag]
	}

	// Format and output
	var formatter output.Formatter
	switch *formatFlag {
	case "json", "jsonl":
		formatter = output.NewJSONFormatter(os.Stdout)
	case "csv":
		formatter = output.NewCSVFormatter(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported format '%s'\n", *formatFlag)
		fmt.Fprintf(os.Stderr, "Supported formats: json, jsonl, csv\n")
		os.Exit(1)
	}

	if err := formatter.Format(rows); err != nil {
		fmt.Fprintf(os.Stderr, "Error formatting output: %v\n", err)
		os.Exit(1)
	}
}
