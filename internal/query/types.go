// Package query provides SQL query parsing and filtering for parquet data.
//
// It implements a SQL-like query language with support for WHERE clauses,
// comparison operators, and boolean logic (AND/OR). The package includes
// a lexer for tokenization, a parser for building ASTs, and an evaluator
// for filtering data rows.
//
// Example usage:
//
//	query, err := Parse("select * from data.parquet where age > 30")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	filtered, err := ApplyFilter(rows, query.Filter)
package query

// TokenType represents the type of a token
type TokenType int

const (
	// Keywords
	TokenSelect TokenType = iota
	TokenFrom
	TokenWhere
	TokenAnd
	TokenOr

	// Operators
	TokenEqual        // =
	TokenNotEqual     // !=
	TokenLess         // <
	TokenGreater      // >
	TokenLessEqual    // <=
	TokenGreaterEqual // >=

	// Literals
	TokenString
	TokenNumber
	TokenIdent
	TokenBool

	// Special
	TokenEOF
	TokenError
)

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
}

// Query represents a parsed SQL query
type Query struct {
	TableName string
	Filter    Expression
}

// Expression represents a boolean expression in the WHERE clause
type Expression interface {
	Evaluate(row map[string]interface{}) (bool, error)
}

// BinaryExpr represents a binary expression (AND/OR)
type BinaryExpr struct {
	Left     Expression
	Operator TokenType // TokenAnd or TokenOr
	Right    Expression
}

// ComparisonExpr represents a comparison expression
type ComparisonExpr struct {
	Column   string
	Operator TokenType
	Value    interface{}
}

// Evaluate evaluates a binary expression
func (b *BinaryExpr) Evaluate(row map[string]interface{}) (bool, error) {
	left, err := b.Left.Evaluate(row)
	if err != nil {
		return false, err
	}

	right, err := b.Right.Evaluate(row)
	if err != nil {
		return false, err
	}

	switch b.Operator {
	case TokenAnd:
		return left && right, nil
	case TokenOr:
		return left || right, nil
	default:
		return false, nil
	}
}

// Evaluate evaluates a comparison expression
func (c *ComparisonExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[c.Column]
	if !exists {
		return false, nil
	}

	return compare(value, c.Operator, c.Value)
}
