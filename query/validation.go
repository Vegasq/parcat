package query

import (
	"errors"
	"fmt"
)

// Validation constants to prevent DoS and resource exhaustion
const (
	// MaxQueryLength is the maximum allowed query string length (1MB)
	MaxQueryLength = 1024 * 1024

	// MaxTokens is the maximum number of tokens in a query
	MaxTokens = 1000

	// MaxExpressionDepth is the maximum nesting depth for expressions
	MaxExpressionDepth = 100

	// MaxColumnNameLength is the maximum length for a column name
	MaxColumnNameLength = 256

	// MaxTableNameLength is the maximum length for a table name
	MaxTableNameLength = 4096 // Allow long file paths
)

var (
	// ErrQueryTooLong is returned when query exceeds MaxQueryLength
	ErrQueryTooLong = errors.New("query too long")

	// ErrTooManyTokens is returned when query has too many tokens
	ErrTooManyTokens = errors.New("too many tokens in query")

	// ErrExpressionTooDeep is returned when expression nesting exceeds limit
	ErrExpressionTooDeep = errors.New("expression nesting too deep")

	// ErrColumnNameTooLong is returned when column name is too long
	ErrColumnNameTooLong = errors.New("column name too long")

	// ErrTableNameTooLong is returned when table name is too long
	ErrTableNameTooLong = errors.New("table name too long")

	// ErrEmptyTableName is returned when table name is empty
	ErrEmptyTableName = errors.New("table name cannot be empty")
)

// ValidateQuery performs security validation on query input
func ValidateQuery(query string) error {
	if len(query) > MaxQueryLength {
		return fmt.Errorf("%w: %d bytes (max %d)", ErrQueryTooLong, len(query), MaxQueryLength)
	}
	return nil
}

// ValidateTableName validates table name length and content
func ValidateTableName(name string) error {
	if name == "" {
		return ErrEmptyTableName
	}
	if len(name) > MaxTableNameLength {
		return fmt.Errorf("%w: %d chars (max %d)", ErrTableNameTooLong, len(name), MaxTableNameLength)
	}
	return nil
}

// ValidateColumnName validates column name length
func ValidateColumnName(name string) error {
	if len(name) > MaxColumnNameLength {
		return fmt.Errorf("%w: %d chars (max %d)", ErrColumnNameTooLong, len(name), MaxColumnNameLength)
	}
	return nil
}

// ValidateTokens validates token count
func ValidateTokens(tokens []Token) error {
	if len(tokens) > MaxTokens {
		return fmt.Errorf("%w: %d tokens (max %d)", ErrTooManyTokens, len(tokens), MaxTokens)
	}
	return nil
}

// ExpressionDepthCounter tracks expression nesting depth
type ExpressionDepthCounter struct {
	depth    int
	maxDepth int
}

// NewExpressionDepthCounter creates a new depth counter
func NewExpressionDepthCounter() *ExpressionDepthCounter {
	return &ExpressionDepthCounter{depth: 0, maxDepth: MaxExpressionDepth}
}

// Enter increments depth and returns error if limit exceeded
func (c *ExpressionDepthCounter) Enter() error {
	c.depth++
	if c.depth > c.maxDepth {
		return fmt.Errorf("%w: %d (max %d)", ErrExpressionTooDeep, c.depth, c.maxDepth)
	}
	return nil
}

// Exit decrements depth
func (c *ExpressionDepthCounter) Exit() {
	c.depth--
}
