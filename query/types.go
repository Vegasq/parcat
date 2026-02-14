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

import "fmt"

// TokenType represents the type of a token
type TokenType int

const (
	// Keywords
	TokenSelect TokenType = iota
	TokenFrom
	TokenWhere
	TokenAnd
	TokenOr
	TokenAs
	TokenGroup
	TokenBy
	TokenHaving
	TokenOrder
	TokenAsc
	TokenDesc
	TokenLimit
	TokenOffset
	TokenIn
	TokenLike
	TokenBetween
	TokenIs
	TokenNot
	TokenNull
	TokenDistinct
	TokenCase
	TokenWhen
	TokenThen
	TokenElse
	TokenEnd
	TokenOver
	TokenPartition
	TokenRows
	TokenRange
	TokenWith
	TokenRecursive
	TokenExists
	TokenJoin
	TokenInner
	TokenLeft
	TokenRight
	TokenFull
	TokenOuter
	TokenCross
	TokenOn

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

	// Delimiters
	TokenComma      // ,
	TokenLeftParen  // (
	TokenRightParen // )

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
	CTEs       []CTE  // WITH clause CTEs
	TableName  string // Single file path or glob pattern
	Subquery   *Query // Subquery in FROM clause (alternative to TableName)
	TableAlias string // Optional alias for table/subquery
	Joins      []Join // JOIN clauses
	SelectList []SelectItem
	Filter     Expression
	GroupBy    []string      // Column names to group by
	Having     Expression    // Post-aggregation filter
	OrderBy    []OrderByItem // Sort specification
	Limit      *int64        // Row limit
	Offset     *int64        // Row offset
	Distinct   bool          // DISTINCT modifier
}

// JoinType represents the type of join operation
type JoinType int

const (
	JoinInner JoinType = iota // INNER JOIN (default)
	JoinLeft                  // LEFT JOIN / LEFT OUTER JOIN
	JoinRight                 // RIGHT JOIN / RIGHT OUTER JOIN
	JoinFull                  // FULL JOIN / FULL OUTER JOIN
	JoinCross                 // CROSS JOIN
)

// Join represents a JOIN clause
type Join struct {
	Type      JoinType   // Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
	TableName string     // Table/file to join
	Subquery  *Query     // Subquery to join (alternative to TableName)
	Alias     string     // Optional alias for joined table/subquery
	Condition Expression // ON clause condition (nil for CROSS JOIN)
}

// CTE represents a Common Table Expression (WITH clause)
type CTE struct {
	Name  string // CTE name
	Query *Query // Subquery defining the CTE
}

// OrderByItem represents a column to sort by
type OrderByItem struct {
	Column string // Column name or alias
	Desc   bool   // DESC vs ASC (default)
}

// SelectItem represents a column or expression in the SELECT list
type SelectItem struct {
	Expr  SelectExpression // Column, function, or expression
	Alias string           // Optional alias (AS name)
}

// SelectExpression is an expression that can appear in a SELECT list
type SelectExpression interface {
	EvaluateSelect(row map[string]interface{}) (interface{}, error)
}

// ColumnRef references a column (or * for all columns)
type ColumnRef struct {
	Column string // Column name or "*"
}

// FunctionCall represents a function invocation
type FunctionCall struct {
	Name string
	Args []SelectExpression
}

// LiteralExpr represents a literal value (number, string, bool)
type LiteralExpr struct {
	Value interface{}
}

// AggregateExpr represents an aggregate function (COUNT, SUM, AVG, MIN, MAX)
type AggregateExpr struct {
	Function string           // COUNT, SUM, AVG, MIN, MAX
	Arg      SelectExpression // Argument expression (nil for COUNT(*))
	Distinct bool             // DISTINCT modifier (not implemented yet)
}

// CaseExpr represents a CASE expression
type CaseExpr struct {
	WhenClauses []WhenClause     // WHEN conditions and their results
	ElseExpr    SelectExpression // ELSE result (optional)
}

// WhenClause represents a single WHEN condition and result
type WhenClause struct {
	Condition Expression       // WHEN condition
	Result    SelectExpression // THEN result
}

// WindowExpr represents a window function call
type WindowExpr struct {
	Function string             // Window function name (ROW_NUMBER, RANK, etc.)
	Args     []SelectExpression // Function arguments
	Window   *WindowSpec        // Window specification
}

// WindowSpec specifies the window behavior
type WindowSpec struct {
	PartitionBy []string      // PARTITION BY column names
	OrderBy     []OrderByItem // ORDER BY specification
	Frame       *WindowFrame  // Frame specification (ROWS/RANGE)
}

// WindowFrame specifies the window frame
type WindowFrame struct {
	Type  FrameType  // ROWS or RANGE
	Start FrameBound // Frame start
	End   FrameBound // Frame end
}

// FrameType represents the type of window frame
type FrameType int

const (
	FrameTypeRows FrameType = iota
	FrameTypeRange
)

// FrameBound represents a frame boundary
type FrameBound struct {
	Type   BoundType // UNBOUNDED, CURRENT, OFFSET
	Offset int64     // Offset for OFFSET bound type
}

// BoundType represents the type of frame bound
type BoundType int

const (
	BoundUnboundedPreceding BoundType = iota
	BoundOffsetPreceding
	BoundCurrentRow
	BoundOffsetFollowing
	BoundUnboundedFollowing
)

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

// ComparisonExpr represents a comparison expression (column op literal)
type ComparisonExpr struct {
	Column   string
	Operator TokenType
	Value    interface{}
}

// ColumnComparisonExpr represents a column-to-column comparison (col1 op col2)
type ColumnComparisonExpr struct {
	LeftColumn  string
	Operator    TokenType
	RightColumn string
}

// InExpr represents an IN expression (col IN (val1, val2, ...))
type InExpr struct {
	Column string
	Values []interface{}
	Negate bool // NOT IN
}

// LikeExpr represents a LIKE expression (col LIKE 'pattern')
type LikeExpr struct {
	Column  string
	Pattern string
	Negate  bool // NOT LIKE
}

// BetweenExpr represents a BETWEEN expression (col BETWEEN lower AND upper)
type BetweenExpr struct {
	Column string
	Lower  interface{}
	Upper  interface{}
	Negate bool // NOT BETWEEN
}

// IsNullExpr represents an IS NULL expression (col IS NULL / col IS NOT NULL)
type IsNullExpr struct {
	Column string
	Negate bool // IS NOT NULL
}

// SubqueryExpr represents a subquery in WHERE clause (for IN, EXISTS, or scalar)
type SubqueryExpr struct {
	Query *Query
	Type  SubqueryType // IN, EXISTS, SCALAR
}

// ExistsExpr represents an EXISTS expression
type ExistsExpr struct {
	Subquery *Query
	Negate   bool // NOT EXISTS
}

// InSubqueryExpr represents an IN expression with a subquery
type InSubqueryExpr struct {
	Column   string
	Subquery *Query
	Negate   bool // NOT IN
}

// SubqueryType represents the type of subquery
type SubqueryType int

const (
	SubqueryScalar SubqueryType = iota // Returns single value
	SubqueryIn                         // Used in IN clause
	SubqueryExists                     // Used in EXISTS clause
)

// ScalarSubqueryExpr represents a scalar subquery in SELECT or WHERE
type ScalarSubqueryExpr struct {
	Query *Query
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
		return false, fmt.Errorf("unsupported binary operator: %v", b.Operator)
	}
}

// Evaluate evaluates a comparison expression
func (c *ComparisonExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[c.Column]
	if !exists {
		return false, fmt.Errorf("column %q not found", c.Column)
	}

	return compare(value, c.Operator, c.Value)
}

// Evaluate evaluates a column-to-column comparison expression
func (c *ColumnComparisonExpr) Evaluate(row map[string]interface{}) (bool, error) {
	leftValue, leftExists := row[c.LeftColumn]
	rightValue, rightExists := row[c.RightColumn]

	// If either column doesn't exist, comparison fails
	if !leftExists {
		return false, fmt.Errorf("column %q not found", c.LeftColumn)
	}
	if !rightExists {
		return false, fmt.Errorf("column %q not found", c.RightColumn)
	}

	return compare(leftValue, c.Operator, rightValue)
}

// Evaluate evaluates an IN expression
func (i *InExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[i.Column]
	if !exists {
		return false, fmt.Errorf("column %q not found", i.Column)
	}

	// Check if value is in the list
	found := false
	for _, listValue := range i.Values {
		match, err := compare(value, TokenEqual, listValue)
		if err != nil {
			return false, err
		}
		if match {
			found = true
			break
		}
	}

	// Apply negation if needed
	if i.Negate {
		return !found, nil
	}
	return found, nil
}

// Evaluate evaluates a LIKE expression
func (l *LikeExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[l.Column]
	if !exists {
		return false, fmt.Errorf("column %q not found", l.Column)
	}

	// Convert value to string
	str, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("LIKE requires string column, got %T", value)
	}

	// Match the LIKE pattern
	match := matchLikePattern(str, l.Pattern)

	// Apply negation if needed
	if l.Negate {
		return !match, nil
	}
	return match, nil
}

// Evaluate evaluates a BETWEEN expression
func (b *BetweenExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[b.Column]
	if !exists {
		return false, fmt.Errorf("column %q not found", b.Column)
	}

	// Check if value >= lower
	lowerMatch, err := compare(value, TokenGreaterEqual, b.Lower)
	if err != nil {
		return false, err
	}

	// Check if value <= upper
	upperMatch, err := compare(value, TokenLessEqual, b.Upper)
	if err != nil {
		return false, err
	}

	// Value is between if it satisfies both conditions
	between := lowerMatch && upperMatch

	// Apply negation if needed
	if b.Negate {
		return !between, nil
	}
	return between, nil
}

// Evaluate evaluates an IS NULL expression
func (i *IsNullExpr) Evaluate(row map[string]interface{}) (bool, error) {
	value, exists := row[i.Column]

	// Check if the column exists and is nil
	isNull := !exists || value == nil

	// Apply negation if needed (IS NOT NULL)
	if i.Negate {
		return !isNull, nil
	}
	return isNull, nil
}

// EvaluateSelect evaluates a column reference
func (c *ColumnRef) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	// Special case: * means all columns
	if c.Column == "*" {
		return row, nil
	}

	value, exists := row[c.Column]
	if !exists {
		return nil, fmt.Errorf("column %q not found", c.Column)
	}
	return value, nil
}

// EvaluateSelect evaluates a function call
func (f *FunctionCall) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	// Look up the function in the registry
	registry := GetGlobalRegistry()
	fn, exists := registry.Get(f.Name)
	if !exists {
		return nil, fmt.Errorf("unknown function: %s", f.Name)
	}

	// Evaluate all arguments
	args := make([]interface{}, len(f.Args))
	for i, arg := range f.Args {
		val, err := arg.EvaluateSelect(row)
		if err != nil {
			return nil, fmt.Errorf("function %s: argument %d: %w", f.Name, i+1, err)
		}
		args[i] = val
	}

	// Check arity
	minArity := fn.MinArity()
	maxArity := fn.MaxArity()
	argCount := len(args)

	if minArity >= 0 && argCount < minArity {
		return nil, fmt.Errorf("function %s: expected at least %d arguments, got %d", f.Name, minArity, argCount)
	}
	if maxArity >= 0 && argCount > maxArity {
		return nil, fmt.Errorf("function %s: expected at most %d arguments, got %d", f.Name, maxArity, argCount)
	}

	// Call the function
	return fn.Evaluate(args)
}

// EvaluateSelect evaluates a literal expression
func (l *LiteralExpr) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	return l.Value, nil
}

// EvaluateSelect for AggregateExpr is handled separately in the aggregation logic
// This method should not be called directly on raw rows
func (a *AggregateExpr) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("aggregate function %s cannot be evaluated on individual rows", a.Function)
}

// EvaluateSelect evaluates a CASE expression
func (c *CaseExpr) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	// Evaluate each WHEN clause in order
	for _, whenClause := range c.WhenClauses {
		// Evaluate the condition
		conditionResult, err := whenClause.Condition.Evaluate(row)
		if err != nil {
			return nil, fmt.Errorf("CASE: evaluating WHEN condition: %w", err)
		}

		// If condition is true, return the result
		if conditionResult {
			result, err := whenClause.Result.EvaluateSelect(row)
			if err != nil {
				return nil, fmt.Errorf("CASE: evaluating THEN result: %w", err)
			}
			return result, nil
		}
	}

	// If no WHEN clause matched, evaluate ELSE (or return nil if no ELSE)
	if c.ElseExpr != nil {
		result, err := c.ElseExpr.EvaluateSelect(row)
		if err != nil {
			return nil, fmt.Errorf("CASE: evaluating ELSE result: %w", err)
		}
		return result, nil
	}

	return nil, nil
}

// EvaluateSelect for WindowExpr is handled separately in the window execution logic
// This method should not be called directly on raw rows
func (w *WindowExpr) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("window function %s cannot be evaluated on individual rows", w.Function)
}

// Evaluate evaluates an EXISTS expression
// Note: This requires access to subquery execution context, which is handled in the executor
func (e *ExistsExpr) Evaluate(row map[string]interface{}) (bool, error) {
	return false, fmt.Errorf("EXISTS subquery evaluation requires executor context")
}

// Evaluate evaluates an IN subquery expression
// Note: This requires access to subquery execution context, which is handled in the executor
func (i *InSubqueryExpr) Evaluate(row map[string]interface{}) (bool, error) {
	return false, fmt.Errorf("IN subquery evaluation requires executor context")
}

// EvaluateSelect evaluates a scalar subquery
// Note: This requires access to subquery execution context, which is handled in the executor
func (s *ScalarSubqueryExpr) EvaluateSelect(row map[string]interface{}) (interface{}, error) {
	return nil, fmt.Errorf("scalar subquery evaluation requires executor context")
}
