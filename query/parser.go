package query

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses SQL queries into AST
type Parser struct {
	tokens       []Token
	pos          int
	depthCounter *ExpressionDepthCounter
}

// NewParser creates a new parser
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens:       tokens,
		pos:          0,
		depthCounter: NewExpressionDepthCounter(),
	}
}

// current returns the current token
func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF, Value: ""}
	}
	return p.tokens[p.pos]
}

// peek returns the next token without advancing
func (p *Parser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF, Value: ""}
	}
	return p.tokens[p.pos+1]
}

// advance moves to the next token
func (p *Parser) advance() {
	p.pos++
}

// expect checks if current token matches expected type and advances
func (p *Parser) expect(tokType TokenType) error {
	if p.current().Type != tokType {
		return fmt.Errorf("expected %v, got %v", tokType, p.current().Type)
	}
	p.advance()
	return nil
}

// Parse parses a SQL query
func Parse(query string) (*Query, error) {
	// Validate query length
	if err := ValidateQuery(query); err != nil {
		return nil, err
	}

	tokens := Tokenize(query)

	// Validate token count
	if err := ValidateTokens(tokens); err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	q, err := parser.parseQuery()
	if err != nil {
		return nil, err
	}

	// Validate that we consumed all tokens (should be at EOF)
	if parser.current().Type == TokenError {
		return nil, fmt.Errorf("invalid character in query: %s", parser.current().Value)
	}
	if parser.current().Type != TokenEOF {
		return nil, fmt.Errorf("unexpected trailing tokens after query: %s", parser.current().Value)
	}

	return q, nil
}

// parseQuery parses: [WITH cte AS (...)] SELECT col1, col2, ... FROM table WHERE expr
func (p *Parser) parseQuery() (*Query, error) {
	var ctes []CTE

	// Parse WITH clause (optional)
	if p.current().Type == TokenWith {
		var err error
		ctes, err = p.parseWithClause()
		if err != nil {
			return nil, err
		}
	}

	// Parse SELECT
	if err := p.expect(TokenSelect); err != nil {
		return nil, fmt.Errorf("query must start with SELECT (or WITH): %w", err)
	}

	// Check for DISTINCT
	distinct := false
	if p.current().Type == TokenDistinct {
		distinct = true
		p.advance()
	}

	// Parse SELECT list
	selectList, err := p.parseSelectList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse SELECT list: %w", err)
	}

	// Parse FROM
	if err := p.expect(TokenFrom); err != nil {
		return nil, fmt.Errorf("expected FROM after SELECT list: %w", err)
	}

	// Initialize query
	q := &Query{
		CTEs:       ctes,
		SelectList: selectList,
		Distinct:   distinct,
	}

	// Parse FROM source (table name, subquery, or CTE reference)
	if p.current().Type == TokenLeftParen {
		// Subquery in FROM clause
		p.advance() // consume (
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, fmt.Errorf("failed to parse subquery in FROM: %w", err)
		}
		if err := p.expect(TokenRightParen); err != nil {
			return nil, fmt.Errorf("expected ) after subquery: %w", err)
		}
		q.Subquery = subquery

		// Parse optional alias for subquery
		if p.current().Type == TokenAs {
			p.advance()
		}
		if p.current().Type == TokenIdent {
			q.TableAlias = p.current().Value
			p.advance()
		}
	} else {
		// Table name or CTE reference (may include glob patterns like 'data/*.parquet')
		tableName := p.current().Value
		if p.current().Type != TokenIdent && p.current().Type != TokenString {
			return nil, fmt.Errorf("expected table name or subquery after FROM")
		}
		p.advance()

		// Validate table name (unless it's a CTE reference or glob pattern)
		isCTE := false
		for _, cte := range ctes {
			if cte.Name == tableName {
				isCTE = true
				break
			}
		}
		if !isCTE {
			// Allow glob patterns (*, ?) without strict validation
			if err := ValidateTableName(tableName); err != nil {
				return nil, err
			}
		}

		q.TableName = tableName

		// Parse optional alias for table
		if p.current().Type == TokenAs {
			p.advance()
		}
		if p.current().Type == TokenIdent {
			q.TableAlias = p.current().Value
			p.advance()
		}
	}

	// Parse JOIN clauses (optional, can be multiple)
	for p.current().Type == TokenJoin || p.current().Type == TokenInner ||
		p.current().Type == TokenLeft || p.current().Type == TokenRight ||
		p.current().Type == TokenFull || p.current().Type == TokenCross {

		join, err := p.parseJoin(ctes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JOIN: %w", err)
		}
		q.Joins = append(q.Joins, *join)
	}

	// Parse WHERE clause (optional)
	if p.current().Type == TokenWhere {
		p.advance()
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		q.Filter = expr
	}

	// Parse GROUP BY clause (optional)
	if p.current().Type == TokenGroup {
		groupBy, err := p.parseGroupBy()
		if err != nil {
			return nil, err
		}
		q.GroupBy = groupBy
	}

	// Parse HAVING clause (optional, only valid with GROUP BY)
	if p.current().Type == TokenHaving {
		if len(q.GroupBy) == 0 {
			return nil, fmt.Errorf("HAVING clause requires GROUP BY")
		}
		p.advance()
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		q.Having = expr
	}

	// Parse ORDER BY clause (optional)
	if p.current().Type == TokenOrder {
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		q.OrderBy = orderBy
	}

	// Parse LIMIT clause (optional)
	if p.current().Type == TokenLimit {
		limit, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		q.Limit = limit
	}

	// Parse OFFSET clause (optional)
	if p.current().Type == TokenOffset {
		offset, err := p.parseOffset()
		if err != nil {
			return nil, err
		}
		q.Offset = offset
	}

	return q, nil
}

// parseJoin parses a JOIN clause
func (p *Parser) parseJoin(ctes []CTE) (*Join, error) {
	join := &Join{}

	// Determine join type
	switch p.current().Type {
	case TokenCross:
		join.Type = JoinCross
		p.advance()
		if err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenInner:
		join.Type = JoinInner
		p.advance()
		if err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenLeft:
		join.Type = JoinLeft
		p.advance()
		// Optional OUTER keyword
		if p.current().Type == TokenOuter {
			p.advance()
		}
		if err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenRight:
		join.Type = JoinRight
		p.advance()
		// Optional OUTER keyword
		if p.current().Type == TokenOuter {
			p.advance()
		}
		if err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenFull:
		join.Type = JoinFull
		p.advance()
		// Optional OUTER keyword
		if p.current().Type == TokenOuter {
			p.advance()
		}
		if err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenJoin:
		// Plain JOIN defaults to INNER JOIN
		join.Type = JoinInner
		p.advance()
	default:
		return nil, fmt.Errorf("expected JOIN keyword")
	}

	// Parse joined table or subquery
	if p.current().Type == TokenLeftParen {
		// Subquery
		p.advance() // consume (
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, fmt.Errorf("failed to parse subquery in JOIN: %w", err)
		}
		if err := p.expect(TokenRightParen); err != nil {
			return nil, fmt.Errorf("expected ) after subquery: %w", err)
		}
		join.Subquery = subquery

		// Parse optional alias for subquery
		if p.current().Type == TokenAs {
			p.advance()
		}
		if p.current().Type == TokenIdent {
			join.Alias = p.current().Value
			p.advance()
		}
	} else {
		// Table name or CTE reference
		tableName := p.current().Value
		if p.current().Type != TokenIdent && p.current().Type != TokenString {
			return nil, fmt.Errorf("expected table name or subquery after JOIN")
		}
		p.advance()

		// Validate table name (unless it's a CTE reference)
		isCTE := false
		for _, cte := range ctes {
			if cte.Name == tableName {
				isCTE = true
				break
			}
		}
		if !isCTE {
			if err := ValidateTableName(tableName); err != nil {
				return nil, err
			}
		}

		join.TableName = tableName

		// Parse optional alias for table
		if p.current().Type == TokenAs {
			p.advance()
		}
		if p.current().Type == TokenIdent {
			join.Alias = p.current().Value
			p.advance()
		}
	}

	// Parse ON clause (required for all join types except CROSS JOIN)
	if join.Type != JoinCross {
		if err := p.expect(TokenOn); err != nil {
			return nil, fmt.Errorf("expected ON clause after JOIN table: %w", err)
		}
		condition, err := p.parseOr()
		if err != nil {
			return nil, fmt.Errorf("failed to parse JOIN condition: %w", err)
		}
		join.Condition = condition
	}

	return join, nil
}

// parseOr parses OR expressions (lowest precedence)
func (p *Parser) parseOr() (Expression, error) {
	if err := p.depthCounter.Enter(); err != nil {
		return nil, err
	}
	defer p.depthCounter.Exit()

	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:     left,
			Operator: TokenOr,
			Right:    right,
		}
	}

	return left, nil
}

// parseAnd parses AND expressions (higher precedence than OR)
func (p *Parser) parseAnd() (Expression, error) {
	if err := p.depthCounter.Enter(); err != nil {
		return nil, err
	}
	defer p.depthCounter.Exit()

	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenAnd {
		p.advance()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:     left,
			Operator: TokenAnd,
			Right:    right,
		}
	}

	return left, nil
}

// parseSelectList parses the SELECT list (columns, expressions, aliases)
func (p *Parser) parseSelectList() ([]SelectItem, error) {
	var items []SelectItem

	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)

		// Check for comma (more items)
		if p.current().Type == TokenComma {
			p.advance()
			continue
		}

		// No comma, we're done with the SELECT list
		break
	}

	return items, nil
}

// parseSelectItem parses a single SELECT item (column, function, or expression with optional alias)
func (p *Parser) parseSelectItem() (SelectItem, error) {
	var item SelectItem

	// Parse the expression (column or function call)
	expr, err := p.parseSelectExpression()
	if err != nil {
		return item, err
	}
	item.Expr = expr

	// Check for AS alias
	if p.current().Type == TokenAs {
		p.advance()
		if p.current().Type != TokenIdent {
			return item, fmt.Errorf("expected alias name after AS")
		}
		item.Alias = p.current().Value
		p.advance()
	} else if p.current().Type == TokenIdent && p.current().Value != "*" {
		// Check for implicit alias (column name without AS)
		// But only if it's not a keyword or operator
		if !isKeyword(p.current().Value) {
			item.Alias = p.current().Value
			p.advance()
		}
	}

	return item, nil
}

// parseSelectExpression parses a select expression (column reference, function call, literal, CASE, or subquery)
func (p *Parser) parseSelectExpression() (SelectExpression, error) {
	// Check for CASE expression
	if p.current().Type == TokenCase {
		return p.parseCaseExpression()
	}

	// Check for scalar subquery (starts with opening paren)
	if p.current().Type == TokenLeftParen {
		// Look ahead to see if it's a subquery (SELECT or WITH)
		nextPos := p.pos + 1
		if nextPos < len(p.tokens) && (p.tokens[nextPos].Type == TokenSelect || p.tokens[nextPos].Type == TokenWith) {
			return p.parseScalarSubquery()
		}
	}

	// Check for aggregate or regular function call (identifier followed by left paren)
	if p.current().Type == TokenIdent && p.peek().Type == TokenLeftParen {
		// Check if it's an aggregate function
		funcName := strings.ToUpper(p.current().Value)
		if isAggregateFunction(funcName) {
			return p.parseAggregateFunction()
		}
		// Check if it's a window function
		if isWindowFunction(funcName) {
			return p.parseWindowFunction()
		}
		return p.parseFunctionCall()
	}

	// Check for literals (numbers, strings, bools)
	switch p.current().Type {
	case TokenNumber:
		numStr := p.current().Value
		p.advance()
		// Try to parse as int first, then float
		if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			return &LiteralExpr{Value: intVal}, nil
		} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
			return &LiteralExpr{Value: floatVal}, nil
		} else {
			return nil, fmt.Errorf("invalid number: %s", numStr)
		}
	case TokenString:
		str := p.current().Value
		p.advance()
		return &LiteralExpr{Value: str}, nil
	case TokenBool:
		b := strings.ToLower(p.current().Value) == "true"
		p.advance()
		return &LiteralExpr{Value: b}, nil
	}

	// Otherwise, it's a column reference
	if p.current().Type != TokenIdent {
		return nil, fmt.Errorf("expected column name, literal, or function call, got %v", p.current().Type)
	}

	column := p.current().Value
	p.advance()

	return &ColumnRef{Column: column}, nil
}

// parseFunctionCall parses a function call
func (p *Parser) parseFunctionCall() (SelectExpression, error) {
	funcName := p.current().Value
	p.advance() // skip function name

	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after function name: %w", err)
	}

	var args []SelectExpression

	// Check for empty argument list
	if p.current().Type == TokenRightParen {
		p.advance()
		return &FunctionCall{Name: funcName, Args: args}, nil
	}

	// Parse arguments
	for {
		arg, err := p.parseSelectExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		if p.current().Type == TokenComma {
			p.advance()
			continue
		}

		break
	}

	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after function arguments: %w", err)
	}

	return &FunctionCall{Name: funcName, Args: args}, nil
}

// parseWindowFunction parses a window function call
func (p *Parser) parseWindowFunction() (SelectExpression, error) {
	funcName := p.current().Value
	p.advance() // skip function name

	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after window function name: %w", err)
	}

	var args []SelectExpression

	// Check for empty argument list
	if p.current().Type != TokenRightParen {
		// Parse arguments
		for {
			arg, err := p.parseSelectExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)

			if p.current().Type == TokenComma {
				p.advance()
				continue
			}

			break
		}
	}

	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after window function arguments: %w", err)
	}

	// Window functions must have an OVER clause
	if p.current().Type != TokenOver {
		return nil, fmt.Errorf("window function %s requires OVER clause", funcName)
	}
	p.advance() // skip OVER

	// Parse window specification
	windowSpec, err := p.parseWindowSpec()
	if err != nil {
		return nil, fmt.Errorf("failed to parse window specification: %w", err)
	}

	return &WindowExpr{
		Function: strings.ToUpper(funcName),
		Args:     args,
		Window:   windowSpec,
	}, nil
}

// parseWindowSpec parses a window specification (PARTITION BY, ORDER BY, frame)
func (p *Parser) parseWindowSpec() (*WindowSpec, error) {
	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after OVER: %w", err)
	}

	spec := &WindowSpec{}

	// Parse PARTITION BY (optional)
	if p.current().Type == TokenPartition {
		p.advance()
		if err := p.expect(TokenBy); err != nil {
			return nil, fmt.Errorf("expected BY after PARTITION: %w", err)
		}

		// Parse partition columns
		for {
			if p.current().Type != TokenIdent {
				return nil, fmt.Errorf("expected column name in PARTITION BY")
			}
			spec.PartitionBy = append(spec.PartitionBy, p.current().Value)
			p.advance()

			if p.current().Type == TokenComma {
				p.advance()
				continue
			}
			break
		}
	}

	// Parse ORDER BY (optional)
	if p.current().Type == TokenOrder {
		p.advance()
		if err := p.expect(TokenBy); err != nil {
			return nil, fmt.Errorf("expected BY after ORDER: %w", err)
		}

		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, fmt.Errorf("failed to parse ORDER BY in window: %w", err)
		}
		spec.OrderBy = orderBy
	}

	// Parse frame specification (optional)
	if p.current().Type == TokenRows || p.current().Type == TokenRange {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, fmt.Errorf("failed to parse window frame: %w", err)
		}
		spec.Frame = frame
	}

	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after window specification: %w", err)
	}

	return spec, nil
}

// parseWindowFrame parses a window frame specification (ROWS/RANGE ...)
func (p *Parser) parseWindowFrame() (*WindowFrame, error) {
	frame := &WindowFrame{}

	// Parse frame type (ROWS or RANGE)
	if p.current().Type == TokenRows {
		frame.Type = FrameTypeRows
	} else if p.current().Type == TokenRange {
		frame.Type = FrameTypeRange
	} else {
		return nil, fmt.Errorf("expected ROWS or RANGE")
	}
	p.advance()

	// For simplicity, we'll support:
	// - UNBOUNDED PRECEDING
	// - CURRENT ROW
	// - n PRECEDING
	// - n FOLLOWING
	// - UNBOUNDED FOLLOWING
	// - BETWEEN <bound> AND <bound>

	// Check for BETWEEN syntax
	if p.current().Type == TokenBetween {
		p.advance()

		// Parse start bound
		startBound, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("failed to parse frame start bound: %w", err)
		}
		frame.Start = startBound

		// Expect AND
		if err := p.expect(TokenAnd); err != nil {
			return nil, fmt.Errorf("expected AND in BETWEEN frame clause: %w", err)
		}

		// Parse end bound
		endBound, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("failed to parse frame end bound: %w", err)
		}
		frame.End = endBound
	} else {
		// Single bound syntax (implies BETWEEN bound AND CURRENT ROW)
		bound, err := p.parseFrameBound()
		if err != nil {
			return nil, fmt.Errorf("failed to parse frame bound: %w", err)
		}
		frame.Start = bound
		frame.End = FrameBound{Type: BoundCurrentRow}
	}

	return frame, nil
}

// parseFrameBound parses a single frame bound
func (p *Parser) parseFrameBound() (FrameBound, error) {
	var bound FrameBound

	// Check for UNBOUNDED
	if strings.ToUpper(p.current().Value) == "UNBOUNDED" {
		p.advance()

		if strings.ToUpper(p.current().Value) == "PRECEDING" {
			bound.Type = BoundUnboundedPreceding
			p.advance()
		} else if strings.ToUpper(p.current().Value) == "FOLLOWING" {
			bound.Type = BoundUnboundedFollowing
			p.advance()
		} else {
			return bound, fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED")
		}

		return bound, nil
	}

	// Check for CURRENT ROW
	if strings.ToUpper(p.current().Value) == "CURRENT" {
		p.advance()
		if strings.ToUpper(p.current().Value) != "ROW" {
			return bound, fmt.Errorf("expected ROW after CURRENT")
		}
		p.advance()
		bound.Type = BoundCurrentRow
		return bound, nil
	}

	// Check for n PRECEDING / n FOLLOWING
	if p.current().Type == TokenNumber {
		offset, err := strconv.ParseInt(p.current().Value, 10, 64)
		if err != nil {
			return bound, fmt.Errorf("invalid offset in frame bound: %w", err)
		}
		bound.Offset = offset
		p.advance()

		if strings.ToUpper(p.current().Value) == "PRECEDING" {
			bound.Type = BoundOffsetPreceding
			p.advance()
		} else if strings.ToUpper(p.current().Value) == "FOLLOWING" {
			bound.Type = BoundOffsetFollowing
			p.advance()
		} else {
			return bound, fmt.Errorf("expected PRECEDING or FOLLOWING after offset")
		}

		return bound, nil
	}

	return bound, fmt.Errorf("invalid frame bound")
}

// isKeyword checks if a string is a SQL keyword
func isKeyword(s string) bool {
	keywords := map[string]bool{
		"select": true, "SELECT": true,
		"from": true, "FROM": true,
		"where": true, "WHERE": true,
		"and": true, "AND": true,
		"or": true, "OR": true,
		"as": true, "AS": true,
		"group": true, "GROUP": true,
		"by": true, "BY": true,
		"having": true, "HAVING": true,
		"order": true, "ORDER": true,
		"asc": true, "ASC": true,
		"desc": true, "DESC": true,
		"limit": true, "LIMIT": true,
		"offset": true, "OFFSET": true,
		"in": true, "IN": true,
		"like": true, "LIKE": true,
		"between": true, "BETWEEN": true,
		"is": true, "IS": true,
		"not": true, "NOT": true,
		"null": true, "NULL": true,
		"distinct": true, "DISTINCT": true,
		"over": true, "OVER": true,
		"partition": true, "PARTITION": true,
		"rows": true, "ROWS": true,
		"range": true, "RANGE": true,
	}
	return keywords[s]
}

// isAggregateFunction checks if a function name is an aggregate function
func isAggregateFunction(name string) bool {
	aggregates := map[string]bool{
		"COUNT": true,
		"SUM":   true,
		"AVG":   true,
		"MIN":   true,
		"MAX":   true,
	}
	return aggregates[strings.ToUpper(name)]
}

// isWindowFunction checks if a function name is a window function
func isWindowFunction(name string) bool {
	windowFuncs := map[string]bool{
		"ROW_NUMBER":  true,
		"RANK":        true,
		"DENSE_RANK":  true,
		"NTILE":       true,
		"FIRST_VALUE": true,
		"LAST_VALUE":  true,
		"NTH_VALUE":   true,
		"LAG":         true,
		"LEAD":        true,
	}
	return windowFuncs[strings.ToUpper(name)]
}

// parseAggregateFunction parses an aggregate function call
func (p *Parser) parseAggregateFunction() (SelectExpression, error) {
	funcName := strings.ToUpper(p.current().Value)
	p.advance() // skip function name

	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after aggregate function: %w", err)
	}

	var arg SelectExpression

	// Check for COUNT(*)
	if funcName == "COUNT" && p.current().Type == TokenIdent && p.current().Value == "*" {
		p.advance()
		arg = nil // COUNT(*) has no argument
	} else {
		// Parse the argument expression
		argExpr, err := p.parseSelectExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse aggregate function argument: %w", err)
		}
		arg = argExpr
	}

	// Check if MIN/MAX has multiple arguments (scalar function form)
	if (funcName == "MIN" || funcName == "MAX") && p.current().Type == TokenComma {
		// Parse as scalar function with multiple arguments
		args := []SelectExpression{arg}
		for p.current().Type == TokenComma {
			p.advance() // skip comma
			nextArg, err := p.parseSelectExpression()
			if err != nil {
				return nil, fmt.Errorf("failed to parse function argument: %w", err)
			}
			args = append(args, nextArg)
		}

		if err := p.expect(TokenRightParen); err != nil {
			return nil, fmt.Errorf("expected ')' after function arguments: %w", err)
		}

		return &FunctionCall{Name: funcName, Args: args}, nil
	}

	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after aggregate function argument: %w", err)
	}

	return &AggregateExpr{
		Function: funcName,
		Arg:      arg,
		Distinct: false,
	}, nil
}

// parseCaseExpression parses a CASE expression
func (p *Parser) parseCaseExpression() (SelectExpression, error) {
	// Expect CASE
	if err := p.expect(TokenCase); err != nil {
		return nil, err
	}

	var whenClauses []WhenClause

	// Parse WHEN clauses
	for p.current().Type == TokenWhen {
		p.advance() // skip WHEN

		// Parse the condition (a WHERE-like expression with AND/OR support)
		condition, err := p.parseOr()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CASE WHEN condition: %w", err)
		}

		// Expect THEN
		if err := p.expect(TokenThen); err != nil {
			return nil, fmt.Errorf("expected THEN after WHEN condition: %w", err)
		}

		// Parse the result expression
		result, err := p.parseSelectExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CASE THEN result: %w", err)
		}

		whenClauses = append(whenClauses, WhenClause{
			Condition: condition,
			Result:    result,
		})
	}

	// Check for at least one WHEN clause
	if len(whenClauses) == 0 {
		return nil, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	// Parse optional ELSE clause
	var elseExpr SelectExpression
	if p.current().Type == TokenElse {
		p.advance() // skip ELSE

		var err error
		elseExpr, err = p.parseSelectExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CASE ELSE result: %w", err)
		}
	}

	// Expect END
	if err := p.expect(TokenEnd); err != nil {
		return nil, fmt.Errorf("expected END after CASE expression: %w", err)
	}

	return &CaseExpr{
		WhenClauses: whenClauses,
		ElseExpr:    elseExpr,
	}, nil
}

// parseGroupBy parses the GROUP BY clause
func (p *Parser) parseGroupBy() ([]string, error) {
	// Expect GROUP
	if err := p.expect(TokenGroup); err != nil {
		return nil, err
	}

	// Expect BY
	if err := p.expect(TokenBy); err != nil {
		return nil, fmt.Errorf("expected BY after GROUP: %w", err)
	}

	var columns []string

	// Parse column list
	for {
		if p.current().Type != TokenIdent {
			return nil, fmt.Errorf("expected column name in GROUP BY, got %v", p.current().Type)
		}

		column := p.current().Value
		if err := ValidateColumnName(column); err != nil {
			return nil, err
		}

		columns = append(columns, column)
		p.advance()

		// Check for comma (more columns)
		if p.current().Type == TokenComma {
			p.advance()
			continue
		}

		// No comma, we're done
		break
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("GROUP BY requires at least one column")
	}

	return columns, nil
}

// parseComparison parses comparison expressions (including IN, LIKE, BETWEEN, IS NULL)
func (p *Parser) parseComparison() (Expression, error) {
	// Check for EXISTS (doesn't start with column)
	if p.current().Type == TokenExists || (p.current().Type == TokenNot && p.peek().Type == TokenExists) {
		return p.parseExistsExpr()
	}

	// Check for scalar subquery (comparison with subquery)
	// This could be a subquery, but it's not common syntax, so we'll skip for now
	// Most scalar subqueries appear on the right side of comparison

	// Parse column name
	if p.current().Type != TokenIdent {
		return nil, fmt.Errorf("expected column name, got %v", p.current().Type)
	}
	column := p.current().Value

	// Validate column name length
	if err := ValidateColumnName(column); err != nil {
		return nil, err
	}

	p.advance()

	// Check for special operators first
	switch p.current().Type {
	case TokenIn:
		return p.parseInExpr(column)
	case TokenNot:
		// Could be "NOT IN", "NOT LIKE", "NOT BETWEEN"
		p.advance()
		switch p.current().Type {
		case TokenIn:
			expr, err := p.parseInExpr(column)
			if err != nil {
				return nil, err
			}
			// Handle both InExpr and InSubqueryExpr
			switch e := expr.(type) {
			case *InExpr:
				e.Negate = true
			case *InSubqueryExpr:
				e.Negate = true
			}
			return expr, nil
		case TokenLike:
			expr, err := p.parseLikeExpr(column)
			if err != nil {
				return nil, err
			}
			if likeExpr, ok := expr.(*LikeExpr); ok {
				likeExpr.Negate = true
			}
			return expr, nil
		case TokenBetween:
			expr, err := p.parseBetweenExpr(column)
			if err != nil {
				return nil, err
			}
			if betweenExpr, ok := expr.(*BetweenExpr); ok {
				betweenExpr.Negate = true
			}
			return expr, nil
		default:
			return nil, fmt.Errorf("expected IN, LIKE, or BETWEEN after NOT, got %v", p.current().Type)
		}
	case TokenLike:
		return p.parseLikeExpr(column)
	case TokenBetween:
		return p.parseBetweenExpr(column)
	case TokenIs:
		return p.parseIsNullExpr(column)
	}

	// Parse standard comparison operator
	operator := p.current().Type
	switch operator {
	case TokenEqual, TokenNotEqual, TokenLess, TokenGreater, TokenLessEqual, TokenGreaterEqual:
		p.advance()
	default:
		return nil, fmt.Errorf("expected comparison operator, got %v", operator)
	}

	// Parse right side - could be a literal value or column reference
	switch p.current().Type {
	case TokenString:
		value := p.current().Value
		p.advance()
		return &ComparisonExpr{
			Column:   column,
			Operator: operator,
			Value:    value,
		}, nil
	case TokenNumber:
		numStr := p.current().Value
		// Try to parse as int first, then float
		var value interface{}
		if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			value = intVal
		} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
			value = floatVal
		} else {
			return nil, fmt.Errorf("invalid number: %s", numStr)
		}
		p.advance()
		return &ComparisonExpr{
			Column:   column,
			Operator: operator,
			Value:    value,
		}, nil
	case TokenBool:
		value := strings.ToLower(p.current().Value) == "true"
		p.advance()
		return &ComparisonExpr{
			Column:   column,
			Operator: operator,
			Value:    value,
		}, nil
	case TokenIdent:
		// Column-to-column comparison (for JOINs)
		rightColumn := p.current().Value
		p.advance()
		return &ColumnComparisonExpr{
			LeftColumn:  column,
			Operator:    operator,
			RightColumn: rightColumn,
		}, nil
	default:
		return nil, fmt.Errorf("expected value (string, number, bool) or column name, got %v", p.current().Type)
	}
}

// parseInExpr parses an IN expression: column IN (val1, val2, ...) or column IN (subquery)
func (p *Parser) parseInExpr(column string) (Expression, error) {
	// Expect IN keyword
	if err := p.expect(TokenIn); err != nil {
		return nil, err
	}

	// Expect opening parenthesis
	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after IN: %w", err)
	}

	// Check if it's a subquery (starts with SELECT or WITH)
	if p.current().Type == TokenSelect || p.current().Type == TokenWith {
		// Parse subquery
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, fmt.Errorf("failed to parse IN subquery: %w", err)
		}

		// Validate subquery selects exactly one column
		if len(subquery.SelectList) == 0 {
			return nil, fmt.Errorf("IN subquery must select at least one column")
		}
		// Check for SELECT * which would select multiple columns
		if len(subquery.SelectList) == 1 {
			if colRef, ok := subquery.SelectList[0].Expr.(*ColumnRef); ok && colRef.Column == "*" {
				return nil, fmt.Errorf("IN subquery cannot use SELECT *, must select exactly one column")
			}
		} else if len(subquery.SelectList) > 1 {
			return nil, fmt.Errorf("IN subquery must select exactly one column, got %d columns", len(subquery.SelectList))
		}

		// Expect closing parenthesis
		if err := p.expect(TokenRightParen); err != nil {
			return nil, fmt.Errorf("expected ')' after IN subquery: %w", err)
		}

		return &InSubqueryExpr{
			Column:   column,
			Subquery: subquery,
			Negate:   false,
		}, nil
	}

	// Parse value list
	var values []interface{}
	for {
		var value interface{}
		switch p.current().Type {
		case TokenString:
			value = p.current().Value
			p.advance()
		case TokenNumber:
			numStr := p.current().Value
			if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				value = intVal
			} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
				value = floatVal
			} else {
				return nil, fmt.Errorf("invalid number in IN list: %s", numStr)
			}
			p.advance()
		case TokenBool:
			value = strings.ToLower(p.current().Value) == "true"
			p.advance()
		default:
			return nil, fmt.Errorf("expected value in IN list, got %v", p.current().Type)
		}
		values = append(values, value)

		// Check for comma (more values) or closing parenthesis
		if p.current().Type == TokenComma {
			p.advance()
			continue
		}
		if p.current().Type == TokenRightParen {
			break
		}
		return nil, fmt.Errorf("expected ',' or ')' in IN list, got %v", p.current().Type)
	}

	// Expect closing parenthesis
	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after IN list: %w", err)
	}

	return &InExpr{
		Column: column,
		Values: values,
		Negate: false,
	}, nil
}

// parseLikeExpr parses a LIKE expression: column LIKE 'pattern'
func (p *Parser) parseLikeExpr(column string) (Expression, error) {
	// Expect LIKE keyword
	if err := p.expect(TokenLike); err != nil {
		return nil, err
	}

	// Expect string pattern
	if p.current().Type != TokenString {
		return nil, fmt.Errorf("expected string pattern after LIKE, got %v", p.current().Type)
	}
	pattern := p.current().Value
	p.advance()

	return &LikeExpr{
		Column:  column,
		Pattern: pattern,
		Negate:  false,
	}, nil
}

// parseBetweenExpr parses a BETWEEN expression: column BETWEEN lower AND upper
func (p *Parser) parseBetweenExpr(column string) (Expression, error) {
	// Expect BETWEEN keyword
	if err := p.expect(TokenBetween); err != nil {
		return nil, err
	}

	// Parse lower bound
	var lower interface{}
	switch p.current().Type {
	case TokenString:
		lower = p.current().Value
		p.advance()
	case TokenNumber:
		numStr := p.current().Value
		if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			lower = intVal
		} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
			lower = floatVal
		} else {
			return nil, fmt.Errorf("invalid lower bound: %s", numStr)
		}
		p.advance()
	default:
		return nil, fmt.Errorf("expected value for BETWEEN lower bound, got %v", p.current().Type)
	}

	// Expect AND
	if err := p.expect(TokenAnd); err != nil {
		return nil, fmt.Errorf("expected AND in BETWEEN expression: %w", err)
	}

	// Parse upper bound
	var upper interface{}
	switch p.current().Type {
	case TokenString:
		upper = p.current().Value
		p.advance()
	case TokenNumber:
		numStr := p.current().Value
		if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			upper = intVal
		} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
			upper = floatVal
		} else {
			return nil, fmt.Errorf("invalid upper bound: %s", numStr)
		}
		p.advance()
	default:
		return nil, fmt.Errorf("expected value for BETWEEN upper bound, got %v", p.current().Type)
	}

	return &BetweenExpr{
		Column: column,
		Lower:  lower,
		Upper:  upper,
		Negate: false,
	}, nil
}

// parseIsNullExpr parses an IS NULL expression: column IS [NOT] NULL
func (p *Parser) parseIsNullExpr(column string) (Expression, error) {
	// Expect IS keyword
	if err := p.expect(TokenIs); err != nil {
		return nil, err
	}

	// Check for NOT
	negate := false
	if p.current().Type == TokenNot {
		negate = true
		p.advance()
	}

	// Expect NULL
	if err := p.expect(TokenNull); err != nil {
		return nil, fmt.Errorf("expected NULL after IS [NOT]: %w", err)
	}

	return &IsNullExpr{
		Column: column,
		Negate: negate,
	}, nil
}

// parseOrderBy parses the ORDER BY clause
func (p *Parser) parseOrderBy() ([]OrderByItem, error) {
	// Expect ORDER
	if err := p.expect(TokenOrder); err != nil {
		return nil, err
	}

	// Expect BY
	if err := p.expect(TokenBy); err != nil {
		return nil, fmt.Errorf("expected BY after ORDER: %w", err)
	}

	return p.parseOrderByList()
}

// parseOrderByList parses the ORDER BY column list (without ORDER BY keywords)
func (p *Parser) parseOrderByList() ([]OrderByItem, error) {
	var items []OrderByItem

	// Parse column list
	for {
		if p.current().Type != TokenIdent {
			return nil, fmt.Errorf("expected column name in ORDER BY, got %v", p.current().Type)
		}

		column := p.current().Value
		if err := ValidateColumnName(column); err != nil {
			return nil, err
		}

		item := OrderByItem{
			Column: column,
			Desc:   false, // Default to ASC
		}
		p.advance()

		// Check for ASC/DESC modifier
		if p.current().Type == TokenAsc {
			item.Desc = false
			p.advance()
		} else if p.current().Type == TokenDesc {
			item.Desc = true
			p.advance()
		}

		items = append(items, item)

		// Check for comma (more columns)
		if p.current().Type == TokenComma {
			p.advance()
			continue
		}

		// No comma, we're done
		break
	}

	if len(items) == 0 {
		return nil, fmt.Errorf("ORDER BY requires at least one column")
	}

	return items, nil
}

// parseLimit parses the LIMIT clause
func (p *Parser) parseLimit() (*int64, error) {
	// Expect LIMIT
	if err := p.expect(TokenLimit); err != nil {
		return nil, err
	}

	// Expect a number
	if p.current().Type != TokenNumber {
		return nil, fmt.Errorf("expected number after LIMIT, got %v", p.current().Type)
	}

	numStr := p.current().Value
	limit, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid LIMIT value: %s", numStr)
	}

	if limit < 0 {
		return nil, fmt.Errorf("LIMIT must be non-negative, got %d", limit)
	}

	p.advance()
	return &limit, nil
}

// parseOffset parses the OFFSET clause
func (p *Parser) parseOffset() (*int64, error) {
	// Expect OFFSET
	if err := p.expect(TokenOffset); err != nil {
		return nil, err
	}

	// Expect a number
	if p.current().Type != TokenNumber {
		return nil, fmt.Errorf("expected number after OFFSET, got %v", p.current().Type)
	}

	numStr := p.current().Value
	offset, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid OFFSET value: %s", numStr)
	}

	if offset < 0 {
		return nil, fmt.Errorf("OFFSET must be non-negative, got %d", offset)
	}

	p.advance()
	return &offset, nil
}

// parseScalarSubquery parses a scalar subquery in SELECT clause
func (p *Parser) parseScalarSubquery() (SelectExpression, error) {
	// Expect opening parenthesis
	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' for scalar subquery: %w", err)
	}

	// Parse subquery
	subquery, err := p.parseQuery()
	if err != nil {
		return nil, fmt.Errorf("failed to parse scalar subquery: %w", err)
	}

	// Validate subquery selects exactly one column
	if len(subquery.SelectList) == 0 {
		return nil, fmt.Errorf("scalar subquery must select at least one column")
	}
	// Check for SELECT * which would select multiple columns
	if len(subquery.SelectList) == 1 {
		if colRef, ok := subquery.SelectList[0].Expr.(*ColumnRef); ok && colRef.Column == "*" {
			return nil, fmt.Errorf("scalar subquery cannot use SELECT *, must select exactly one column")
		}
	} else if len(subquery.SelectList) > 1 {
		return nil, fmt.Errorf("scalar subquery must select exactly one column, got %d columns", len(subquery.SelectList))
	}

	// Expect closing parenthesis
	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after scalar subquery: %w", err)
	}

	return &ScalarSubqueryExpr{
		Query: subquery,
	}, nil
}

// parseExistsExpr parses an EXISTS expression: EXISTS (subquery) or NOT EXISTS (subquery)
func (p *Parser) parseExistsExpr() (Expression, error) {
	negate := false

	// Check for NOT EXISTS
	if p.current().Type == TokenNot {
		negate = true
		p.advance()
	}

	// Expect EXISTS keyword
	if err := p.expect(TokenExists); err != nil {
		return nil, err
	}

	// Expect opening parenthesis
	if err := p.expect(TokenLeftParen); err != nil {
		return nil, fmt.Errorf("expected '(' after EXISTS: %w", err)
	}

	// Parse subquery
	subquery, err := p.parseQuery()
	if err != nil {
		return nil, fmt.Errorf("failed to parse EXISTS subquery: %w", err)
	}

	// Expect closing parenthesis
	if err := p.expect(TokenRightParen); err != nil {
		return nil, fmt.Errorf("expected ')' after EXISTS subquery: %w", err)
	}

	return &ExistsExpr{
		Subquery: subquery,
		Negate:   negate,
	}, nil
}

// parseWithClause parses the WITH clause (Common Table Expressions)
// Syntax: WITH cte1 AS (query1), cte2 AS (query2)
func (p *Parser) parseWithClause() ([]CTE, error) {
	if err := p.expect(TokenWith); err != nil {
		return nil, err
	}

	// Check for RECURSIVE (not supported yet)
	if p.current().Type == TokenRecursive {
		return nil, fmt.Errorf("RECURSIVE CTEs are not supported yet")
	}

	var ctes []CTE

	for {
		// Parse CTE name
		if p.current().Type != TokenIdent {
			return nil, fmt.Errorf("expected CTE name, got %v", p.current().Type)
		}
		cteName := p.current().Value
		p.advance()

		// Expect AS
		if err := p.expect(TokenAs); err != nil {
			return nil, fmt.Errorf("expected AS after CTE name: %w", err)
		}

		// Expect (
		if err := p.expect(TokenLeftParen); err != nil {
			return nil, fmt.Errorf("expected ( after AS: %w", err)
		}

		// Parse the subquery
		subquery, err := p.parseQuery()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CTE subquery: %w", err)
		}

		// Expect )
		if err := p.expect(TokenRightParen); err != nil {
			return nil, fmt.Errorf("expected ) after CTE subquery: %w", err)
		}

		// Add CTE
		ctes = append(ctes, CTE{
			Name:  cteName,
			Query: subquery,
		})

		// Check for comma (more CTEs)
		if p.current().Type == TokenComma {
			p.advance()
			continue
		}

		// No comma, we're done with CTEs
		break
	}

	return ctes, nil
}
