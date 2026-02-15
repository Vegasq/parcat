package query

import (
	"fmt"
	"strconv"
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
