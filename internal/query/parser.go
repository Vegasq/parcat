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
	return parser.parseQuery()
}

// parseQuery parses: SELECT * FROM table WHERE expr
func (p *Parser) parseQuery() (*Query, error) {
	// Parse SELECT
	if err := p.expect(TokenSelect); err != nil {
		return nil, fmt.Errorf("query must start with SELECT: %w", err)
	}

	// Parse * (we only support SELECT *)
	if p.current().Type != TokenIdent || p.current().Value != "*" {
		return nil, fmt.Errorf("only SELECT * is supported")
	}
	p.advance()

	// Parse FROM
	if err := p.expect(TokenFrom); err != nil {
		return nil, fmt.Errorf("expected FROM after SELECT *: %w", err)
	}

	// Parse table name (file path)
	tableName := p.current().Value
	if p.current().Type != TokenIdent && p.current().Type != TokenString {
		return nil, fmt.Errorf("expected table name after FROM")
	}
	p.advance()

	// Validate table name
	if err := ValidateTableName(tableName); err != nil {
		return nil, err
	}

	q := &Query{
		TableName: tableName,
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

	return q, nil
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

// parseComparison parses comparison expressions
func (p *Parser) parseComparison() (Expression, error) {
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

	// Parse operator
	operator := p.current().Type
	switch operator {
	case TokenEqual, TokenNotEqual, TokenLess, TokenGreater, TokenLessEqual, TokenGreaterEqual:
		p.advance()
	default:
		return nil, fmt.Errorf("expected comparison operator, got %v", operator)
	}

	// Parse value
	var value interface{}
	switch p.current().Type {
	case TokenString:
		value = p.current().Value
		p.advance()
	case TokenNumber:
		numStr := p.current().Value
		// Try to parse as int first, then float
		if intVal, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			value = intVal
		} else if floatVal, err := strconv.ParseFloat(numStr, 64); err == nil {
			value = floatVal
		} else {
			return nil, fmt.Errorf("invalid number: %s", numStr)
		}
		p.advance()
	case TokenBool:
		value = strings.ToLower(p.current().Value) == "true"
		p.advance()
	default:
		return nil, fmt.Errorf("expected value (string, number, or bool), got %v", p.current().Type)
	}

	return &ComparisonExpr{
		Column:   column,
		Operator: operator,
		Value:    value,
	}, nil
}
