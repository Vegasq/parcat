package query

import (
	"fmt"
	"strconv"
	"strings"
)

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
