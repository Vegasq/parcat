package query

import (
	"fmt"
	"strconv"
	"strings"
)

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
