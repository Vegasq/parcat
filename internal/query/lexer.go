package query

import (
	"strings"
	"unicode"
)

// Lexer tokenizes SQL query strings
type Lexer struct {
	input string
	pos   int
	ch    rune
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character
func (l *Lexer) readChar() {
	if l.pos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = rune(l.input[l.pos])
	}
	l.pos++
}

// peekChar looks at the next character without advancing
func (l *Lexer) peekChar() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return rune(l.input[l.pos])
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readString reads a quoted string
func (l *Lexer) readString(quote rune) string {
	var result strings.Builder
	l.readChar() // skip opening quote

	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar()
			switch l.ch {
			case 'n':
				result.WriteRune('\n')
			case 't':
				result.WriteRune('\t')
			case '\\':
				result.WriteRune('\\')
			case quote:
				result.WriteRune(quote)
			default:
				result.WriteRune(l.ch)
			}
		} else {
			result.WriteRune(l.ch)
		}
		l.readChar()
	}

	if l.ch == quote {
		l.readChar() // skip closing quote
	}

	return result.String()
}

// readNumber reads a number
func (l *Lexer) readNumber() string {
	var result strings.Builder
	for unicode.IsDigit(l.ch) || l.ch == '.' || l.ch == '-' {
		result.WriteRune(l.ch)
		l.readChar()
	}
	return result.String()
}

// readIdentifier reads an identifier or keyword (including file paths)
func (l *Lexer) readIdentifier() string {
	var result strings.Builder
	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' || l.ch == '.' || l.ch == '/' || l.ch == '-' {
		result.WriteRune(l.ch)
		l.readChar()
	}
	return result.String()
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	var tok Token

	switch l.ch {
	case 0:
		tok = Token{Type: TokenEOF, Value: ""}
	case '=':
		tok = Token{Type: TokenEqual, Value: "="}
		l.readChar()
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenNotEqual, Value: "!="}
			l.readChar()
		} else {
			tok = Token{Type: TokenError, Value: "!"}
			l.readChar()
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenLessEqual, Value: "<="}
			l.readChar()
		} else {
			tok = Token{Type: TokenLess, Value: "<"}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenGreaterEqual, Value: ">="}
			l.readChar()
		} else {
			tok = Token{Type: TokenGreater, Value: ">"}
			l.readChar()
		}
	case '\'', '"':
		quote := l.ch
		tok = Token{Type: TokenString, Value: l.readString(quote)}
	case '*':
		tok = Token{Type: TokenIdent, Value: "*"}
		l.readChar()
	default:
		if unicode.IsDigit(l.ch) || l.ch == '-' {
			value := l.readNumber()
			tok = Token{Type: TokenNumber, Value: value}
		} else if unicode.IsLetter(l.ch) || l.ch == '_' {
			value := l.readIdentifier()
			tok = Token{Type: identifierType(value), Value: value}
		} else {
			tok = Token{Type: TokenError, Value: string(l.ch)}
			l.readChar()
		}
	}

	return tok
}

// identifierType determines if an identifier is a keyword
func identifierType(ident string) TokenType {
	keywords := map[string]TokenType{
		"select": TokenSelect,
		"SELECT": TokenSelect,
		"from":   TokenFrom,
		"FROM":   TokenFrom,
		"where":  TokenWhere,
		"WHERE":  TokenWhere,
		"and":    TokenAnd,
		"AND":    TokenAnd,
		"or":     TokenOr,
		"OR":     TokenOr,
		"true":   TokenBool,
		"TRUE":   TokenBool,
		"false":  TokenBool,
		"FALSE":  TokenBool,
	}

	if tokType, ok := keywords[ident]; ok {
		return tokType
	}
	return TokenIdent
}

// Tokenize returns all tokens from the input
func Tokenize(input string) []Token {
	lexer := NewLexer(input)
	var tokens []Token

	for {
		tok := lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF || tok.Type == TokenError {
			break
		}
	}

	return tokens
}
