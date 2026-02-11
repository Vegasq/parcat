package query

import (
	"testing"
)

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "SELECT keyword",
			input: "SELECT",
			expected: []Token{
				{Type: TokenSelect, Value: "SELECT"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "case insensitive keywords",
			input: "select FROM where",
			expected: []Token{
				{Type: TokenSelect, Value: "select"},
				{Type: TokenFrom, Value: "FROM"},
				{Type: TokenWhere, Value: "where"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "AND OR keywords",
			input: "AND OR and or",
			expected: []Token{
				{Type: TokenAnd, Value: "AND"},
				{Type: TokenOr, Value: "OR"},
				{Type: TokenAnd, Value: "and"},
				{Type: TokenOr, Value: "or"},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := Tokenize(tt.input)
			if len(tokens) != len(tt.expected) {
				t.Fatalf("expected %d tokens, got %d", len(tt.expected), len(tokens))
			}
			for i, tok := range tokens {
				if tok.Type != tt.expected[i].Type {
					t.Errorf("token %d: expected type %v, got %v", i, tt.expected[i].Type, tok.Type)
				}
				if tok.Value != tt.expected[i].Value {
					t.Errorf("token %d: expected value %q, got %q", i, tt.expected[i].Value, tok.Value)
				}
			}
		})
	}
}

func TestLexer_Operators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "comparison operators",
			input: "= != < > <= >=",
			expected: []Token{
				{Type: TokenEqual, Value: "="},
				{Type: TokenNotEqual, Value: "!="},
				{Type: TokenLess, Value: "<"},
				{Type: TokenGreater, Value: ">"},
				{Type: TokenLessEqual, Value: "<="},
				{Type: TokenGreaterEqual, Value: ">="},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			name:  "operators with whitespace",
			input: "  =   !=  ",
			expected: []Token{
				{Type: TokenEqual, Value: "="},
				{Type: TokenNotEqual, Value: "!="},
				{Type: TokenEOF, Value: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := Tokenize(tt.input)
			if len(tokens) != len(tt.expected) {
				t.Fatalf("expected %d tokens, got %d", len(tt.expected), len(tokens))
			}
			for i, tok := range tokens {
				if tok.Type != tt.expected[i].Type {
					t.Errorf("token %d: expected type %v, got %v", i, tt.expected[i].Type, tok.Type)
				}
			}
		})
	}
}

func TestLexer_Strings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Token
	}{
		{
			name:     "single quoted string",
			input:    "'hello world'",
			expected: Token{Type: TokenString, Value: "hello world"},
		},
		{
			name:     "double quoted string",
			input:    `"hello world"`,
			expected: Token{Type: TokenString, Value: "hello world"},
		},
		{
			name:     "string with escape sequences",
			input:    `'hello\nworld\ttab'`,
			expected: Token{Type: TokenString, Value: "hello\nworld\ttab"},
		},
		{
			name:     "string with escaped quotes",
			input:    `'alice\'s data'`,
			expected: Token{Type: TokenString, Value: "alice's data"},
		},
		{
			name:     "empty string",
			input:    "''",
			expected: Token{Type: TokenString, Value: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.expected.Type {
				t.Errorf("expected type %v, got %v", tt.expected.Type, tok.Type)
			}
			if tok.Value != tt.expected.Value {
				t.Errorf("expected value %q, got %q", tt.expected.Value, tok.Value)
			}
		})
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Token
	}{
		{
			name:     "integer",
			input:    "42",
			expected: Token{Type: TokenNumber, Value: "42"},
		},
		{
			name:     "float",
			input:    "3.14",
			expected: Token{Type: TokenNumber, Value: "3.14"},
		},
		{
			name:     "negative number",
			input:    "-123",
			expected: Token{Type: TokenNumber, Value: "-123"},
		},
		{
			name:     "negative float",
			input:    "-3.14",
			expected: Token{Type: TokenNumber, Value: "-3.14"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.expected.Type {
				t.Errorf("expected type %v, got %v", tt.expected.Type, tok.Type)
			}
			if tok.Value != tt.expected.Value {
				t.Errorf("expected value %q, got %q", tt.expected.Value, tok.Value)
			}
		})
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Token
	}{
		{
			name:     "simple identifier",
			input:    "age",
			expected: Token{Type: TokenIdent, Value: "age"},
		},
		{
			name:     "identifier with underscore",
			input:    "user_id",
			expected: Token{Type: TokenIdent, Value: "user_id"},
		},
		{
			name:     "identifier with numbers",
			input:    "column123",
			expected: Token{Type: TokenIdent, Value: "column123"},
		},
		{
			name:     "file path",
			input:    "testdata/simple.parquet",
			expected: Token{Type: TokenIdent, Value: "testdata/simple.parquet"},
		},
		{
			name:     "asterisk",
			input:    "*",
			expected: Token{Type: TokenIdent, Value: "*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.expected.Type {
				t.Errorf("expected type %v, got %v", tt.expected.Type, tok.Type)
			}
			if tok.Value != tt.expected.Value {
				t.Errorf("expected value %q, got %q", tt.expected.Value, tok.Value)
			}
		})
	}
}

func TestLexer_Booleans(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Token
	}{
		{
			name:     "true lowercase",
			input:    "true",
			expected: Token{Type: TokenBool, Value: "true"},
		},
		{
			name:     "TRUE uppercase",
			input:    "TRUE",
			expected: Token{Type: TokenBool, Value: "TRUE"},
		},
		{
			name:     "false lowercase",
			input:    "false",
			expected: Token{Type: TokenBool, Value: "false"},
		},
		{
			name:     "FALSE uppercase",
			input:    "FALSE",
			expected: Token{Type: TokenBool, Value: "FALSE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.expected.Type {
				t.Errorf("expected type %v, got %v", tt.expected.Type, tok.Type)
			}
			if tok.Value != tt.expected.Value {
				t.Errorf("expected value %q, got %q", tt.expected.Value, tok.Value)
			}
		})
	}
}

func TestLexer_CompleteQuery(t *testing.T) {
	input := "select * from data.parquet where age > 30 AND name = 'alice'"

	expected := []TokenType{
		TokenSelect,
		TokenIdent, // *
		TokenFrom,
		TokenIdent, // data.parquet
		TokenWhere,
		TokenIdent, // age
		TokenGreater,
		TokenNumber, // 30
		TokenAnd,
		TokenIdent, // name
		TokenEqual,
		TokenString, // alice
		TokenEOF,
	}

	tokens := Tokenize(input)
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token %d: expected type %v, got %v (value: %q)", i, expected[i], tok.Type, tok.Value)
		}
	}
}
