package query

import (
	"math"
	"testing"
)

func TestUpperFunc(t *testing.T) {
	fn := &UpperFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"simple", []interface{}{"hello"}, "HELLO", false},
		{"mixed case", []interface{}{"HeLLo WoRLd"}, "HELLO WORLD", false},
		{"already upper", []interface{}{"HELLO"}, "HELLO", false},
		{"empty string", []interface{}{""}, "", false},
		{"with numbers", []interface{}{"hello123"}, "HELLO123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpperFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("UpperFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLowerFunc(t *testing.T) {
	fn := &LowerFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"simple", []interface{}{"HELLO"}, "hello", false},
		{"mixed case", []interface{}{"HeLLo WoRLd"}, "hello world", false},
		{"already lower", []interface{}{"hello"}, "hello", false},
		{"empty string", []interface{}{""}, "", false},
		{"with numbers", []interface{}{"HELLO123"}, "hello123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("LowerFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LowerFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConcatFunc(t *testing.T) {
	fn := &ConcatFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"two strings", []interface{}{"hello", "world"}, "helloworld", false},
		{"three strings", []interface{}{"hello", " ", "world"}, "hello world", false},
		{"single string", []interface{}{"hello"}, "hello", false},
		{"with numbers", []interface{}{"value:", int64(42)}, "value:42", false},
		{"empty strings", []interface{}{"", ""}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConcatFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConcatFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLengthFunc(t *testing.T) {
	fn := &LengthFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"simple", []interface{}{"hello"}, int64(5), false},
		{"empty string", []interface{}{""}, int64(0), false},
		{"with spaces", []interface{}{"hello world"}, int64(11), false},
		{"unicode", []interface{}{"hello🌍"}, int64(9), false}, // UTF-8 byte count
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("LengthFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LengthFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrimFunc(t *testing.T) {
	fn := &TrimFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"leading spaces", []interface{}{"  hello"}, "hello", false},
		{"trailing spaces", []interface{}{"hello  "}, "hello", false},
		{"both sides", []interface{}{"  hello  "}, "hello", false},
		{"no spaces", []interface{}{"hello"}, "hello", false},
		{"tabs and newlines", []interface{}{"\t\nhello\n\t"}, "hello", false},
		{"only spaces", []interface{}{"   "}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("TrimFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TrimFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAbsFunc(t *testing.T) {
	fn := &AbsFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"positive int", []interface{}{int64(42)}, 42.0, false},
		{"negative int", []interface{}{int64(-42)}, 42.0, false},
		{"positive float", []interface{}{3.14}, 3.14, false},
		{"negative float", []interface{}{-3.14}, 3.14, false},
		{"zero", []interface{}{int64(0)}, 0.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("AbsFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AbsFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoundFunc(t *testing.T) {
	fn := &RoundFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"default decimals", []interface{}{3.14159}, 3.0, false},
		{"two decimals", []interface{}{3.14159, int64(2)}, 3.14, false},
		{"zero decimals", []interface{}{3.7, int64(0)}, 4.0, false},
		{"negative decimals", []interface{}{1234.0, int64(-2)}, 1200.0, false},
		{"integer input", []interface{}{int64(42), int64(2)}, 42.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("RoundFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if math.Abs(got.(float64)-tt.want.(float64)) > 0.0001 {
				t.Errorf("RoundFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFloorFunc(t *testing.T) {
	fn := &FloorFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"positive", []interface{}{3.7}, 3.0, false},
		{"negative", []interface{}{-3.7}, -4.0, false},
		{"integer", []interface{}{int64(42)}, 42.0, false},
		{"zero", []interface{}{0.0}, 0.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("FloorFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FloorFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCeilFunc(t *testing.T) {
	fn := &CeilFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		{"positive", []interface{}{3.1}, 4.0, false},
		{"negative", []interface{}{-3.1}, -3.0, false},
		{"integer", []interface{}{int64(42)}, 42.0, false},
		{"zero", []interface{}{0.0}, 0.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("CeilFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CeilFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestModFunc(t *testing.T) {
	fn := &ModFunc{}

	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
		errMsg  string
	}{
		{"positive", []interface{}{int64(10), int64(3)}, 1.0, false, ""},
		{"negative dividend", []interface{}{int64(-10), int64(3)}, -1.0, false, ""},
		{"floats", []interface{}{7.5, 2.0}, 1.5, false, ""},
		{"division by zero", []interface{}{int64(10), int64(0)}, nil, true, "division by zero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ModFunc.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.errMsg != "" {
				if err.Error() != "MOD: "+tt.errMsg {
					t.Errorf("ModFunc.Evaluate() error = %v, want error containing %q", err, tt.errMsg)
				}
				return
			}
			if !tt.wantErr && math.Abs(got.(float64)-tt.want.(float64)) > 0.0001 {
				t.Errorf("ModFunc.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFunctionRegistry(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register a test function
	registry.Register(&UpperFunc{})

	t.Run("get existing function", func(t *testing.T) {
		fn, exists := registry.Get("UPPER")
		if !exists {
			t.Error("expected UPPER function to exist")
		}
		if fn.Name() != "UPPER" {
			t.Errorf("expected function name UPPER, got %s", fn.Name())
		}
	})

	t.Run("get case insensitive", func(t *testing.T) {
		fn, exists := registry.Get("upper")
		if !exists {
			t.Error("expected upper function to exist (case insensitive)")
		}
		if fn.Name() != "UPPER" {
			t.Errorf("expected function name UPPER, got %s", fn.Name())
		}
	})

	t.Run("get non-existing function", func(t *testing.T) {
		_, exists := registry.Get("NONEXISTENT")
		if exists {
			t.Error("expected NONEXISTENT function to not exist")
		}
	})
}

func TestFunctionCallEvaluateSelect(t *testing.T) {
	tests := []struct {
		name     string
		funcCall *FunctionCall
		row      map[string]interface{}
		want     interface{}
		wantErr  bool
	}{
		{
			name: "UPPER with column",
			funcCall: &FunctionCall{
				Name: "UPPER",
				Args: []SelectExpression{&ColumnRef{Column: "name"}},
			},
			row:     map[string]interface{}{"name": "alice"},
			want:    "ALICE",
			wantErr: false,
		},
		{
			name: "CONCAT two columns",
			funcCall: &FunctionCall{
				Name: "CONCAT",
				Args: []SelectExpression{
					&ColumnRef{Column: "first"},
					&ColumnRef{Column: "last"},
				},
			},
			row:     map[string]interface{}{"first": "Alice", "last": "Smith"},
			want:    "AliceSmith",
			wantErr: false,
		},
		{
			name: "ABS of number",
			funcCall: &FunctionCall{
				Name: "ABS",
				Args: []SelectExpression{&ColumnRef{Column: "value"}},
			},
			row:     map[string]interface{}{"value": int64(-42)},
			want:    42.0,
			wantErr: false,
		},
		{
			name: "unknown function",
			funcCall: &FunctionCall{
				Name: "UNKNOWN",
				Args: []SelectExpression{},
			},
			row:     map[string]interface{}{},
			wantErr: true,
		},
		{
			name: "wrong number of arguments",
			funcCall: &FunctionCall{
				Name: "UPPER",
				Args: []SelectExpression{
					&ColumnRef{Column: "a"},
					&ColumnRef{Column: "b"},
				},
			},
			row:     map[string]interface{}{"a": "test", "b": "test"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.funcCall.EvaluateSelect(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("FunctionCall.EvaluateSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("FunctionCall.EvaluateSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGlobalRegistry(t *testing.T) {
	registry := GetGlobalRegistry()

	// Check that all expected functions are registered
	expectedFunctions := []string{
		// String functions (15)
		"UPPER", "LOWER", "CONCAT", "LENGTH", "TRIM",
		"LTRIM", "RTRIM", "SUBSTRING", "REPLACE", "SPLIT",
		"REVERSE", "CONTAINS", "STARTS_WITH", "ENDS_WITH", "REPEAT",
		// Math functions (12)
		"ABS", "ROUND", "FLOOR", "CEIL", "MOD",
		"SQRT", "POW", "SIGN", "TRUNC", "RANDOM", "MIN", "MAX",
		// Date/Time functions (10)
		"NOW", "CURRENT_DATE", "CURRENT_TIME", "DATE_TRUNC", "DATE_PART",
		"DATE_ADD", "DATE_SUB", "DATE_DIFF", "YEAR", "MONTH",
		// Type conversion (5)
		"CAST", "TRY_CAST", "TO_STRING", "TO_NUMBER", "TO_DATE",
		// Conditional (2)
		"COALESCE", "NULLIF",
	}

	for _, name := range expectedFunctions {
		t.Run(name, func(t *testing.T) {
			fn, exists := registry.Get(name)
			if !exists {
				t.Errorf("expected function %s to be registered in global registry", name)
			}
			if fn.Name() != name {
				t.Errorf("expected function name %s, got %s", name, fn.Name())
			}
		})
	}
}

// Test new string functions

func TestLTrimFunc(t *testing.T) {
	fn := &LTrimFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"leading spaces", []interface{}{"  hello"}, "hello"},
		{"no spaces", []interface{}{"hello"}, "hello"},
		{"trailing only", []interface{}{"hello  "}, "hello  "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRTrimFunc(t *testing.T) {
	fn := &RTrimFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"trailing spaces", []interface{}{"hello  "}, "hello"},
		{"no spaces", []interface{}{"hello"}, "hello"},
		{"leading only", []interface{}{"  hello"}, "  hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubstringFunc(t *testing.T) {
	fn := &SubstringFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"from start", []interface{}{"hello", int64(1), int64(3)}, "hel"},
		{"from middle", []interface{}{"hello", int64(2), int64(3)}, "ell"},
		{"no length", []interface{}{"hello", int64(3)}, "llo"},
		{"past end", []interface{}{"hello", int64(3), int64(10)}, "llo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplaceFunc(t *testing.T) {
	fn := &ReplaceFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"simple", []interface{}{"hello world", "world", "go"}, "hello go"},
		{"multiple", []interface{}{"aaabbbccc", "b", "x"}, "aaaxxxccc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReverseFunc(t *testing.T) {
	fn := &ReverseFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"simple", []interface{}{"hello"}, "olleh"},
		{"empty", []interface{}{""}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsFunc(t *testing.T) {
	fn := &ContainsFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"found", []interface{}{"hello world", "world"}, true},
		{"not found", []interface{}{"hello world", "xyz"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartsWithFunc(t *testing.T) {
	fn := &StartsWithFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"match", []interface{}{"hello world", "hello"}, true},
		{"no match", []interface{}{"hello world", "world"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEndsWithFunc(t *testing.T) {
	fn := &EndsWithFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"match", []interface{}{"hello world", "world"}, true},
		{"no match", []interface{}{"hello world", "hello"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepeatFunc(t *testing.T) {
	fn := &RepeatFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"simple", []interface{}{"ha", int64(3)}, "hahaha"},
		{"zero", []interface{}{"ha", int64(0)}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Test new math functions

func TestSqrtFunc(t *testing.T) {
	fn := &SqrtFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    float64
		wantErr bool
	}{
		{"positive", []interface{}{int64(16)}, 4.0, false},
		{"zero", []interface{}{int64(0)}, 0.0, false},
		{"negative", []interface{}{int64(-1)}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPowFunc(t *testing.T) {
	fn := &PowFunc{}
	tests := []struct {
		name string
		args []interface{}
		want float64
	}{
		{"square", []interface{}{int64(2), int64(3)}, 8.0},
		{"cube", []interface{}{int64(3), int64(2)}, 9.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSignFunc(t *testing.T) {
	fn := &SignFunc{}
	tests := []struct {
		name string
		args []interface{}
		want float64
	}{
		{"positive", []interface{}{int64(42)}, 1.0},
		{"negative", []interface{}{int64(-42)}, -1.0},
		{"zero", []interface{}{int64(0)}, 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncFunc(t *testing.T) {
	fn := &TruncFunc{}
	tests := []struct {
		name string
		args []interface{}
		want float64
	}{
		{"positive", []interface{}{3.7}, 3.0},
		{"negative", []interface{}{-3.7}, -3.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMinMaxFunc(t *testing.T) {
	minFn := &MinFunc{}
	maxFn := &MaxFunc{}

	t.Run("min", func(t *testing.T) {
		got, err := minFn.Evaluate([]interface{}{int64(5), int64(3)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 3.0 {
			t.Errorf("got %v, want 3.0", got)
		}
	})

	t.Run("max", func(t *testing.T) {
		got, err := maxFn.Evaluate([]interface{}{int64(5), int64(3)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 5.0 {
			t.Errorf("got %v, want 5.0", got)
		}
	})
}

// Test conditional functions

func TestCoalesceFunc(t *testing.T) {
	fn := &CoalesceFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"first non-null", []interface{}{nil, nil, "hello", "world"}, "hello"},
		{"all null", []interface{}{nil, nil}, nil},
		{"first value", []interface{}{"first", "second"}, "first"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNullIfFunc(t *testing.T) {
	fn := &NullIfFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		{"equal", []interface{}{"hello", "hello"}, nil},
		{"not equal", []interface{}{"hello", "world"}, "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Test Date/Time Functions

func TestDateNowFunc(t *testing.T) {
	fn := &NowFunc{}
	got, err := fn.Evaluate([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Just verify it returns a non-empty string in RFC3339 format
	if got == "" {
		t.Errorf("NOW() returned empty string")
	}
	// Verify it can be parsed as RFC3339
	if _, ok := got.(string); !ok {
		t.Errorf("NOW() should return string, got %T", got)
	}
}

func TestDateCurrentDateFunc(t *testing.T) {
	fn := &CurrentDateFunc{}
	got, err := fn.Evaluate([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify format YYYY-MM-DD (10 chars)
	if str, ok := got.(string); ok {
		if len(str) != 10 {
			t.Errorf("CURRENT_DATE() should return YYYY-MM-DD format, got %s", str)
		}
	} else {
		t.Errorf("CURRENT_DATE() should return string, got %T", got)
	}
}

func TestDateCurrentTimeFunc(t *testing.T) {
	fn := &CurrentTimeFunc{}
	got, err := fn.Evaluate([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify format HH:MM:SS (8 chars)
	if str, ok := got.(string); ok {
		if len(str) != 8 {
			t.Errorf("CURRENT_TIME() should return HH:MM:SS format, got %s", str)
		}
	} else {
		t.Errorf("CURRENT_TIME() should return string, got %T", got)
	}
}

func TestDateParseDate(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{"RFC3339", "2023-12-25T10:30:00Z", false},
		{"date only", "2023-12-25", false},
		{"date with time", "2023-12-25 10:30:00", false},
		{"date with T", "2023-12-25T10:30:00", false},
		{"invalid format", "25/12/2023", true},
		{"invalid string", "not a date", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseDate(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDateTruncFunc(t *testing.T) {
	fn := &DateTruncFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{"year", []interface{}{"year", "2023-12-25T10:30:45Z"}, false},
		{"month", []interface{}{"month", "2023-12-25T10:30:45Z"}, false},
		{"day", []interface{}{"day", "2023-12-25T10:30:45Z"}, false},
		{"hour", []interface{}{"hour", "2023-12-25T10:30:45Z"}, false},
		{"invalid unit", []interface{}{"week", "2023-12-25T10:30:45Z"}, true},
		{"invalid date", []interface{}{"day", "invalid"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DATE_TRUNC() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == "" {
				t.Errorf("DATE_TRUNC() returned empty string")
			}
		})
	}
}

func TestDatePartFunc(t *testing.T) {
	fn := &DatePartFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"year", []interface{}{"year", "2023-12-25T10:30:45Z"}, 2023, false},
		{"month", []interface{}{"month", "2023-12-25T10:30:45Z"}, 12, false},
		{"day", []interface{}{"day", "2023-12-25T10:30:45Z"}, 25, false},
		{"hour", []interface{}{"hour", "2023-12-25T10:30:45Z"}, 10, false},
		{"minute", []interface{}{"minute", "2023-12-25T10:30:45Z"}, 30, false},
		{"second", []interface{}{"second", "2023-12-25T10:30:45Z"}, 45, false},
		{"invalid unit", []interface{}{"week", "2023-12-25T10:30:45Z"}, 0, true},
		{"invalid date", []interface{}{"year", "invalid"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DATE_PART() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("DATE_PART() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDateAddFunc(t *testing.T) {
	fn := &DateAddFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{"add year", []interface{}{"2023-01-15T10:00:00Z", int64(1), "year"}, false},
		{"add month", []interface{}{"2023-01-15T10:00:00Z", int64(2), "month"}, false},
		{"add day", []interface{}{"2023-01-15T10:00:00Z", int64(5), "day"}, false},
		{"add hour", []interface{}{"2023-01-15T10:00:00Z", int64(3), "hour"}, false},
		{"invalid unit", []interface{}{"2023-01-15T10:00:00Z", int64(1), "week"}, true},
		{"invalid date", []interface{}{"invalid", int64(1), "day"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DATE_ADD() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == "" {
				t.Errorf("DATE_ADD() returned empty string")
			}
		})
	}
}

func TestDateSubFunc(t *testing.T) {
	fn := &DateSubFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{"sub year", []interface{}{"2023-01-15T10:00:00Z", int64(1), "year"}, false},
		{"sub month", []interface{}{"2023-01-15T10:00:00Z", int64(2), "month"}, false},
		{"sub day", []interface{}{"2023-01-15T10:00:00Z", int64(5), "day"}, false},
		{"sub hour", []interface{}{"2023-01-15T10:00:00Z", int64(3), "hour"}, false},
		{"invalid unit", []interface{}{"2023-01-15T10:00:00Z", int64(1), "week"}, true},
		{"invalid date", []interface{}{"invalid", int64(1), "day"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DATE_SUB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == "" {
				t.Errorf("DATE_SUB() returned empty string")
			}
		})
	}
}

func TestDateDiffFunc(t *testing.T) {
	fn := &DateDiffFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"same date", []interface{}{"2023-01-15", "2023-01-15"}, 0, false},
		{"one day diff", []interface{}{"2023-01-16", "2023-01-15"}, 1, false},
		{"negative diff", []interface{}{"2023-01-14", "2023-01-15"}, -1, false},
		{"week diff", []interface{}{"2023-01-22", "2023-01-15"}, 7, false},
		{"invalid first date", []interface{}{"invalid", "2023-01-15"}, 0, true},
		{"invalid second date", []interface{}{"2023-01-15", "invalid"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DATE_DIFF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("DATE_DIFF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDateYearFunc(t *testing.T) {
	fn := &YearFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"basic year", []interface{}{"2023-12-25"}, 2023, false},
		{"with time", []interface{}{"2024-01-15T10:30:00Z"}, 2024, false},
		{"invalid date", []interface{}{"invalid"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("YEAR() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("YEAR() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDateMonthFunc(t *testing.T) {
	fn := &MonthFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"january", []interface{}{"2023-01-15"}, 1, false},
		{"december", []interface{}{"2023-12-25"}, 12, false},
		{"with time", []interface{}{"2024-06-15T10:30:00Z"}, 6, false},
		{"invalid date", []interface{}{"invalid"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("MONTH() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("MONTH() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test Type Conversion Functions

func TestCastFunc(t *testing.T) {
	fn := &CastFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		// String conversions
		{"int to string", []interface{}{int64(42), "string"}, "42", false},
		{"float to string", []interface{}{3.14, "string"}, "3.14", false},
		{"string to string", []interface{}{"hello", "string"}, "hello", false},
		{"bool to string", []interface{}{true, "string"}, "true", false},

		// Number conversions
		{"string to number", []interface{}{"123", "number"}, 123.0, false},
		{"int to number", []interface{}{int64(42), "number"}, 42.0, false},
		{"float to number", []interface{}{3.14, "number"}, 3.14, false},
		{"invalid number", []interface{}{"abc", "number"}, nil, true},

		// Date conversions
		{"date string to date", []interface{}{"2023-12-25", "date"}, nil, false},
		{"datetime to date", []interface{}{"2023-12-25T10:30:00Z", "date"}, nil, false},
		{"invalid date", []interface{}{"not-a-date", "date"}, nil, true},

		// Unknown type
		{"unknown type", []interface{}{"value", "unknown"}, nil, true},

		// Case insensitivity
		{"uppercase STRING", []interface{}{42, "STRING"}, "42", false},
		{"uppercase NUMBER", []interface{}{"123", "NUMBER"}, 123.0, false},
		{"uppercase DATE", []interface{}{"2023-12-25", "DATE"}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("CAST() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.want != nil && got != tt.want {
				t.Errorf("CAST() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTryCastFunc(t *testing.T) {
	fn := &TryCastFunc{}
	tests := []struct {
		name string
		args []interface{}
		want interface{}
	}{
		// Successful conversions
		{"int to string", []interface{}{int64(42), "string"}, "42"},
		{"string to number", []interface{}{"123", "number"}, 123.0},
		{"date string", []interface{}{"2023-12-25", "date"}, nil}, // Returns time.Time object

		// Failed conversions return nil instead of error
		{"invalid number", []interface{}{"abc", "number"}, nil},
		{"invalid date", []interface{}{"not-a-date", "date"}, nil},
		{"unknown type", []interface{}{"value", "unknown"}, nil},

		// Case insensitivity
		{"uppercase STRING", []interface{}{42, "STRING"}, "42"},
		{"uppercase NUMBER", []interface{}{"123", "NUMBER"}, 123.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Errorf("TRY_CAST() unexpected error = %v", err)
				return
			}
			if tt.want != nil && got != tt.want {
				t.Errorf("TRY_CAST() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToDateFunc(t *testing.T) {
	fn := &ToDateFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"date string", []interface{}{"2023-12-25"}, "2023-12-25", false},
		{"datetime string", []interface{}{"2023-12-25T10:30:00Z"}, "2023-12-25", false},
		{"date with time", []interface{}{"2023-12-25 10:30:00"}, "2023-12-25", false},
		{"invalid date", []interface{}{"not-a-date"}, "", true},
		{"empty string", []interface{}{""}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("TO_DATE() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("TO_DATE() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitFunc(t *testing.T) {
	fn := &SplitFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    []string
		wantErr bool
	}{
		{"simple split", []interface{}{"a,b,c", ","}, []string{"a", "b", "c"}, false},
		{"split by space", []interface{}{"hello world", " "}, []string{"hello", "world"}, false},
		{"split by dash", []interface{}{"2023-12-25", "-"}, []string{"2023", "12", "25"}, false},
		{"no delimiter found", []interface{}{"hello", ","}, []string{"hello"}, false},
		{"empty string", []interface{}{"", ","}, []string{""}, false},
		{"multiple char delimiter", []interface{}{"a::b::c", "::"}, []string{"a", "b", "c"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("SPLIT() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				gotSlice, ok := got.([]string)
				if !ok {
					t.Errorf("SPLIT() returned %T, want []string", got)
					return
				}
				if len(gotSlice) != len(tt.want) {
					t.Errorf("SPLIT() = %v, want %v", gotSlice, tt.want)
					return
				}
				for i := range gotSlice {
					if gotSlice[i] != tt.want[i] {
						t.Errorf("SPLIT()[%d] = %v, want %v", i, gotSlice[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestRandomFunc(t *testing.T) {
	fn := &RandomFunc{}

	// Test that it returns a float between 0 and 1
	for i := 0; i < 10; i++ {
		got, err := fn.Evaluate([]interface{}{})
		if err != nil {
			t.Fatalf("RANDOM() unexpected error: %v", err)
		}
		f, ok := got.(float64)
		if !ok {
			t.Errorf("RANDOM() returned %T, want float64", got)
			continue
		}
		if f < 0 || f >= 1 {
			t.Errorf("RANDOM() = %v, want value in range [0, 1)", f)
		}
	}

	// Test arity
	if fn.MinArity() != 0 {
		t.Errorf("RANDOM() MinArity = %d, want 0", fn.MinArity())
	}
	if fn.MaxArity() != 0 {
		t.Errorf("RANDOM() MaxArity = %d, want 0", fn.MaxArity())
	}
}

func TestMinFunc(t *testing.T) {
	fn := &MinFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    float64
		wantErr bool
	}{
		{"int values", []interface{}{int64(5), int64(3)}, 3.0, false},
		{"float values", []interface{}{5.5, 3.3}, 3.3, false},
		{"mixed int float", []interface{}{int64(5), 3.3}, 3.3, false},
		{"negative values", []interface{}{int64(-5), int64(-3)}, -5.0, false},
		{"zero and positive", []interface{}{int64(0), int64(5)}, 0.0, false},
		{"equal values", []interface{}{int64(7), int64(7)}, 7.0, false},
		{"invalid first arg", []interface{}{"abc", int64(5)}, 0, true},
		{"invalid second arg", []interface{}{int64(5), "abc"}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("MIN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if math.Abs(got.(float64)-tt.want) > 0.0001 {
					t.Errorf("MIN() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestMaxFunc(t *testing.T) {
	fn := &MaxFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    float64
		wantErr bool
	}{
		{"int values", []interface{}{int64(5), int64(3)}, 5.0, false},
		{"float values", []interface{}{5.5, 3.3}, 5.5, false},
		{"mixed int float", []interface{}{int64(5), 7.7}, 7.7, false},
		{"negative values", []interface{}{int64(-5), int64(-3)}, -3.0, false},
		{"zero and negative", []interface{}{int64(0), int64(-5)}, 0.0, false},
		{"equal values", []interface{}{int64(7), int64(7)}, 7.0, false},
		{"invalid first arg", []interface{}{"abc", int64(5)}, 0, true},
		{"invalid second arg", []interface{}{int64(5), "abc"}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("MAX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if math.Abs(got.(float64)-tt.want) > 0.0001 {
					t.Errorf("MAX() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestToStringFunc(t *testing.T) {
	fn := &ToStringFunc{}
	tests := []struct {
		name string
		args []interface{}
		want string
	}{
		{"int", []interface{}{int64(42)}, "42"},
		{"float", []interface{}{3.14}, "3.14"},
		{"string", []interface{}{"hello"}, "hello"},
		{"bool true", []interface{}{true}, "true"},
		{"bool false", []interface{}{false}, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if err != nil {
				t.Errorf("TO_STRING() unexpected error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("TO_STRING() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToNumberFunc(t *testing.T) {
	fn := &ToNumberFunc{}
	tests := []struct {
		name    string
		args    []interface{}
		want    float64
		wantErr bool
	}{
		{"int", []interface{}{int64(42)}, 42.0, false},
		{"float", []interface{}{3.14}, 3.14, false},
		{"string int", []interface{}{"123"}, 123.0, false},
		{"string float", []interface{}{"3.14"}, 3.14, false},
		{"invalid string", []interface{}{"abc"}, 0, true},
		{"bool", []interface{}{true}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fn.Evaluate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("TO_NUMBER() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if math.Abs(got.(float64)-tt.want) > 0.0001 {
					t.Errorf("TO_NUMBER() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
