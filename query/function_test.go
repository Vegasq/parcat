package query

import (
	"testing"
)

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
