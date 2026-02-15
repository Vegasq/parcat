package query

import (
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
