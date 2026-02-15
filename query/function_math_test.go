package query

import (
	"math"
	"testing"
)

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
