package query

import (
	"math"
	"testing"
)

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
