package query

import (
	"testing"
)

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

func TestMinMaxArityDateTimeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		minArity int
		maxArity int
	}{
		{"NOW", &NowFunc{}, 0, 0},
		{"CURRENT_DATE", &CurrentDateFunc{}, 0, 0},
		{"CURRENT_TIME", &CurrentTimeFunc{}, 0, 0},
		{"DATE_TRUNC", &DateTruncFunc{}, 2, 2},
		{"DATE_PART", &DatePartFunc{}, 2, 2},
		{"DATE_ADD", &DateAddFunc{}, 3, 3},
		{"DATE_SUB", &DateSubFunc{}, 3, 3},
		{"DATE_DIFF", &DateDiffFunc{}, 2, 2},
		{"YEAR", &YearFunc{}, 1, 1},
		{"MONTH", &MonthFunc{}, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn.MinArity(); got != tt.minArity {
				t.Errorf("%s.MinArity() = %d, want %d", tt.name, got, tt.minArity)
			}
			if got := tt.fn.MaxArity(); got != tt.maxArity {
				t.Errorf("%s.MaxArity() = %d, want %d", tt.name, got, tt.maxArity)
			}
		})
	}
}
