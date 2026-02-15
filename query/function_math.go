package query

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// Math Functions

// AbsFunc returns the absolute value of a number
type AbsFunc struct{}

func (f *AbsFunc) Name() string  { return "ABS" }
func (f *AbsFunc) MinArity() int { return 1 }
func (f *AbsFunc) MaxArity() int { return 1 }
func (f *AbsFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("ABS: %w", err)
	}
	return math.Abs(num), nil
}

// RoundFunc rounds a number to the specified number of decimal places
type RoundFunc struct{}

func (f *RoundFunc) Name() string  { return "ROUND" }
func (f *RoundFunc) MinArity() int { return 1 }
func (f *RoundFunc) MaxArity() int { return 2 }
func (f *RoundFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND: %w", err)
	}

	// Default to 0 decimal places
	decimals := 0.0
	if len(args) == 2 {
		decimals, err = valueToNumber(args[1])
		if err != nil {
			return nil, fmt.Errorf("ROUND: decimals argument: %w", err)
		}
	}

	multiplier := math.Pow(10, float64(decimals))
	return math.Round(num*multiplier) / multiplier, nil
}

// FloorFunc returns the largest integer less than or equal to a number
type FloorFunc struct{}

func (f *FloorFunc) Name() string  { return "FLOOR" }
func (f *FloorFunc) MinArity() int { return 1 }
func (f *FloorFunc) MaxArity() int { return 1 }
func (f *FloorFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("FLOOR: %w", err)
	}
	return math.Floor(num), nil
}

// CeilFunc returns the smallest integer greater than or equal to a number
type CeilFunc struct{}

func (f *CeilFunc) Name() string  { return "CEIL" }
func (f *CeilFunc) MinArity() int { return 1 }
func (f *CeilFunc) MaxArity() int { return 1 }
func (f *CeilFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("CEIL: %w", err)
	}
	return math.Ceil(num), nil
}

// ModFunc returns the remainder of division
type ModFunc struct{}

func (f *ModFunc) Name() string  { return "MOD" }
func (f *ModFunc) MinArity() int { return 2 }
func (f *ModFunc) MaxArity() int { return 2 }
func (f *ModFunc) Evaluate(args []interface{}) (interface{}, error) {
	dividend, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MOD: dividend: %w", err)
	}

	divisor, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MOD: divisor: %w", err)
	}

	if divisor == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}

	return math.Mod(dividend, divisor), nil
}

// SqrtFunc returns the square root
type SqrtFunc struct{}

func (f *SqrtFunc) Name() string  { return "SQRT" }
func (f *SqrtFunc) MinArity() int { return 1 }
func (f *SqrtFunc) MaxArity() int { return 1 }
func (f *SqrtFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("SQRT: %w", err)
	}
	if num < 0 {
		return nil, fmt.Errorf("SQRT: negative number")
	}
	return math.Sqrt(num), nil
}

// PowFunc returns x raised to the power of y
type PowFunc struct{}

func (f *PowFunc) Name() string  { return "POW" }
func (f *PowFunc) MinArity() int { return 2 }
func (f *PowFunc) MaxArity() int { return 2 }
func (f *PowFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("POW: base: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("POW: exponent: %w", err)
	}

	return math.Pow(x, y), nil
}

// SignFunc returns the sign of a number (-1, 0, or 1)
type SignFunc struct{}

func (f *SignFunc) Name() string  { return "SIGN" }
func (f *SignFunc) MinArity() int { return 1 }
func (f *SignFunc) MaxArity() int { return 1 }
func (f *SignFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("SIGN: %w", err)
	}

	if num < 0 {
		return float64(-1), nil
	}
	if num > 0 {
		return float64(1), nil
	}
	return float64(0), nil
}

// TruncFunc truncates a number to an integer
type TruncFunc struct{}

func (f *TruncFunc) Name() string  { return "TRUNC" }
func (f *TruncFunc) MinArity() int { return 1 }
func (f *TruncFunc) MaxArity() int { return 1 }
func (f *TruncFunc) Evaluate(args []interface{}) (interface{}, error) {
	num, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("TRUNC: %w", err)
	}
	return math.Trunc(num), nil
}

// RandomFunc returns a random number between 0 and 1
type RandomFunc struct{}

func (f *RandomFunc) Name() string  { return "RANDOM" }
func (f *RandomFunc) MinArity() int { return 0 }
func (f *RandomFunc) MaxArity() int { return 0 }
func (f *RandomFunc) Evaluate(args []interface{}) (interface{}, error) {
	return rand.Float64(), nil
}

// MinFunc returns the minimum of two values
type MinFunc struct{}

func (f *MinFunc) Name() string  { return "MIN" }
func (f *MinFunc) MinArity() int { return 2 }
func (f *MinFunc) MaxArity() int { return 2 }
func (f *MinFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MIN: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MIN: %w", err)
	}

	return math.Min(x, y), nil
}

// MaxFunc returns the maximum of two values
type MaxFunc struct{}

func (f *MaxFunc) Name() string  { return "MAX" }
func (f *MaxFunc) MinArity() int { return 2 }
func (f *MaxFunc) MaxArity() int { return 2 }
func (f *MaxFunc) Evaluate(args []interface{}) (interface{}, error) {
	x, err := valueToNumber(args[0])
	if err != nil {
		return nil, fmt.Errorf("MAX: %w", err)
	}

	y, err := valueToNumber(args[1])
	if err != nil {
		return nil, fmt.Errorf("MAX: %w", err)
	}

	return math.Max(x, y), nil
}
