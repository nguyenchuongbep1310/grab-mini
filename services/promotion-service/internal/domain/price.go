package domain

import (
	"errors"
	"math"
	"strconv"
)

// ParsePrice reads the trip price in VND. A client may send 87000 or 87000.0;
// it is rounded to a whole dong, and must be positive and not absurd.
func ParsePrice(v string) (int64, error) {
	if v == "" {
		return 0, errors.New("price is required")
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errors.New("price must be a number of VND")
	}
	if f <= 0 || f > float64(MaxAmount) {
		return 0, errors.New("price must be above 0 VND")
	}
	return int64(math.Round(f)), nil
}
