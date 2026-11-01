package domain

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

func TestPriceToMinorRoundsToWholeDong(t *testing.T) {
	cases := []struct {
		price float64
		want  int64
	}{
		{0, 0},
		{50000, 50000},
		{49999.9999999, 50000}, // float noise
		{50000.4, 50000},
		{50000.5, 50001},
		{123000, 123000},
		{float64(MaxAmount), MaxAmount},
	}
	for _, c := range cases {
		got, err := PriceToMinor(c.price)
		if err != nil || got != c.want {
			t.Errorf("PriceToMinor(%v) = %d, %v; want %d", c.price, got, err, c.want)
		}
	}
}

func TestPriceToMinorRefusesValuesThatCanNeverBeCharged(t *testing.T) {
	for _, price := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), float64(MaxAmount) * 2} {
		_, err := PriceToMinor(price)
		var ae *AmountError
		if !errors.As(err, &ae) || !ae.Permanent() {
			t.Errorf("PriceToMinor(%v) error = %v, want a permanent AmountError", price, err)
		}
	}
}

func TestCheckAmount(t *testing.T) {
	for _, ok := range []int64{0, 1, 20000, MaxAmount} {
		if err := CheckAmount(ok); err != nil {
			t.Errorf("CheckAmount(%d) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []int64{-1, MaxAmount + 1, math.MinInt64, math.MaxInt64} {
		if err := CheckAmount(bad); err == nil {
			t.Errorf("CheckAmount(%d) = nil, want an error", bad)
		}
	}
}

func TestSplitKnownValues(t *testing.T) {
	cases := []struct {
		amount, bps, driver, platform int64
	}{
		{100000, 2000, 80000, 20000},
		{100000, 0, 100000, 0},
		{100000, 10000, 0, 100000},
		{15000, 2000, 12000, 3000},
		{1, 2000, 1, 0},             // the commission rounds down: the driver gets the odd dong
		{19, 2000, 16, 3},           // 19 * 20% = 3.8 -> 3
		{33333, 3333, 22224, 11109}, // 33333 * 33.33% = 11109.89 -> 11109
		{0, 2000, 0, 0},
	}
	for _, c := range cases {
		d, p := Split(c.amount, c.bps)
		if d != c.driver || p != c.platform {
			t.Errorf("Split(%d, %d) = %d, %d; want %d, %d", c.amount, c.bps, d, p, c.driver, c.platform)
		}
	}
}

// No rounding rule may create or lose a dong, nor give anyone a negative share.
func TestSplitAlwaysSumsToTheAmount(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100000; i++ {
		amount := rng.Int63n(MaxAmount + 1)
		bps := rng.Int63n(BpsDenominator + 1)
		d, p := Split(amount, bps)
		if d+p != amount || d < 0 || p < 0 {
			t.Fatalf("Split(%d, %d) = %d, %d", amount, bps, d, p)
		}
	}
	// The extremes: the largest amount at the largest rate must not overflow.
	if d, p := Split(MaxAmount, BpsDenominator); d != 0 || p != MaxAmount {
		t.Errorf("Split(max, 100%%) = %d, %d", d, p)
	}
}
