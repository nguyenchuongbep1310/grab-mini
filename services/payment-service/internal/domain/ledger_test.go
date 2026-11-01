package domain

import (
	"errors"
	"math/rand"
	"testing"
)

func TestValidateAcceptsABalancedTransaction(t *testing.T) {
	txn := Txn{ID: "t", Key: "fare:1", Kind: "FARE", Currency: CurrencyVND, Entries: []Entry{
		{RiderAccount("r"), -100},
		{DriverPayableAccount("d"), 80},
		{PlatformRevenueAccount(), 20},
	}}
	if err := txn.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
}

func TestValidateRefusesAnUnbalancedTransactionPermanently(t *testing.T) {
	txn := Txn{ID: "t", Key: "fare:1", Kind: "FARE", Currency: CurrencyVND, Entries: []Entry{
		{RiderAccount("r"), -100},
		{DriverPayableAccount("d"), 80},
		{PlatformRevenueAccount(), 19}, // a dong lost
	}}
	err := txn.Validate()
	var ub *UnbalancedError
	if !errors.As(err, &ub) || !ub.Permanent() {
		t.Fatalf("Validate = %v, want a permanent UnbalancedError", err)
	}
}

func TestValidateRefusesMalformedTransactions(t *testing.T) {
	r, d := RiderAccount("r"), DriverPayableAccount("d")
	cases := map[string]Txn{
		"one entry":     {ID: "t", Key: "k", Currency: CurrencyVND, Entries: []Entry{{r, 0}}},
		"a zero entry":  {ID: "t", Key: "k", Currency: CurrencyVND, Entries: []Entry{{r, -5}, {d, 5}, {PlatformRevenueAccount(), 0}}},
		"no key":        {ID: "t", Currency: CurrencyVND, Entries: []Entry{{r, -5}, {d, 5}}},
		"no currency":   {ID: "t", Key: "k", Entries: []Entry{{r, -5}, {d, 5}}},
		"out of bounds": {ID: "t", Key: "k", Currency: CurrencyVND, Entries: []Entry{{r, -MaxAmount - 1}, {d, MaxAmount + 1}}},
		"empty":         {ID: "t", Key: "k", Currency: CurrencyVND},
		"no id":         {Key: "k", Currency: CurrencyVND, Entries: []Entry{{r, -5}, {d, 5}}},
	}
	for name, txn := range cases {
		if err := txn.Validate(); err == nil {
			t.Errorf("%s: Validate = nil, want an error", name)
		}
	}
}

func TestChargeTxnAlwaysBalances(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 20000; i++ {
		amount := 1 + rng.Int63n(MaxAmount)
		driver, platform := Split(amount, rng.Int63n(BpsDenominator+1))
		txn := ChargeTxn("t", "k", KindFare, "r", "d", driver, platform)
		if err := txn.Validate(); err != nil {
			t.Fatalf("amount %d split %d/%d: %v", amount, driver, platform, err)
		}
		for _, e := range txn.Entries {
			if e.Amount == 0 {
				t.Fatalf("zero entry for %d split %d/%d", amount, driver, platform)
			}
		}
	}
}

func TestChargeTxnLeavesOutAZeroShare(t *testing.T) {
	tip := ChargeTxn("t", "tip:1", KindTip, "r", "d", 20000, 0)
	if len(tip.Entries) != 2 {
		t.Fatalf("a tip has %d entries, want 2 (rider, driver)", len(tip.Entries))
	}
	all := ChargeTxn("t", "fare:1", KindFare, "r", "d", 0, 5000)
	if len(all.Entries) != 2 || all.Entries[1].Acct.Kind != AccountPlatformRevenue {
		t.Fatalf("a fare kept wholly by the platform has entries %+v", all.Entries)
	}
	for _, txn := range []Txn{tip, all} {
		if err := txn.Validate(); err != nil {
			t.Error(err)
		}
	}
}
